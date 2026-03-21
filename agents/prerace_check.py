"""
agents/prerace_check.py  発走30分前チェックエージェント
- 今日選出されたレースの発走時刻を確認
- 発走30分前±20分の範囲なら馬体重を取得
- スコアを再計算してDiscord通知
"""

import json, re, datetime, sys
from pathlib import Path
from datetime import timezone, timedelta

JST = timezone(timedelta(hours=9))
DATA_DIR = Path('data')

def parse_race_time(time_str: str, date_str: str) -> datetime.datetime | None:
    """'14:15' + '20260322' → JSTのdatetimeに変換"""
    try:
        m = re.match(r'(\d{1,2}):(\d{2})', str(time_str))
        if not m: return None
        h, mi = int(m.group(1)), int(m.group(2))
        d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        return datetime.datetime(d.year, d.month, d.day, h, mi, tzinfo=JST)
    except:
        return None

def fetch_body_weights(race_id: str, page) -> dict:
    """netkeibaの出馬表から馬体重を取得"""
    from playwright.sync_api import sync_playwright
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        weights = page.evaluate("""() => {
            const rows = Array.from(document.querySelectorAll('table tr'))
                .filter(r => r.querySelectorAll('td').length >= 6);
            const result = {};
            rows.forEach(r => {
                const link = r.querySelector('a[href*="/horse/"]');
                const tds = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
                if (!link) return;
                const name = link.textContent.trim().replace(/^\\d+\\s*\\n?/,'').trim();
                // 馬体重セルを探す（例: "486(+2)"）
                const wCell = tds.find(t => /\\d{3}\\([+-]?\\d+\\)/.test(t));
                if (name && wCell) result[name] = wCell;
            });
            return result;
        }""")
        return weights or {}
    except Exception as e:
        print(f"[prerace] 馬体重取得エラー: {e}")
        return {}

def bw_score_from_str(weight_str: str) -> tuple[int, int, str]:
    """'486(+2)' → (486, +2, スコア)"""
    m = re.search(r'(\d+)\(([+-]?\d+)\)', str(weight_str))
    if not m: return 0, 0, ''
    bw = int(m.group(1))
    chg = int(m.group(2))
    if chg > 8:        score = 9; comment = '⬆大幅増量'
    elif 1 <= chg <= 4:score = 8; comment = '✅良好'
    elif chg == 0:     score = 8; comment = '✅変化なし'
    elif -4 <= chg < 0:score = 7; comment = '✅小幅減'
    elif 4 < chg <= 8: score = 7; comment = '⚠️やや増量'
    elif -8 <= chg < -4:score = 6; comment = '⚠️やや減量'
    elif -13 <= chg < -8:score = 5; comment = '⚠️減量注意'
    else:              score = 4; comment = '❌大幅減量'
    return bw, chg, comment

def run_prerace_check(date_str: str = None):
    """発走30分前チェックのメイン処理"""
    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    now_jst = datetime.datetime.now(JST)
    print(f"[prerace] チェック開始: {now_jst.strftime('%H:%M')} JST")

    # 今日の予測結果を読み込み
    pred_file = DATA_DIR / f'selected_{date_str}.json'
    if not pred_file.exists():
        print(f"[prerace] 予測ファイルなし: {pred_file}")
        return []

    with open(pred_file) as f:
        predictions = json.load(f)

    if not predictions:
        print("[prerace] 本日の参加レースなし")
        return []

    # 発走30分前±20分のレースを探す
    target_races = []
    for pred in predictions:
        race_time_str = pred.get('start_time', '')
        if not race_time_str:
            # race_nameから時刻を推定できない場合はスキップ
            print(f"[prerace] 発走時刻不明: {pred.get('race_name','?')}")
            continue
        race_dt = parse_race_time(race_time_str, date_str)
        if not race_dt: continue
        minutes_to_go = (race_dt - now_jst).total_seconds() / 60
        print(f"[prerace] {pred.get('race_name','?')}: 発走まで{minutes_to_go:.0f}分")
        if 10 <= minutes_to_go <= 50:  # 10〜50分前の範囲でチェック
            target_races.append(pred)

    if not target_races:
        print("[prerace] 発走30分前のレースなし")
        return []

    # 馬体重を取得してスコア再計算
    from playwright.sync_api import sync_playwright
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(20000)

        for pred in target_races:
            race_id = pred['race_id']
            race_name = pred['race_name']
            print(f"[prerace] 馬体重取得: {race_name}")

            weights = fetch_body_weights(race_id, page)
            if not weights:
                print(f"[prerace] 馬体重取得失敗: {race_name}")
                continue

            # 各馬の馬体重スコアを評価
            horse_updates = []
            has_warning = False
            for horse in pred.get('scores', [])[:5]:
                name = horse['name']
                w_str = weights.get(name, '')
                if w_str:
                    bw, chg, comment = bw_score_from_str(w_str)
                    if score_val := (4 if chg < -13 else 5 if chg < -8 else 6 if chg < -4 else 7):
                        if chg < -4: has_warning = True
                    horse_updates.append({
                        'name': name,
                        'weight': w_str,
                        'chg': chg,
                        'comment': comment,
                        'is_main': name == pred['best']['name']
                    })
                else:
                    horse_updates.append({'name': name, 'weight': '未発表', 'chg': 0, 'comment': '—', 'is_main': name == pred['best']['name']})

            results.append({
                'pred': pred,
                'weights': weights,
                'horse_updates': horse_updates,
                'has_warning': has_warning,
            })

        browser.close()

    return results

def format_prerace_message(result: dict) -> str:
    """Discord通知用テキストを生成"""
    pred = result['pred']
    horse_updates = result['horse_updates']
    has_warning = result['has_warning']

    best = pred['best']
    bet = pred.get('bet', {})

    lines = [
        f"## 発走30分前チェック ⏰",
        f"**{pred['race_name']}**（{pred['course']}{pred['dist']}m / {pred['n_horses']}頭）",
        f"判定: {pred['condition']}",
        "",
        "**馬体重**",
    ]

    for h in horse_updates:
        mark = '◎' if h['is_main'] else '  '
        lines.append(f"{mark} {h['name']}: {h['weight']} {h['comment']}")

    lines.append("")

    if has_warning:
        lines.append("⚠️ 大幅な体重変化あり → 買い方を再検討してください")
    else:
        lines.append("✅ 馬体重に問題なし → 予定通り購入OK")

    lines.append("")
    lines.append("**最終推奨**")
    for b in bet.get('bets', []):
        if 'horses' in b:
            lines.append(f"  三連複 {'-'.join(b['horses'])} → {b['amount']:,}円")
        else:
            lines.append(f"  {b['type']} {b.get('horse','')} → {b['amount']:,}円")

    return "\n".join(lines)

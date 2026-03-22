"""
agents/tracker.py  結果追跡エージェント
prerace_check.py + recorder.pyを統合
- 発走30分前：馬体重取得・最終チェック通知
- レース後：結果取得・HorseRacingDBに記録
"""
import json, re, os, requests, datetime
from pathlib import Path
from datetime import timezone, timedelta

JST       = timezone(timedelta(hours=9))
DATA_DIR  = Path('data')
HORSE_RACING_DB = "2df1333d21b1808293adc2fe02155ce9"


def _headers():
    key = os.environ.get('NOTION_API_KEY', '')
    if not key: return None
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


# ===== 発走前チェック（旧prerace_check） =====

def parse_race_time(time_str, date_str):
    m = re.match(r'(\d{1,2}):(\d{2})', str(time_str))
    if not m: return None
    h, mi = int(m.group(1)), int(m.group(2))
    d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    return datetime.datetime(d.year, d.month, d.day, h, mi, tzinfo=JST)


def fetch_body_weights(race_id: str, page) -> dict:
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        return page.evaluate("""() => {
            const rows = Array.from(document.querySelectorAll('table tr'))
                .filter(r => r.querySelectorAll('td').length >= 6);
            const result = {};
            rows.forEach(r => {
                const link = r.querySelector('a[href*="/horse/"]');
                const tds  = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
                if (!link) return;
                const name  = link.textContent.trim().replace(/^\\d+\\s*\\n?/,'').trim();
                const wCell = tds.find(t => /\\d{3}\\([+-]?\\d+\\)/.test(t));
                if (name && wCell) result[name] = wCell;
            });
            return result;
        }""") or {}
    except Exception as e:
        print(f"[tracker] 馬体重取得エラー: {e}")
        return {}


def run_prerace(date_str: str = None, predictions: list = None) -> list:
    """発走30分前チェック"""
    from playwright.sync_api import sync_playwright
    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    now_jst  = datetime.datetime.now(JST)

    if predictions is None:
        try:
            from notion_store import load_predictions
            predictions = load_predictions(date_str)
        except:
            pred_file = DATA_DIR / f'selected_{date_str}.json'
            predictions = json.load(open(pred_file)) if pred_file.exists() else []

    if not predictions:
        print("[tracker] 本日の予測データなし")
        return []

    # 発走10〜50分前のレースを絞り込み
    targets = []
    for pred in predictions:
        rt = parse_race_time(pred.get('start_time',''), date_str)
        if rt:
            mins = (rt - now_jst).total_seconds() / 60
            print(f"[tracker] {pred.get('race_name','?')}: 発走まで{mins:.0f}分")
            if 10 <= mins <= 50:
                targets.append(pred)

    if not targets:
        print("[tracker] 発走30分前のレースなし")
        return []

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        page.set_default_timeout(20000)
        for pred in targets:
            weights = fetch_body_weights(pred['race_id'], page)
            updates = []
            has_warning = False
            for horse in pred.get('scores', [])[:3]:
                name  = horse['name']
                w_str = weights.get(name, '')
                m = re.search(r'(\d+)\(([+-]?\d+)\)', w_str) if w_str else None
                if m:
                    chg = int(m.group(2))
                    if chg < -4: has_warning = True
                    comment = '✅良好' if -4<=chg<=4 else ('⚠️注意' if -8<=chg<-4 else '❌大幅減量')
                    updates.append({'name': name, 'weight': w_str, 'comment': comment,
                                    'is_main': name == pred['best']['name']})
                else:
                    updates.append({'name': name, 'weight': '未発表', 'comment': '—',
                                    'is_main': name == pred['best']['name']})
            results.append({'pred': pred, 'updates': updates, 'has_warning': has_warning})
        browser.close()

    return results


def format_prerace_message(result: dict) -> str:
    pred    = result['pred']
    updates = result['updates']
    lines   = [
        f"## ⏰ 発走30分前チェック",
        f"**{pred['race_name']}**（{pred['course']}{pred['dist']}m / {pred['n_horses']}頭）",
        "", "**馬体重**",
    ]
    for h in updates:
        mark = '◎' if h['is_main'] else '  '
        lines.append(f"{mark} {h['name']}: {h['weight']} {h['comment']}")
    lines.append("")
    lines.append("⚠️ 大幅な体重変化あり → 購入を慎重に" if result['has_warning'] else "✅ 馬体重に問題なし → 予定通り購入OK")
    return '\n'.join(lines)


# ===== レース結果記録（旧recorder） =====

def _get_existing_records(date_fmt: str) -> set:
    h = _headers()
    if not h: return set()
    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{HORSE_RACING_DB}/query",
            headers=h,
            json={"filter": {"property": "日付", "date": {"equals": date_fmt}}},
            timeout=15
        )
        resp.raise_for_status()
        names = set()
        for p in resp.json().get('results', []):
            title = p.get('properties', {}).get('名前', {}).get('title', [])
            if title: names.add(title[0].get('plain_text', ''))
        return names
    except:
        return set()


def _save_race_page(name: str, amount: int, payout: int, date_fmt: str):
    h = _headers()
    if not h: return
    payload = {
        "parent": {"database_id": HORSE_RACING_DB},
        "properties": {
            "名前":   {"title": [{"text": {"content": name}}]},
            "掛け金": {"number": amount},
            "払戻金": {"number": payout},
            "日付":   {"date": {"start": date_fmt}},
        }
    }
    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=h, json=payload, timeout=15
        )
        resp.raise_for_status()
        print(f"[tracker] 記録: {name} 掛け{amount:,}円 払戻{payout:,}円")
    except Exception as e:
        print(f"[tracker] 保存エラー: {name} - {e}")


def run_record(date_str: str, predictions: list, comparisons: list) -> dict:
    """レース結果をHorseRacingDBに記録"""
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    existing = _get_existing_records(date_fmt)
    saved = skipped = 0

    for pred in predictions:
        comp          = next((c for c in comparisons if c['race_id'] == pred['race_id']), {})
        race_name     = pred.get('race_name', pred['race_id'])
        tansho_hit    = comp.get('tansho_hit', False)
        tansho_payout = comp.get('tansho_payout') or 0

        for b in pred.get('bet', {}).get('bets', []):
            bet_type = b.get('type', '')
            amount   = b.get('amount', 0)
            horse    = b.get('horse', '') or '-'.join(b.get('horses', []))
            name     = f"{race_name} {bet_type} {horse}"

            if name in existing:
                skipped += 1
                continue

            payout = 0
            if '単勝' in bet_type and tansho_hit:
                payout = int(amount * (tansho_payout / 100))

            _save_race_page(name, amount, payout, date_fmt)
            saved += 1

    print(f"[tracker] ✅ 記録完了: {saved}件保存 / {skipped}件スキップ")
    return {"saved": saved, "skipped": skipped}

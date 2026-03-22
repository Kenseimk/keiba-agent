"""
agents/learner.py  夜間学習エージェント
毎夜：レース結果取得 → 予測との比較 → パラメータ改善提案 → Discord報告
"""

import json, re, datetime, anthropic
from pathlib import Path
from playwright.sync_api import sync_playwright

DATA_DIR = Path('data')
LOG_FILE  = DATA_DIR / 'learning_log.jsonl'

LEARNER_SYSTEM = """あなたは競馬予想モデルの改善エージェントです。
本日の予測と実際の結果を比較し、スコアパラメータの改善提案を行います。

以下を分析してください：
1. 本命が的中したか（単勝）
2. 三連複の上位3頭が正しかったか
3. 脚質スコア・上がりスコアが有効だったか
4. ペース（テン3F）の影響はどうだったか
5. 割安フラグが機能したか

出力はJSON形式で：
{
  "summary": "今日の予測精度の要約（2〜3文）",
  "what_worked": ["うまく機能した点"],
  "what_failed": ["うまくいかなかった点"],
  "param_suggestions": {
    "w_pop": 現在2.0 → 提案値,
    "w_dev": 現在0.5 → 提案値,
    "w_dc":  現在0.5 → 提案値
  },
  "notes": "その他の気づき"
}"""


def fetch_results_from_netkeiba(race_ids: list[str]) -> dict:
    """当日のレース結果をnetkeibaから取得"""
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for race_id in race_ids:
            try:
                url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(2000)

                data = page.evaluate("""() => {
                    const rows = Array.from(document.querySelectorAll('table tr'))
                        .filter(r => r.querySelectorAll('td').length >= 10);
                    const result = rows.slice(0,5).map(r => {
                        const tds = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
                        return {着順:tds[0], 馬番:tds[2], 馬名:tds[3], 人気:tds[10], 後3F:tds[11]};
                    }).filter(r => r.馬番);

                    // 払戻
                    const payText = document.body.innerText;
                    const tanshoM = payText.match(/単勝.*?(\\d+)円/);
                    const sanpukuM = payText.match(/3連複.*?(\\d[,\\d]+)円/);

                    // ラップ
                    const lapTable = Array.from(document.querySelectorAll('table'))
                        .find(t => t.textContent.includes('200m'));
                    const lapRows = lapTable ? Array.from(lapTable.querySelectorAll('tr'))
                        .map(r => Array.from(r.querySelectorAll('th,td')).map(c => c.textContent.trim())) : [];

                    return {
                        result,
                        tansho_payout: tanshoM ? parseInt(tanshoM[1].replace(',','')) : null,
                        sanpuku_payout: sanpukuM ? parseInt(sanpukuM[1].replace(',','')) : null,
                        lap: lapRows,
                    };
                }""")
                results[race_id] = data
                print(f"[learner] 結果取得: {race_id}")
            except Exception as e:
                print(f"[learner] ERROR {race_id}: {e}")
        browser.close()
    return results

def compare_predictions(predictions: list[dict], results: dict) -> list[dict]:
    """予測と結果を比較"""
    comparisons = []
    for pred in predictions:
        race_id = pred['race_id']
        actual  = results.get(race_id, {})
        if not actual:
            continue

        result_list = actual.get('result', [])
        actual_1st = result_list[0]['馬名'] if result_list else ''
        actual_top3 = {r['馬名'] for r in result_list[:3]}

        pred_best = pred['best']['name']
        pred_top3 = {h['name'] for h in pred['scores'][:3]}

        tansho_hit    = (pred_best == actual_1st)
        sanpuku_hit   = actual_top3.issubset(pred_top3) or pred_top3.issubset(actual_top3)
        top3_overlap  = len(actual_top3 & pred_top3)

        # テン3F計算
        lap = actual.get('lap', [])
        ten3f = None
        if len(lap) >= 3 and len(lap[2]) >= 3:
            try:
                ten3f = sum(float(lap[2][i]) for i in range(3))
            except: pass

        comparisons.append({
            'race_id':      race_id,
            'race_name':    pred['race_name'],
            'condition':    pred['condition'],
            'pred_best':    pred_best,
            'actual_1st':   actual_1st,
            'tansho_hit':   tansho_hit,
            'sanpuku_hit':  sanpuku_hit,
            'top3_overlap': top3_overlap,
            'pred_top3':    list(pred_top3),
            'actual_top3':  list(actual_top3),
            'tansho_payout': actual.get('tansho_payout'),
            'ten3f':         ten3f,
        })

    return comparisons

def run_learner(date_str: str = None) -> dict:
    """夜間学習のメイン実行"""
    date_str = date_str or datetime.date.today().strftime('%Y%m%d')
    print(f"[learner] 学習開始: {date_str}")

    # 当日の予測データを読み込み
    pred_file = DATA_DIR / f'selected_{date_str}.json'
    if not pred_file.exists():
        print(f"[learner] 予測ファイルなし: {pred_file}")
        return {'error': 'no predictions found'}

    with open(pred_file) as f:
        predictions = json.load(f)

    if not predictions:
        return {'summary': '本日は参加レースなし'}

    # 実際の結果を取得
    race_ids = [p['race_id'] for p in predictions]
    results  = fetch_results_from_netkeiba(race_ids)

    # 比較
    comparisons = compare_predictions(predictions, results)

    # Claude APIで分析
    client = anthropic.Anthropic()
    comp_text = json.dumps(comparisons, ensure_ascii=False, indent=2)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=LEARNER_SYSTEM,
        messages=[{"role": "user", "content": f"本日({date_str})の予測・結果比較:\n{comp_text}"}]
    )

    raw = response.content[0].text
    # JSONを抽出
    try:
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        analysis = json.loads(m.group()) if m else {"summary": raw}
    except:
        analysis = {"summary": raw}

    # 結果を保存
    log_entry = {
        'date':        date_str,
        'comparisons': comparisons,
        'analysis':    analysis,
    }
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

    # サマリー保存
    result_file = DATA_DIR / f'learning_{date_str}.json'
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(log_entry, f, ensure_ascii=False, indent=2)

    # ===== Notion自動記録 =====
    try:
        # レース結果をHorseRacingDBに記録
        for pred in predictions:
            pred['date'] = date_str
        save_races_to_notion(comparisons, predictions)

        # 月末なら月全体の損益をNotionから集計して軍資金に記録
        if check_monthly_end(date_str):
            year  = int(date_str[:4])
            month = int(date_str[4:6])
            monthly_profit = calc_monthly_profit_from_notion(year, month)
            if monthly_profit > 0:
                add_amount = int(monthly_profit * 0.70)
                save_cash_to_notion(
                    f"{year}年{month}月 利益70%追加",
                    add_amount,
                    f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                )
                print(f"[learner] 月末利益追加: {add_amount}円（{year}/{month}月 利益{monthly_profit}円の70%）")
            else:
                print(f"[learner] 月末: {year}/{month}月は損益{monthly_profit}円のため軍資金追加なし")
    except Exception as e:
        print(f"[learner] Notion記録エラー（続行）: {e}")

    print(f"[learner] 学習完了: {analysis.get('summary','')}")
    return log_entry

def format_learner_output(log_entry: dict) -> str:
    """Discord通知用テキスト"""
    if 'error' in log_entry:
        return f"夜間学習: {log_entry['error']}"

    analysis = log_entry.get('analysis', {})
    comps    = log_entry.get('comparisons', [])

    hits  = sum(1 for c in comps if c['tansho_hit'])
    total = len(comps)

    lines = [
        f"## 本日の学習レポート",
        f"単勝的中: {hits}/{total}レース",
        f"",
        f"**{analysis.get('summary','')}**",
        "",
    ]

    worked = analysis.get('what_worked', [])
    failed = analysis.get('what_failed', [])

    if worked:
        lines.append("✅ うまくいった点")
        for w in worked:
            lines.append(f"  - {w}")

    if failed:
        lines.append("⚠️ 改善点")
        for f_item in failed:
            lines.append(f"  - {f_item}")

    params = analysis.get('param_suggestions', {})
    if params:
        lines.append("\n📊 パラメータ改善提案")
        for k, v in params.items():
            lines.append(f"  {k}: → {v}")

    if analysis.get('notes'):
        lines.append(f"\n📝 {analysis['notes']}")

    return "\n".join(lines)


if __name__ == '__main__':
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    log = run_learner(date_arg)
    print(format_learner_output(log))


# ===== Notion自動記録 =====

NOTION_HORSE_RACING_DS = "2df1333d-21b1-8089-8c51-000bd8a8c87d"  # HorseRacingDB
NOTION_CASH_DS         = "2eb1333d-21b1-8038-9c5e-000b44ea9f23"  # HorseRacing-Cash

def save_races_to_notion(comparisons: list[dict], predictions: list[dict]) -> bool:
    """レース結果をNotionのHorseRacingDBに自動記録"""
    import requests, os
    key = os.environ.get('NOTION_API_KEY', '')
    if not key:
        print("[notion] NOTION_API_KEY未設定")
        return False

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    saved = 0
    for pred in predictions:
        comp = next((c for c in comparisons if c['race_id'] == pred['race_id']), None)
        race_name = pred.get('race_name', pred['race_id'])
        date_str  = pred.get('date', datetime.datetime.now().strftime('%Y-%m-%d'))
        # YYYY-MM-DD形式に変換
        if len(date_str) == 8:
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # bet情報から各馬券を取得
        bet = pred.get('bet', {})
        bets = bet.get('bets', [])

        for b in bets:
            horse = b.get('horse', '') or '-'.join(b.get('horses', []))
            bet_type = b.get('type', '')
            amount = b.get('amount', 0)

            # 払戻金を判定（結果から）
            payout = 0
            if comp:
                actual_1st = comp.get('actual_1st', '')
                tansho_hit = comp.get('tansho_hit', False)
                tansho_payout = comp.get('tansho_payout') or 0

                if '単勝' in bet_type and tansho_hit:
                    payout = int(amount * (tansho_payout / 100))
                elif '三連複' in bet_type:
                    # 三連複の払戻は別途取得が必要なため0とする（手動修正）
                    payout = 0

            name = f"{race_name} {bet_type} {horse}"
            payload = {
                "parent": {"database_id": NOTION_HORSE_RACING_DS},
                "properties": {
                    "名前": {"title": [{"text": {"content": name}}]},
                    "掛け金": {"number": amount},
                    "払戻金": {"number": payout},
                    "日付": {"date": {"start": date_str}},
                }
            }
            try:
                resp = requests.post(
                    "https://api.notion.com/v1/pages",
                    headers=headers, json=payload, timeout=15
                )
                resp.raise_for_status()
                saved += 1
                print(f"[notion] レース記録: {name} 掛け{amount}円 払戻{payout}円")
            except Exception as e:
                print(f"[notion] レース記録エラー: {e}")

    print(f"[notion] {saved}件記録完了")
    return saved > 0


def save_cash_to_notion(label: str, amount: int, date_str: str = None) -> bool:
    """軍資金DBに記録（月次補充・利益追加）"""
    import requests, os
    key = os.environ.get('NOTION_API_KEY', '')
    if not key: return False

    if not date_str:
        date_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).strftime('%Y-%m-%d')

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }
    payload = {
        "parent": {"database_id": NOTION_CASH_DS},
        "properties": {
            "名前": {"title": [{"text": {"content": label}}]},
            "記録": {"number": amount},
            "日付": {"date": {"start": date_str}},
        }
    }
    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers, json=payload, timeout=15
        )
        resp.raise_for_status()
        print(f"[notion] 軍資金記録: {label} {amount}円")
        return True
    except Exception as e:
        print(f"[notion] 軍資金記録エラー: {e}")
        return False


def check_monthly_end(date_str: str) -> bool:
    """月末かどうか判定"""
    import calendar
    d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day == last_day

def calc_monthly_profit_from_notion(year: int, month: int) -> int:
    """NotionのHorseRacingDBからその月の合計損益を計算"""
    import requests, os
    key = os.environ.get('NOTION_API_KEY', '')
    if not key:
        return 0

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    start = f"{year:04d}-{month:02d}-01"
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    end = f"{year:04d}-{month:02d}-{last_day:02d}"

    # HorseRacingDBから当月分を取得
    payload = {
        "filter": {
            "and": [
                {"property": "日付", "date": {"on_or_after": start}},
                {"property": "日付", "date": {"on_or_before": end}},
            ]
        },
        "page_size": 100
    }

    HORSE_RACING_DB = "2df1333d21b1808293adc2fe02155ce9"
    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{HORSE_RACING_DB}/query",
            headers=headers, json=payload, timeout=15
        )
        resp.raise_for_status()
        pages = resp.json().get('results', [])

        total_invest = 0
        total_payout = 0
        for p in pages:
            props = p.get('properties', {})
            invest = props.get('掛け金', {}).get('number') or 0
            payout = props.get('払戻金', {}).get('number') or 0
            total_invest += invest
            total_payout += payout

        profit = total_payout - total_invest
        print(f"[notion] {year}/{month}月 集計: 投資{total_invest}円 回収{total_payout}円 損益{profit}円")
        return profit
    except Exception as e:
        print(f"[notion] 月次集計エラー: {e}")
        return 0

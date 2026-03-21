"""
agents/learner.py  夜間学習エージェント
毎夜：レース結果取得 → 予測との比較 → パラメータ改善提案 → Discord報告
"""

import json, re, datetime, anthropic
from pathlib import Path
from playwright.sync_api import sync_playwright

DATA_DIR = Path('/home/claude/keiba_agent/data')
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

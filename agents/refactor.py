"""
agents/refactor.py  改修エージェント
evaluator.pyと双方向でやりとりして
スコアリングパラメータを能動的にブラッシュアップし続ける

フロー:
1. 現在のパラメータをベースラインとして評価
2. Claude APIでパラメータ改善案を生成
3. evaluator.pyでバックテスト
4. 改善なら採用、悪化なら却下
5. 最大MAX_ITER回繰り返し
6. 最終結果をDiscord通知 + Notionに記録
7. ユーザーに「適用しますか？」と確認
"""
import json, os, datetime, anthropic
from pathlib import Path
from datetime import timezone, timedelta

JST      = timezone(timedelta(hours=9))
DATA_DIR = Path('data')

MAX_ITER            = 10    # 最大試行回数
MAX_CONSECUTIVE_BAD = 3     # 連続悪化でストップ

# 現在のパラメータ（score_v4.pyのデフォルト）
DEFAULT_PARAMS = {
    'w_jockey':   1.0,
    'w_odds_ev':  1.0,
    'w_pop':      2.0,
    'w_bweight':  0.5,
    'w_dev':      0.5,
    'w_dc':       0.5,
    'w_agari':    1.0,
    'w_leg':      1.0,
}

REFACTOR_SYSTEM = """あなたは競馬予想スコアリングモデルの改善専門エージェントです。
現在のパラメータと過去のバックテスト結果を分析し、
的中率と回収率を改善するためのパラメータ変更案を提案してください。

## 現在のパラメータの意味
- w_jockey:  騎手スコアの重み（デフォルト1.0）
- w_odds_ev: オッズ期待値スコアの重み（デフォルト1.0）
- w_pop:     人気スコアの重み（デフォルト2.0）
- w_bweight: 馬体重スコアの重み（デフォルト0.5）
- w_dev:     タイム偏差値スコアの重み（デフォルト0.5）
- w_dc:      同コース・同距離実績スコアの重み（デフォルト0.5）
- w_agari:   上がり3Fスコアの重み（デフォルト1.0）
- w_leg:     脚質スコアの重み（デフォルト1.0）

## 制約
- 各パラメータは0.1〜3.0の範囲
- 一度に変更するパラメータは1〜2個まで（小さく試す）
- 前回「悪化」だった変更方向には進まない

## 出力形式（JSON）
{
  "proposed_params": {"w_pop": 1.8, ...},
  "reasoning": "なぜこの変更をするか",
  "expected_effect": "どう改善されると期待するか"
}"""


def run_refactor(date_str: str = None) -> dict:
    """
    改修×評価ループのメイン実行
    """
    from agents.evaluator import backtest, evaluate_change
    from agents.reporter  import _send, report_error

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    print(f"[refactor] 改修ループ開始: {date_str}")

    client = anthropic.Anthropic()

    # Step1: ベースライン評価
    current_params = _load_current_params()
    print(f"[refactor] 現在のパラメータ: {current_params}")
    baseline = backtest(current_params)
    if 'error' in baseline:
        print(f"[refactor] ベースライン評価エラー: {baseline['error']}")
        return {'error': baseline['error']}

    print(f"[refactor] ベースライン: 的中率{baseline['hit_rate']:.1%} 回収率{baseline['recovery_rate']:.1%}")

    # Step2: 改修ループ
    best_params  = current_params.copy()
    best_result  = baseline.copy()
    history      = []
    consecutive_bad = 0

    for i in range(MAX_ITER):
        print(f"\n[refactor] === イテレーション {i+1}/{MAX_ITER} ===")

        # Claude APIで改善案を生成
        proposal = _propose_change(client, current_params, best_result, history)
        if not proposal:
            print("[refactor] 改善案の生成に失敗")
            break

        proposed_params = proposal['proposed_params']
        print(f"[refactor] 提案: {proposed_params}")
        print(f"[refactor] 理由: {proposal['reasoning']}")

        # 評価
        new_result = backtest(proposed_params)
        if 'error' in new_result:
            print(f"[refactor] 評価エラー: {new_result['error']}")
            continue

        evaluation = evaluate_change(best_result, new_result)
        verdict    = evaluation['verdict']
        print(f"[refactor] 判定: {verdict}")
        print(f"[refactor] {evaluation['summary']}")

        # 履歴に追加
        history.append({
            'iter':     i + 1,
            'params':   proposed_params,
            'verdict':  verdict,
            'score':    evaluation['score'],
            'summary':  evaluation['summary'],
            'reasoning': proposal['reasoning'],
        })

        if verdict == '改善':
            best_params  = proposed_params.copy()
            best_result  = new_result.copy()
            current_params = proposed_params.copy()
            consecutive_bad = 0
            print(f"[refactor] ✅ 改善採用: {proposed_params}")
        elif verdict == '悪化':
            consecutive_bad += 1
            print(f"[refactor] ❌ 悪化却下 (連続悪化: {consecutive_bad}/{MAX_CONSECUTIVE_BAD})")
            if consecutive_bad >= MAX_CONSECUTIVE_BAD:
                print("[refactor] 連続悪化のため終了")
                break
        else:  # 誤差範囲
            consecutive_bad = 0
            print("[refactor] ⚪ 誤差範囲 → 現状維持")

    # Step3: 最終レポート作成
    improved = evaluate_change(baseline, best_result)
    report   = _format_report(baseline, best_result, best_params, history, improved)

    # Step4: Notionに記録
    _save_to_notion(date_str, best_params, improved, history)

    # Step5: Discordに通知（適用確認を促す）
    _send(report)

    print(f"\n[refactor] 改修ループ完了: {len(history)}回試行")
    return {
        'baseline':    baseline,
        'best_result': best_result,
        'best_params': best_params,
        'history':     history,
        'improved':    improved,
    }


def _load_current_params() -> dict:
    """現在の適用パラメータを読み込む（Notionまたはデフォルト）"""
    params_file = DATA_DIR / 'current_params.json'
    if params_file.exists():
        with open(params_file) as f:
            return json.load(f)
    return DEFAULT_PARAMS.copy()


def _propose_change(client, current_params: dict, current_result: dict,
                    history: list) -> dict | None:
    """Claude APIでパラメータ改善案を生成"""
    context = (
        f"現在のパラメータ: {json.dumps(current_params, ensure_ascii=False)}\n\n"
        f"現在の成績:\n"
        f"- 的中率: {current_result['hit_rate']:.1%}\n"
        f"- 回収率: {current_result['recovery_rate']:.1%}\n"
        f"- 条件A'回収率: {current_result.get('condition_a_recovery', 0):.1%}\n"
        f"- 参加レース数: {current_result['n_races']}R\n\n"
    )
    if history:
        context += "過去の試行履歴:\n"
        for h in history[-3:]:  # 直近3件のみ
            context += f"  [{h['iter']}] {h['params']} → {h['verdict']} ({h['reasoning'][:50]})\n"

    context += "\n次の改善案をJSONで提案してください。"

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=REFACTOR_SYSTEM,
            messages=[{"role": "user", "content": context}]
        )
        raw  = resp.content[0].text
        import re
        m    = re.search(r'\{.*\}', raw, re.DOTALL)
        if not m:
            return None
        data = json.loads(m.group())

        # バリデーション
        proposed = data.get('proposed_params', {})
        for k, v in proposed.items():
            if k not in DEFAULT_PARAMS:
                continue
            proposed[k] = max(0.1, min(3.0, float(v)))

        # 変更なしの場合はデフォルトに微変更
        if proposed == current_params:
            return None

        merged = {**current_params, **proposed}
        return {
            'proposed_params': merged,
            'reasoning':       data.get('reasoning', ''),
            'expected_effect': data.get('expected_effect', ''),
        }
    except Exception as e:
        print(f"[refactor] 提案生成エラー: {e}")
        return None


def _format_report(baseline: dict, best: dict, best_params: dict,
                   history: list, improved: dict) -> str:
    """Discord通知用レポート"""
    verdict = improved['verdict']
    icon    = '✅' if verdict == '改善' else '⚪' if verdict == '誤差範囲' else '❌'

    lines = [
        f"## 🔧 パラメータ改善レポート {datetime.datetime.now(JST).strftime('%Y/%m/%d')}",
        f"",
        f"**{icon} 最終判定: {verdict}**",
        f"試行回数: {len(history)}回",
        f"",
        f"**成績変化**",
        f"```",
        improved['summary'],
        f"```",
        f"",
        f"**最適パラメータ**",
        f"```json",
        json.dumps(best_params, ensure_ascii=False, indent=2),
        f"```",
    ]

    if history:
        lines += [
            f"",
            f"**試行履歴（上位3件）**",
        ]
        good = [h for h in history if h['verdict'] == '改善'][:3]
        for h in good:
            lines.append(f"- [{h['iter']}回目] {h['verdict']}: {h['reasoning'][:60]}")

    if verdict == '改善':
        lines += [
            f"",
            f"⚠️ **このパラメータを適用するにはActionsから `apply_params` を実行してください**",
        ]
    else:
        lines += [
            f"",
            f"ℹ️ 現在のパラメータが最適です。変更不要。",
        ]

    return '\n'.join(lines)


def _save_to_notion(date_str: str, best_params: dict,
                    improved: dict, history: list):
    """パラメータ更新履歴をNotionに記録"""
    import requests
    key = os.environ.get('NOTION_API_KEY', '')
    if not key:
        return

    # keiba-agentデータストアに子ページとして記録
    STORE_PAGE = "32a1333d-21b1-813e-a2c8-f18d52f3c7de"
    headers    = {
        "Authorization":  f"Bearer {key}",
        "Content-Type":   "application/json",
        "Notion-Version": "2022-06-28",
    }
    content = (
        f"判定: {improved['verdict']}\n"
        f"試行回数: {len(history)}\n\n"
        f"成績変化:\n{improved['summary']}\n\n"
        f"最適パラメータ:\n{json.dumps(best_params, ensure_ascii=False, indent=2)}"
    )
    payload = {
        "parent": {"page_id": STORE_PAGE},
        "properties": {
            "title": {"title": [{"text": {"content": f"params_{date_str}"}}]}
        },
        "children": [{
            "object": "block", "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": content[:2000]}}],
                "language":  "json"
            }
        }]
    }
    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers, json=payload, timeout=15
        )
        resp.raise_for_status()
        print(f"[refactor] Notion記録完了: params_{date_str}")
    except Exception as e:
        print(f"[refactor] Notion記録エラー: {e}")


def apply_params(params: dict):
    """
    承認されたパラメータをcurrent_params.jsonに保存
    （ユーザーがActionsから apply_params コマンドを実行したとき）
    """
    params_file = DATA_DIR / 'current_params.json'
    with open(params_file, 'w') as f:
        json.dump(params, f, ensure_ascii=False, indent=2)
    print(f"[refactor] パラメータ適用完了: {params}")

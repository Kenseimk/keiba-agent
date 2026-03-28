"""
multi_agent_optimizer.py - 多エージェント戦略最適化システム

2チーム × 2エージェント構成:
  穴馬チーム:
    思考エージェント  → パラメータ改善案を提案
    反証エージェント  → 過学習・リスクを指摘して反論
  複勝チーム（スコアリング型）:
    思考エージェント  → 重み・閾値の改善案を提案
    反証エージェント  → 過学習・リスクを指摘して反論

使い方:
    python multi_agent_optimizer.py           # 両チーム無限ループ
    python multi_agent_optimizer.py --iters 5 # 各チーム5イテレーション
    python multi_agent_optimizer.py --team ana # 穴馬チームのみ
"""
import sys, json, os, subprocess, copy, time, argparse
from datetime import datetime
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# backtest_core を同じディレクトリから import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest_core import load_data, run_ana_backtest, run_fukusho_backtest

LOG_FILE = 'optimization_log.json'  # デフォルト（--log で上書き可能）
MODEL    = 'claude-haiku-4-5-20251001'  # 速度重視（sonnet-4-6 に変更可能）

# ══════════════════════════════════════════════════════════
# 初期パラメータ
# ══════════════════════════════════════════════════════════
ANA_INIT_PARAMS = {
    'odds_min':   10,
    'odds_max':   30,
    'prob_min':   25.0,
    'count_max':  15,
    'field_min':  8,
    'pop_min':    4,
    'pop_max':    18,
    'kelly_tiers': [
        [35, 0.03, 20000],
        [30, 0.02, 15000],
        [0,  0.015, 8000],
    ],
}

FUKUSHO_INIT_PARAMS = {
    # 隠れ末脚型複勝: 前走最速上がりを出したが大敗した馬（市場の過小評価を狙う）
    'prev_f3rank_max':  1,     # 前走上がり3F: 1位のみ（最速）
    'prev_finish_min':  6,     # 前走着順: 6着以下（大敗）
    'prev_field_min':   8,     # 前走出走頭数下限
    'odds_min':        12.0,   # EVプラス帯: 12〜18倍
    'odds_max':        18.0,
    'pop_min':          5,
    'pop_max':         12,
    'field_min':        8,
    'count_max':       15,
    'kelly_pct':        0.020,
    'kelly_max':       12000,
}

# ══════════════════════════════════════════════════════════
# Claude エージェント呼び出し
# ══════════════════════════════════════════════════════════
def call_claude(prompt: str, max_retries=3) -> str:
    """claude -p でモデルを呼び出し、テキストを返す"""
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ['claude', '-p', '--model', MODEL],
                input=prompt, capture_output=True,
                text=True, encoding='utf-8', timeout=120
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
            time.sleep(2)
        except subprocess.TimeoutExpired:
            print(f'  [WARN] タイムアウト (試行 {attempt+1}/{max_retries})')
            time.sleep(5)
        except Exception as e:
            print(f'  [WARN] エラー: {e}')
            time.sleep(2)
    return '{}'


def extract_json(text: str) -> dict:
    """テキストから JSON ブロックを抽出"""
    # ```json ... ``` 形式
    import re
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if m:
        try: return json.loads(m.group(1))
        except: pass
    # 直接 JSON
    m = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if m:
        try: return json.loads(m.group(0))
        except: pass
    return {}


# ══════════════════════════════════════════════════════════
# 思考エージェント
# ══════════════════════════════════════════════════════════
ANA_THINKING_SYSTEM = """あなたは競馬バックテストの穴馬戦略最適化エキスパートです。
与えられたバックテスト結果とパラメータを分析し、ROIを改善する具体的な提案を1つだけ行ってください。

【戦略概要】
- 穴馬複勝: オッズ10〜30倍、4番人気以上の馬を対象に複勝を購入
- top3_prob モデルで複勝確率を推定し、閾値以上の馬を選択
- Conservative Kelly 配分

【提案ルール】
1. 変更は1パラメータのみ
2. 根拠は具体的な数値に基づくこと（感覚論は不可）
3. 過学習リスクを考慮すること

以下のJSONフォーマットのみで回答（説明文は不要）:
{
  "param": "変更するパラメータ名",
  "old_value": 現在値,
  "new_value": 提案値,
  "rationale": "根拠（データに基づく50字以内）",
  "expected_roi_change": "+X%"
}"""

FUKUSHO_THINKING_SYSTEM = """あなたは競馬バックテストの「隠れ末脚型複勝」戦略最適化エキスパートです。
与えられたバックテスト結果とパラメータを分析し、ROIを改善する具体的な提案を1つだけ行ってください。

【戦略概要】
隠れ末脚型複勝: 前走で最速上がり3Fを出したのに大敗した馬を狙う
- シグナル: prev_f3rank_max（前走上がり順位上限）かつ prev_finish_min（前走着順下限）
- 対象オッズ: 12〜18倍（グリッドサーチでEVプラスが確認された帯）
- 市場が「負けた」事実だけを見て過小評価する馬のEVを狙う

【チューニング可能なパラメータ】
- prev_f3rank_max: 1=最速のみ / 2=2位以内 / 3=3位以内（広げるとサンプル増だがシグナル希薄）
- prev_finish_min: 4〜8（大きいほど大敗馬のみ、小さいほど範囲広い）
- prev_field_min: 前走レースの最低頭数（小さいと小頭数レースを除外できない）
- odds_min/max: 12-18倍周辺が実績あり（EVプラス帯）
- pop_min/max: 5-12番人気周辺
- kelly_pct: 0.015〜0.025（軍資金比率）
- kelly_max: 上限額
- count_max: 月あたり件数上限

【提案ルール】
1. 変更は1パラメータのみ
2. 根拠は具体的な数値・理論に基づくこと（感覚論は不可）
3. サンプル数が月平均3件以下になる変更は避ける

以下のJSONフォーマットのみで回答（説明文は不要）:
{
  "param": "変更するパラメータ名",
  "old_value": 現在値,
  "new_value": 提案値,
  "rationale": "根拠（50字以内）",
  "expected_roi_change": "+X%"
}"""


def thinking_agent(strategy: str, params: dict, metrics: dict, history: list) -> dict:
    """思考エージェント: 改善案を1つ提案"""
    system = ANA_THINKING_SYSTEM if strategy == 'ana' else FUKUSHO_THINKING_SYSTEM
    hist_summary = '\n'.join(
        f'  イテ{i+1}: {h["proposal"].get("param")} {h["proposal"].get("old_value")}→{h["proposal"].get("new_value")} '
        f'→ ROI {h["old_metrics"]["roi"]:.1f}%→{h["result_metrics"]["roi"]:.1f}% ({h["verdict"]})'
        for i, h in enumerate(history[-5:])  # 直近5件
    ) or '  (なし)'

    prompt = f"""{system}

【現在のパラメータ】
{json.dumps(params, ensure_ascii=False, indent=2)}

【現在のバックテスト結果】
- ROI: {metrics['roi']:.1f}%
- 的中率: {metrics['hit_rate']:.1f}%
- ベット数: {metrics['count']}件
- 赤字月数: {metrics['red_months']}/23ヶ月
- 最悪月ROI: {metrics['worst_month_roi']:.1f}%
- 最終軍資金: ¥{metrics['final_capital']:,}

【直近の変更履歴】
{hist_summary}

上記を踏まえ、次の改善案を1つ提案してください。"""

    raw = call_claude(prompt)
    result = extract_json(raw)
    if not result:
        print(f'  [WARN] 思考エージェント JSON 抽出失敗: {raw[:100]}')
    return result


# ══════════════════════════════════════════════════════════
# 反証エージェント
# ══════════════════════════════════════════════════════════
ANA_REBUTTAL_SYSTEM = """あなたは競馬バックテストの批判的評価者（反証エージェント）です。
提案されたパラメータ変更を厳しく評価し、問題点・過学習リスクを指摘してください。

【評価基準】
- サンプル数が少なすぎる変更（月2件以下）は reject
- 赤字月が増える変更は reject
- ROI改善が1%未満は reject（誤差範囲）
- ロジックに矛盾がある変更は reject
- リスク管理を悪化させる変更は reject

以下のJSONフォーマットのみで回答:
{
  "verdict": "accept または reject",
  "risk_score": 1〜5（1=低リスク, 5=高リスク）,
  "criticism": "指摘事項（50字以内）",
  "counter_suggestion": "代替案があれば（なければ空文字）"
}"""

FUKUSHO_REBUTTAL_SYSTEM = """あなたは競馬バックテストの批判的評価者（反証エージェント）です。
「隠れ末脚型複勝」戦略への提案を厳しく評価し、問題点・過学習リスクを指摘してください。

【評価基準】
- ベット数が50件以下になる変更は reject（統計的信頼性が低い）
- 赤字月が2ヶ月以上増える変更は reject
- ROI改善が2%未満は reject（誤差範囲）
- ルックアヘッドバイアスを生む変更は reject（未来データの参照）
- オッズ帯をEVプラス帯（12-18倍）から大きく外す変更は高リスク
- ROI+3%以上かつベット数50件以上なら積極的に accept を検討すること

以下のJSONフォーマットのみで回答:
{
  "verdict": "accept または reject",
  "risk_score": 1〜5（1=低リスク, 5=高リスク）,
  "criticism": "指摘事項（50字以内）",
  "counter_suggestion": "代替案があれば（なければ空文字）"
}"""


def rebuttal_agent(strategy: str, proposal: dict,
                   old_metrics: dict, new_metrics: dict) -> dict:
    """反証エージェント: 提案を評価して accept/reject"""
    system = ANA_REBUTTAL_SYSTEM if strategy == 'ana' else FUKUSHO_REBUTTAL_SYSTEM

    roi_diff = new_metrics['roi'] - old_metrics['roi']
    red_diff = new_metrics['red_months'] - old_metrics['red_months']

    prompt = f"""{system}

【提案内容】
{json.dumps(proposal, ensure_ascii=False, indent=2)}

【変更前の結果】
- ROI: {old_metrics['roi']:.1f}%
- 的中率: {old_metrics['hit_rate']:.1f}%
- ベット数: {old_metrics['count']}件
- 赤字月: {old_metrics['red_months']}/23

【変更後の結果】
- ROI: {new_metrics['roi']:.1f}%（{roi_diff:+.1f}%）
- 的中率: {new_metrics['hit_rate']:.1f}%
- ベット数: {new_metrics['count']}件
- 赤字月: {new_metrics['red_months']}/23（{red_diff:+d}ヶ月）

この変更を採用すべきか評価してください。"""

    raw = call_claude(prompt)
    result = extract_json(raw)
    if not result:
        print(f'  [WARN] 反証エージェント JSON 抽出失敗: {raw[:100]}')
        result = {'verdict': 'reject', 'risk_score': 3, 'criticism': 'JSON解析失敗', 'counter_suggestion': ''}
    return result


# ══════════════════════════════════════════════════════════
# パラメータ適用
# ══════════════════════════════════════════════════════════
def apply_proposal(params: dict, proposal: dict) -> dict:
    """提案をパラメータに適用して新しいパラメータ dict を返す"""
    new_params = copy.deepcopy(params)
    param = proposal.get('param', '')
    new_value = proposal.get('new_value')
    if new_value is None:
        return new_params

    # ネストしたキー（weights.market 等）
    if '.' in param:
        parts = param.split('.', 1)
        if parts[0] in new_params and isinstance(new_params[parts[0]], dict):
            new_params[parts[0]][parts[1]] = new_value
    elif param in new_params:
        # kelly_tiers は [[thresh, pct, cap], ...] 形式を強制検証
        if param == 'kelly_tiers':
            if (isinstance(new_value, list) and
                    all(isinstance(row, list) and len(row) == 3 for row in new_value)):
                new_params[param] = new_value
            else:
                print(f'  [WARN] kelly_tiers の形式が不正のため却下: {new_value}')
                # パラメータを変更せずに返す
                return copy.deepcopy(params)
        else:
            new_params[param] = new_value

    # weights の正規化（合計を 1.0 に）
    if 'weights' in new_params:
        W = new_params['weights']
        total = sum(W.values())
        if total > 0:
            new_params['weights'] = {k: round(v / total, 4) for k, v in W.items()}

    return new_params


# ══════════════════════════════════════════════════════════
# ログ管理
# ══════════════════════════════════════════════════════════
def load_log() -> dict:
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {
        'ana':     {'best_params': ANA_INIT_PARAMS,     'best_metrics': None, 'history': []},
        'fukusho': {'best_params': FUKUSHO_INIT_PARAMS, 'best_metrics': None, 'history': []},
    }

def save_log(log: dict):
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════
# チーム最適化ループ（1イテレーション）
# ══════════════════════════════════════════════════════════
def run_iteration(strategy: str, log_entry: dict, races, jstats) -> bool:
    """
    1イテレーション実行。
    params が更新された場合 True を返す。
    """
    params  = log_entry['best_params']
    history = log_entry['history']
    tag     = '穴馬' if strategy == 'ana' else '複勝スコア'

    # ── 現在のメトリクス ──
    if log_entry['best_metrics'] is None:
        print(f'  [{tag}] 初期バックテスト実行中...')
        if strategy == 'ana':
            cur_metrics = run_ana_backtest(races, params)
        else:
            cur_metrics = run_fukusho_backtest(races, jstats, params)
        log_entry['best_metrics'] = cur_metrics
    else:
        cur_metrics = log_entry['best_metrics']

    print(f'  [{tag}] 現在 ROI={cur_metrics["roi"]:.1f}% '
          f'赤字月={cur_metrics["red_months"]} '
          f'件数={cur_metrics["count"]}')

    # ── 思考エージェント ──
    print(f'  [{tag}] 思考エージェント 提案中...')
    proposal = thinking_agent(strategy, params, cur_metrics, history)
    if not proposal or 'param' not in proposal:
        print(f'  [{tag}] 提案なし → スキップ')
        return False

    print(f'  [{tag}] 提案: {proposal.get("param")} '
          f'{proposal.get("old_value")} → {proposal.get("new_value")} '
          f'（{proposal.get("rationale", "")}）')

    # ── バックテスト実行 ──
    new_params = apply_proposal(params, proposal)
    if strategy == 'ana':
        new_metrics = run_ana_backtest(races, new_params)
    else:
        new_metrics = run_fukusho_backtest(races, jstats, new_params)

    roi_diff = new_metrics['roi'] - cur_metrics['roi']
    print(f'  [{tag}] 結果: ROI {cur_metrics["roi"]:.1f}% → {new_metrics["roi"]:.1f}% '
          f'({roi_diff:+.1f}%)')

    # ── 反証エージェント ──
    print(f'  [{tag}] 反証エージェント 評価中...')
    rebuttal = rebuttal_agent(strategy, proposal, cur_metrics, new_metrics)
    verdict  = rebuttal.get('verdict', 'reject')

    # ── ハードコード自動採用（反証エージェントの過保守を補正）──
    # 条件: ROI+4%以上 かつ ベット数50以上 かつ 赤字月+1以下
    auto_accept_threshold = 4.0 if strategy == 'fukusho' else 5.0
    count_ok  = new_metrics['count'] >= 50
    red_ok    = (new_metrics['red_months'] - cur_metrics['red_months']) <= 1
    if verdict == 'reject' and roi_diff >= auto_accept_threshold and count_ok and red_ok:
        verdict = 'accept'
        rebuttal['verdict'] = 'accept'
        rebuttal['criticism'] = f'[自動採用] ROI+{roi_diff:.1f}%かつ基準クリア'
        print(f'  [{tag}] ⚡ 自動採用条件クリア（ROI+{roi_diff:.1f}%, bets={new_metrics["count"]}）')

    print(f'  [{tag}] 判定: {verdict.upper()} '
          f'[リスク:{rebuttal.get("risk_score","?")}] '
          f'{rebuttal.get("criticism", "")}')

    # ── 履歴記録 ──
    history.append({
        'timestamp':    datetime.now().isoformat(),
        'proposal':     proposal,
        'new_params':   new_params,
        'old_metrics':  cur_metrics,
        'result_metrics': new_metrics,
        'rebuttal':     rebuttal,
        'verdict':      verdict,
    })

    # ── accept なら更新 ──
    if verdict == 'accept' and new_metrics['roi'] > cur_metrics['roi']:
        log_entry['best_params']  = new_params
        log_entry['best_metrics'] = new_metrics
        print(f'  [{tag}] ✅ パラメータ更新！ROI {cur_metrics["roi"]:.1f}% → {new_metrics["roi"]:.1f}%')
        if rebuttal.get('counter_suggestion'):
            print(f'  [{tag}]    反証: {rebuttal["counter_suggestion"]}')
        return True
    else:
        print(f'  [{tag}] ❌ 却下 → パラメータ維持')
        if rebuttal.get('counter_suggestion'):
            print(f'  [{tag}]    代替案: {rebuttal["counter_suggestion"]}')
        return False


# ══════════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════════
def print_summary(log: dict):
    print()
    print('=' * 65)
    print('=== 最適化サマリ ===')
    for strategy, tag in [('ana', '穴馬'), ('fukusho', '複勝スコア')]:
        e = log[strategy]
        m = e['best_metrics']
        if m:
            print(f'[{tag}チーム]')
            print(f'  ROI: {m["roi"]:.1f}%  的中率: {m["hit_rate"]:.1f}%  '
                  f'件数: {m["count"]}  赤字月: {m["red_months"]}/23')
            print(f'  最終資金: ¥{m["final_capital"]:,}')
            print(f'  イテレーション数: {len(e["history"])}')
        accepted = sum(1 for h in e['history'] if h['verdict'] == 'accept')
        print(f'  採用/提案: {accepted}/{len(e["history"])}')
    print('=' * 65)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--iters', type=int, default=0,
                        help='各チームのイテレーション数（0=無限）')
    parser.add_argument('--team', choices=['ana', 'fukusho', 'both'],
                        default='both', help='実行するチーム')
    parser.add_argument('--data', default='data', help='データディレクトリ')
    parser.add_argument('--log', default=None, help='ログファイルパス（省略時は optimization_log_<team>.json）')
    parser.add_argument('--reset', action='store_true',
                        help='ログをリセットして初期パラメータから再開')
    args = parser.parse_args()

    # ログファイル名の決定
    global LOG_FILE
    if args.log:
        LOG_FILE = args.log
    elif args.team != 'both':
        LOG_FILE = f'optimization_log_{args.team}.json'

    print('=' * 65)
    print('=== 多エージェント競馬戦略最適化システム ===')
    print(f'モデル: {MODEL}')
    print(f'チーム: {args.team}  イテレーション: {"無限" if args.iters == 0 else args.iters}')
    print(f'ログ: {LOG_FILE}')
    print('Ctrl+C で停止（途中でもログは保存されます）')
    print('=' * 65)
    print()

    # データ読み込み
    print('データ読み込み中...')
    races, jstats = load_data(args.data)
    print(f'{len(races)}レース / 騎手{len(jstats)}名\n')

    # ログ
    if args.reset and os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    log = load_log()

    # 対象チーム
    teams = []
    if args.team in ('ana',     'both'): teams.append('ana')
    if args.team in ('fukusho', 'both'): teams.append('fukusho')

    iter_count = {t: 0 for t in teams}
    iteration  = 0

    try:
        while True:
            iteration += 1
            print(f'\n{"─"*65}')
            print(f'イテレーション {iteration}  ({datetime.now().strftime("%H:%M:%S")})')
            print(f'{"─"*65}')

            for strategy in teams:
                if args.iters > 0 and iter_count[strategy] >= args.iters:
                    continue
                run_iteration(strategy, log[strategy], races, jstats)
                iter_count[strategy] += 1
                save_log(log)

            # 全チームが上限に達したら終了
            if args.iters > 0 and all(iter_count[t] >= args.iters for t in teams):
                break

    except KeyboardInterrupt:
        print('\n\n[中断] ログを保存して終了します...')

    save_log(log)
    print_summary(log)

    print(f'\nログファイル: {LOG_FILE}')
    print('最終ベストパラメータ:')
    for strategy, tag in [('ana', '穴馬'), ('fukusho', '複勝スコア')]:
        print(f'\n[{tag}]')
        print(json.dumps(log[strategy]['best_params'], ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

"""
agents/strategist.py  戦略エージェント（馬券配分専任）
selector.pyから馬券配分ロジックを分離
- controllerから掛け金上限を受け取る
- 分析結果をもとに最適な馬券・金額を決定
- ケース判定（1〜4）
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timezone, timedelta
JST = timezone(timedelta(hours=9))

# デフォルト上限（controllerから渡されない場合のフォールバック）
DEFAULT_LIMITS = {"単勝": 5000, "三連複": 1500, "三連単": 500}


def get_bet_limits() -> dict:
    """controllerを通じて今月の掛け金上限を取得"""
    try:
        now = datetime.now(JST)
        from agents.controller import get_limits
        return get_limits(now.year, now.month)
    except Exception as e:
        print(f"[strategist] 上限取得失敗: {e} → デフォルト使用")
        return DEFAULT_LIMITS


def build_bet_plan(analysis: dict, limits: dict = None) -> dict:
    """
    分析結果から馬券プランを生成
    Returns: {
        'case_label', 'bets': [{type, horse/horses, amount}],
        'total', 'remainder'
    }
    """
    if limits is None:
        limits = get_bet_limits()

    scores   = analysis['scores']
    best     = scores[0]
    second   = scores[1]
    third    = scores[2]
    gap      = analysis['gap']
    cond     = analysis['condition']

    tansho_limit  = limits.get('単勝',   5000)
    sanpuku_limit = limits.get('三連複',  1500)
    santan_limit  = limits.get('三連単',  500)
    total_budget  = tansho_limit + sanpuku_limit + santan_limit

    # ケース判定
    top1_odds = best['odds']
    top3_names = [s['name'] for s in scores[:3]]

    # ケース1：本命圧倒的（オッズ2倍台・スコア差5pt以上）
    if top1_odds < 3.0 and gap >= 5.0:
        case = "ケース1：本命固定"
        bets = [
            {'type': '単勝', 'horse': best['name'], 'amount': tansho_limit},
        ]

    # ケース2：本命明確（スコア差3pt以上）← 条件A'
    elif gap >= 3.0:
        case = "ケース2：本命明確（条件A'）"
        bets = [
            {'type': '単勝',  'horse': best['name'], 'amount': tansho_limit},
            {'type': '三連複', 'horses': top3_names,  'amount': sanpuku_limit},
        ]

    # ケース3：3頭絞れた・順番不明（スコア差1.5〜3pt）
    elif gap >= 1.5:
        case = "ケース3：3頭絞れた・順番不明"
        bets = [
            {'type': '単勝',  'horse': best['name'], 'amount': int(tansho_limit * 0.6)},
            {'type': '三連複', 'horses': top3_names,  'amount': sanpuku_limit},
            {'type': '三連単', 'horses': top3_names,  'amount': santan_limit},
        ]

    # ケース4：混戦（スコア差1.5pt未満）
    else:
        case = "ケース4：混戦"
        bets = [
            {'type': '三連複', 'horses': top3_names, 'amount': sanpuku_limit},
        ]

    total      = sum(b['amount'] for b in bets)
    remainder  = total_budget - total

    return {
        'case_label': case,
        'bets':       bets,
        'total':      total,
        'remainder':  remainder,
        'limits':     limits,
    }


def plan_all_races(analyzed_races: list[dict], limits: dict = None) -> list[dict]:
    """全レースに馬券プランを付与"""
    if limits is None:
        limits = get_bet_limits()

    result = []
    for race in analyzed_races:
        bet_plan  = build_bet_plan(race, limits)
        race_plan = {**race, 'bet': bet_plan}
        result.append(race_plan)
    return result


def format_strategy_output(planned_races: list[dict]) -> str:
    """Discord通知用テキスト生成"""
    lines = []
    for r in planned_races:
        bet  = r['bet']
        best = r['best']
        flag = '⚡ 割安フラグ: ' + r['scores'][2]['name'] + \
               ' 前走2着→今走5人気（割安候補）' \
               if any(s.get('flag') for s in r['scores'][:3]) else ''

        lines += [
            f"## {r['race_name']}（{r['course']}{r['dist']}m / {r['n_horses']}頭）",
            f"判定: ○{r['condition']}（参加） / スコア差: {r['gap']}pt",
            f"◎ 本命: {best['name']}（{best['jockey']}）{best['odds']}倍 {best['pop']}人気",
            f"○ 2着:  {r['scores'][1]['name']}（{r['scores'][1]['jockey']}）{r['scores'][1]['odds']}倍",
            f"▲ 3着:  {r['scores'][2]['name']}（{r['scores'][2]['jockey']}）{r['scores'][2]['odds']}倍",
        ]
        if flag:
            lines.append(f" {flag}")

        lines.append(f"\n**{bet['case_label']}**")
        for b in bet['bets']:
            if 'horses' in b:
                lines.append(f"  {b['type']} {'-'.join(b['horses'])} → {b['amount']:,}円")
            else:
                lines.append(f"  {b['type']} {b['horse']} → {b['amount']:,}円")
        lines.append(f"  合計: {bet['total']:,}円 / 残{bet['remainder']:,}円\n")

    return '\n'.join(lines) if lines else "本日は参加対象レースなし（条件C/A以上のレースがありませんでした）"

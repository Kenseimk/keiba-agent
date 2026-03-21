"""
agents/selector.py  エージェント①（選出）
score_v4.pyでスコア計算 → 条件判定 → 買い方提案
"""

import sys, json, re, numpy as np
sys.path.insert(0, '.')

from score_v4 import (
    load_models, calc_total_score_v4, parse_passage,
    judge_condition, recommend_bet, prev_rank_flag,
    calc_pace_factor, TEN3F_BASELINE
)


def parse_agari(s):
    try: return float(s)
    except: return None

def parse_bwchg(s):
    m = re.search(r'\(([+-]?\d+)\)', str(s))
    return int(m.group(1)) if m else 0

def parse_rank(s):
    try: return int(s)
    except: return None

def build_horse_stats(horse_id: str, history: list) -> dict:
    """戦績データから各種統計を計算"""
    agaris   = [parse_agari(r.get('agari','')) for r in history if parse_agari(r.get('agari',''))]
    passages = [parse_passage(r.get('passage','')) for r in history if r.get('passage')]
    passages = [p for p in passages if p is not None]
    ranks    = [parse_rank(r.get('rank','')) for r in history if parse_rank(r.get('rank',''))]

    return {
        'avg_agari':  round(np.mean(agaris[:3]), 2) if agaris else None,
        'avg_pos':    round(np.mean(passages[:5]), 1) if passages else None,
        'prev_rank':  ranks[0] if ranks else None,
        'prev_bwchg': parse_bwchg(history[0].get('weight','')) if history else 0,
    }

def score_race(race: dict, js, dc_db, budget: int = 10000) -> dict | None:
    """1レース分のスコア計算・条件判定・買い方提案"""
    odds_list = race.get('odds', [])
    histories = race.get('histories', {})
    horses_meta = {h['horse_id']: h for h in race.get('horses', [])}

    if not odds_list:
        return None

    n = race['n_horses']
    course = race['course']
    dist   = race['dist']

    # 各馬のスコア計算
    horses_data = []
    for o in odds_list:
        hid  = o.get('horse_id')
        name = o['name']
        pop  = o['pop']
        odds = o['odds']
        jockey = horses_meta.get(hid, {}).get('jockey', '') if hid else ''

        hist = histories.get(hid, []) if hid else []
        st   = build_horse_stats(hid, hist)

        # 上がりの相対評価用に後で計算するのでまず値だけ保存
        horses_data.append({
            'name':    name,
            'jockey':  jockey,
            'odds':    odds,
            'pop':     pop,
            'bw_chg':  st['prev_bwchg'],
            'agari':   st['avg_agari'],
            'avg_pos': st['avg_pos'],
            'prev_rank': st['prev_rank'],
        })

    # 上がり平均・標準偏差（レース内）
    agaris = [h['agari'] for h in horses_data if h['agari']]
    r_agari_mean = np.mean(agaris) if agaris else None
    r_agari_std  = np.std(agaris)  if agaris else None

    # スコア計算
    results = []
    for h in horses_data:
        sc = calc_total_score_v4(
            h['name'], h['jockey'], h['odds'], h['pop'], h['bw_chg'],
            js, dc_db,
            agari=h['agari'],
            race_agari_mean=r_agari_mean,
            race_agari_std=r_agari_std,
            avg_pos=h['avg_pos'],
            course=course,
            dist=dist,
            race_ten3f=None,   # 前日時点では不明
        )
        flag, flag_msg = prev_rank_flag(h['prev_rank'], h['pop'])
        etype = sc['breakdown']['脚質タイプ']

        results.append({
            'name':      h['name'],
            'jockey':    h['jockey'],
            'odds':      h['odds'],
            'pop':       h['pop'],
            'score':     sc['score'],
            'etype':     etype,
            'agari':     h['agari'],
            'agari_pt':  round(sc['breakdown']['上がり(補正後)'], 1),
            'pos_pt':    round(sc['breakdown']['脚質(補正後)'], 1),
            'dc_pt':     round(sc['breakdown']['同コース'], 1),
            'prev_rank': h['prev_rank'],
            'flag':      flag_msg,
        })

    results.sort(key=lambda x: x['score'], reverse=True)

    best = results[0]
    gap  = round(best['score'] - results[1]['score'], 1) if len(results) > 1 else 0
    cond = judge_condition(best['odds'], n, gap, dist)

    # 見送り
    if '見送り' in cond:
        return None

    # 買い方提案
    bet = recommend_bet(results, cond, budget)

    return {
        'race_id':   race['race_id'],
        'race_name': race['race_name'],
        'course':    course,
        'dist':      dist,
        'n_horses':  n,
        'condition': cond,
        'gap':       gap,
        'scores':    results,
        'best':      best,
        'bet':       bet,
    }

def run_selector(races_data: dict, budget: int = 10000) -> list[dict]:
    """全候補レースのスコア計算・選出を実行"""
    js, dc_db = load_models('data')
    candidates = races_data.get('candidates', [])

    selected = []
    for race in candidates:
        result = score_race(race, js, dc_db, budget)
        if result:
            selected.append(result)
            print(f"[selector] ✅ {result['race_name']} → {result['condition']}")
        else:
            print(f"[selector] ❌ {race.get('race_name','')} → 見送り")

    return selected

def format_selector_output(selected: list[dict]) -> str:
    """Discord通知用のテキスト整形"""
    if not selected:
        return "本日は参加対象レースなし（条件C/A以上のレースがありませんでした）"

    lines = []
    for r in selected:
        best = r['best']
        bet  = r['bet']
        lines.append(f"## {r['race_name']} ({r['course']}{r['dist']}m / {r['n_horses']}頭)")
        lines.append(f"判定: **{r['condition']}** / スコア差: {r['gap']}pt")
        lines.append(f"◎ 本命: **{best['name']}**（{best['jockey']}）{best['odds']}倍 {best['pop']}人気")
        lines.append(f"○ 2着: {r['scores'][1]['name']}（{r['scores'][1]['jockey']}）{r['scores'][1]['odds']}倍")
        lines.append(f"▲ 3着: {r['scores'][2]['name']}（{r['scores'][2]['jockey']}）{r['scores'][2]['odds']}倍")

        # フラグ馬
        flags = [s for s in r['scores'] if s.get('flag')]
        if flags:
            lines.append(f"⚡ 割安フラグ: {flags[0]['name']} {flags[0]['flag']}")

        # 買い方
        lines.append(f"\n**{bet['case_label']}**")
        for b in bet['bets']:
            if 'horses' in b:
                lines.append(f"  {b['type']} {'-'.join(b['horses'])} → {b['amount']:,}円")
            else:
                lines.append(f"  {b['type']} {b['horse']} → {b['amount']:,}円")
        lines.append(f"  合計: {bet['total']:,}円 / 残{bet['remainder']:,}円")
        lines.append("")

    return "\n".join(lines)


if __name__ == '__main__':
    # テスト実行
    import glob
    files = sorted(glob.glob('data/races_*.json'))
    if files:
        with open(files[-1]) as f:
            data = json.load(f)
        selected = run_selector(data)
        print("\n" + "="*60)
        print(format_selector_output(selected))

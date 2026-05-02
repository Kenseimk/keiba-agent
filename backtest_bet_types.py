"""
backtest_bet_types.py  分布タイプ × 賭け方 ROI バックテスト
=============================================================
勝率分布の「形」(突出/やや集中/拮抗) ×  賭け方 の組み合わせで
どの戦略が最も高いROIを示すかを検証する。

分布タイプ判定:
  突出      : max_prob >= 20%
  やや集中  : 15% <= max_prob < 20%
  拮抗      : max_prob < 15%

賭け方:
  単勝          top1       1点  (100円)
  複勝          top1       1点  (100円)
  ワイド        top1-2     1点  (100円)
  馬連          top1-2     1点  (100円)
  馬単          top1→2     1点  (100円)
  3連複top3     top1-2-3   1点  (100円)
  3連複top4BOX  top4中3    4点  (400円)
  3連複top5BOX  top5中3   10点 (1000円)
  3連単top3全   top3全順   6点  (600円)

使い方:
  python backtest_bet_types.py
  python backtest_bet_types.py --test-start 202401 --test-end 202412
"""

import os, json, re, math, argparse
from collections import defaultdict, Counter
from itertools import combinations, permutations

import numpy as np

from score_agent_core import (
    load_models, compute_horse_factors, LOGISTIC_FACTORS,
    _float, _int, parse_avg_pos, parse_bw_change, parse_margin, parse_time,
)
from backtest_agent import load_all_races
from walkforward_winprob import (
    build_db_upto, add_month_to_db, race_relative_features,
    predict_prob, load_model,
)


# ─── 払い戻しパーサ ───────────────────────────────────────

def _parse_tansho(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+):(\d+)', part.strip())
        if m:
            out[m.group(1)] = int(m.group(2))
    return out

def _parse_pair(raw: str) -> dict:
    """馬連/ワイド: '馬番 - 馬番:払戻|...' → {frozenset: 払戻}"""
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-–]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2)])] = int(m.group(3))
    return out

def _parse_umatan(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[→>]+\s*(\d+):(\d+)', part.strip())
        if m:
            out[(m.group(1), m.group(2))] = int(m.group(3))
    return out

def _parse_sanrenpuku(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-–]\s*(\d+)\s*[-–]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out

def _parse_sanrentan(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[→>]+\s*(\d+)\s*[→>]+\s*(\d+):(\d+)', part.strip())
        if m:
            out[(m.group(1), m.group(2), m.group(3))] = int(m.group(4))
    return out

def get_payouts(info: dict) -> dict:
    for h in info['horses']:
        if h.get('単勝払戻'):
            return {
                'tansho':     _parse_tansho(h.get('単勝払戻', '')),
                'fukusho':    _parse_tansho(h.get('複勝払戻', '')),
                'umaren':     _parse_pair(h.get('馬連払戻', '')),
                'umatan':     _parse_umatan(h.get('馬単払戻', '')),
                'wide':       _parse_pair(h.get('ワイド払戻', '')),
                'sanrenpuku': _parse_sanrenpuku(h.get('三連複払戻', '')),
                'sanrentan':  _parse_sanrentan(h.get('三連単払戻', '')),
            }
    return {}


# ─── 分布タイプ ────────────────────────────────────────────

def dist_type(max_prob_pct: float) -> str:
    if max_prob_pct >= 20.0:
        return '突出'
    if max_prob_pct >= 15.0:
        return 'やや集中'
    return '拮抗'


# ─── 1レースのベット評価 ────────────────────────────────────

BET_TYPES = ['単勝','複勝','ワイド','馬連','馬単','3連複top3','3連複top4','3連複top5','3連単top3']

def eval_bets(ranked: list, payouts: dict, actual_ranks: dict) -> dict:
    name2ban   = {e['name']: e['umaban'] for e in ranked}
    rank_map   = {v: k for k, v in actual_ranks.items()}
    top3_bans  = frozenset(
        filter(None, [name2ban.get(rank_map.get(i,''),'') for i in [1,2,3]])
    )
    winner_ban = name2ban.get(rank_map.get(1,''), '')
    sec_ban    = name2ban.get(rank_map.get(2,''), '')

    def b(i):  # 予測i位の馬番 (1始まり)
        return name2ban.get(ranked[i-1]['name'], '') if len(ranked) >= i else ''

    results = {}

    # 単勝
    payout = payouts.get('tansho', {}).get(b(1), 0)
    results['単勝'] = (100, payout)

    # 複勝
    payout = payouts.get('fukusho', {}).get(b(1), 0)
    results['複勝'] = (100, payout)

    # ワイド top1-2
    key = frozenset([b(1), b(2)])
    payout = payouts.get('wide', {}).get(key, 0) if b(1) and b(2) else 0
    results['ワイド'] = (100, payout)

    # 馬連 top1-2
    key = frozenset([b(1), b(2)])
    payout = payouts.get('umaren', {}).get(key, 0) if b(1) and b(2) else 0
    results['馬連'] = (100, payout)

    # 馬単 top1→top2
    key = (b(1), b(2))
    payout = payouts.get('umatan', {}).get(key, 0) if b(1) and b(2) else 0
    results['馬単'] = (100, payout)

    # 3連複 top3 (1点)
    key = frozenset([b(1), b(2), b(3)])
    payout = payouts.get('sanrenpuku', {}).get(key, 0) if len(key) == 3 else 0
    results['3連複top3'] = (100, payout)

    # 3連複 top4ボックス (C(4,3)=4点)
    top4 = [b(i) for i in range(1,5) if b(i)]
    cost4 = len(list(combinations(top4, 3))) * 100 if len(top4) >= 3 else 0
    pay4  = 0
    if cost4:
        for combo in combinations(top4, 3):
            if frozenset(combo) == top3_bans and len(top3_bans) == 3:
                pay4 = payouts.get('sanrenpuku', {}).get(top3_bans, 0)
                break
    results['3連複top4'] = (cost4, pay4)

    # 3連複 top5ボックス (C(5,3)=10点)
    top5 = [b(i) for i in range(1,6) if b(i)]
    cost5 = len(list(combinations(top5, 3))) * 100 if len(top5) >= 3 else 0
    pay5  = 0
    if cost5:
        for combo in combinations(top5, 3):
            if frozenset(combo) == top3_bans and len(top3_bans) == 3:
                pay5 = payouts.get('sanrenpuku', {}).get(top3_bans, 0)
                break
    results['3連複top5'] = (cost5, pay5)

    # 3連単 top3全通り (P(3,3)=6点)
    top3list = [b(i) for i in range(1,4) if b(i)]
    cost6 = len(list(permutations(top3list, 3))) * 100 if len(top3list) == 3 else 0
    pay6  = 0
    if cost6:
        for perm in permutations(top3list, 3):
            p = payouts.get('sanrentan', {}).get(tuple(perm), 0)
            if p:
                pay6 = p
                break
    results['3連単top3'] = (cost6, pay6)

    return results


# ─── バックテスト本体 ──────────────────────────────────────

DIST_TYPES = ['突出', 'やや集中', '拮抗']

def backtest(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std):
    # {dist_type: {bet_type: [invest, ret, hit, total]}}
    stats = {dt: {bt: [0, 0, 0, 0] for bt in BET_TYPES} for dt in DIST_TYPES}
    race_log = []

    for ym in yms:
        month_races = sorted(
            [rid for rid in all_races
             if rid[:6] == ym and int(rid[10:12]) in target_rnums]
        )
        for race_id in month_races:
            info   = all_races[race_id]
            dist   = info.get('dist') or 1800
            course = info.get('course') or 'ダート'
            track  = info.get('track_cond') or ''
            venue  = race_id[4:6]
            ym_str = race_id[:6]

            actual_ranks = {h['馬名']: _int(h.get('着順'))
                            for h in info['horses'] if _int(h.get('着順')) is not None}
            if not actual_ranks or min(actual_ranks.values()) != 1:
                continue

            payouts = get_payouts(info)
            if not payouts:
                continue

            race_entries = []
            for h in info['horses']:
                name = h['馬名']
                if _int(h.get('着順')) is None:
                    continue
                jockey = h.get('騎手', '')
                pop    = _int(h.get('人気'), 99)
                odds   = _float(h.get('単勝オッズ'), 0.0)
                hist   = horse_db.get(name, [])
                factors = compute_horse_factors(
                    name, jockey, course, dist, hist, jstats, dc_db,
                    track_cond=track, venue_code=venue, race_ym=ym_str,
                    pop=pop, odds=odds,
                )
                race_entries.append({
                    'name':    name,
                    'factors': factors,
                    'umaban':  h.get('馬番', '').strip(),
                })

            if len(race_entries) < 3:
                continue

            normed = race_relative_features([e['factors'] for e in race_entries])

            horse_probs = []
            for entry, fv_rel in zip(race_entries, normed):
                fv_n = (fv_rel - mean) / (std + 1e-8)
                prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
                horse_probs.append({'name': entry['name'], 'prob': prob, 'umaban': entry['umaban']})

            ranked = sorted(horse_probs, key=lambda x: -x['prob'])
            max_p  = ranked[0]['prob'] * 100
            dtype  = dist_type(max_p)

            bet_results = eval_bets(ranked, payouts, actual_ranks)

            for bt in BET_TYPES:
                invest, ret = bet_results.get(bt, (0, 0))
                if invest == 0:
                    continue
                stats[dtype][bt][0] += invest
                stats[dtype][bt][1] += ret
                stats[dtype][bt][2] += (1 if ret > 0 else 0)
                stats[dtype][bt][3] += 1

            race_log.append({'race_id': race_id, 'dtype': dtype, 'max_p': max_p})

        add_month_to_db(horse_db, all_races, ym)

    return stats, race_log


# ─── 表示 ─────────────────────────────────────────────────

def print_results(stats, race_log):
    dist_counts = Counter(r['dtype'] for r in race_log)
    print(f'\n  レース総数: {len(race_log)}レース')
    for dt in DIST_TYPES:
        print(f'    {dt}: {dist_counts.get(dt,0)}レース '
              f'({dist_counts.get(dt,0)/len(race_log)*100:.1f}%)')
    print()

    for dt in DIST_TYPES:
        thresh = '≥20%' if dt == '突出' else '15-20%' if dt == 'やや集中' else '<15%'
        print(f'{"="*65}')
        print(f'  【{dt}】  max_prob {thresh}  ({dist_counts.get(dt,0)}レース)')
        print(f'{"="*65}')
        print(f'  {"賭け方":<14} {"的中率":>7} {"ROI":>8}  {"結果":>10}  評価')
        print(f'  {"-"*55}')

        rows = []
        for bt in BET_TYPES:
            invest, ret, hit, total = stats[dt][bt]
            if total == 0:
                continue
            roi   = (ret - invest) / invest * 100
            hrate = hit / total * 100
            rows.append((roi, bt, hrate, hit, total))

        rows.sort(key=lambda x: -x[0])

        for i, (roi, bt, hrate, hit, total) in enumerate(rows):
            mark = ' ◀ 最良' if i == 0 else ''
            bar  = '★' * min(5, max(0, int((roi + 50) / 30)))
            print(f'  {bt:<14} {hrate:>6.1f}% {roi:>+8.1f}%  {hit:>4}/{total:<5}  {bar}{mark}')
        print()


# ─── メイン ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',       default='data')
    parser.add_argument('--test-start', default='202401', dest='test_start')
    parser.add_argument('--test-end',   default='',       dest='test_end')
    parser.add_argument('--rnum', nargs='+', type=int, default=[8,9,10,11])
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 65)
    print('  分布タイプ × 賭け方 ROI バックテスト')
    print('=' * 65)
    print(f'  テスト期間: {args.test_start}〜{args.test_end or "最新"}')
    print(f'  対象R    : {sorted(target_rnums)}')

    print('\n[0] データ・モデル読み込み...')
    all_races       = load_all_races(args.data)
    jstats, dc_db   = load_models(args.data)
    w, b, mean, std = load_model()
    print(f'  レース: {len(all_races)}件')

    print('\n[1] horse_db 構築...')
    horse_db = build_db_upto(all_races, args.test_start)
    print(f'  {len(horse_db)}頭')

    all_yms = sorted({rid[:6] for rid in all_races})
    yms = [ym for ym in all_yms
           if ym >= args.test_start and (not args.test_end or ym <= args.test_end)]
    print(f'\n[2] バックテスト実行 ({yms[0]}〜{yms[-1]})...')

    stats, race_log = backtest(
        all_races, horse_db, yms, target_rnums,
        jstats, dc_db, w, b, mean, std
    )

    print_results(stats, race_log)

    print('=' * 65)
    print('  最適戦略サマリー')
    print('=' * 65)
    print(f'  {"分布タイプ":<12} {"→ 推奨賭け方":<16} {"ROI":>8}  {"的中率":>7}')
    print(f'  {"-"*50}')
    for dt in DIST_TYPES:
        best = max(
            [(bt, stats[dt][bt]) for bt in BET_TYPES if stats[dt][bt][3] > 0],
            key=lambda x: (x[1][1]-x[1][0])/x[1][0],
            default=(None, None)
        )
        if best[0]:
            invest, ret, hit, total = best[1]
            roi   = (ret - invest) / invest * 100
            hrate = hit / total * 100
            print(f'  {dt:<12} → {best[0]:<16} {roi:>+8.1f}%  {hrate:>6.1f}%')
    print()


if __name__ == '__main__':
    main()

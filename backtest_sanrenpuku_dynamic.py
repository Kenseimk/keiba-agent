"""
backtest_sanrenpuku_dynamic.py
==============================
三連複BOXの点数を「勝率の累積確率」で動的決定するバックテスト。

戦略:
  モデル勝率を高い順に並べ、累積確率が閾値Tを超えるまで馬を追加し、
  そのN頭をBOXで三連複購入。

閾値候補: 50%, 55%, 60%, 65%, 70%, 75%, 80%
  例) 突出レースで threshold=65%:
      1位 prob=28% → cum 28%
      2位 prob=18% → cum 46%
      3位 prob=14% → cum 60%
      4位 prob=11% → cum 71% ← ≥65% で止まる → top4 BOX (4点)

分布タイプ別・閾値別のROIを比較する。
"""

import os, json, re, argparse
from collections import defaultdict, Counter
from itertools import combinations

import numpy as np

from score_agent_core import (
    load_models, compute_horse_factors, LOGISTIC_FACTORS,
    _float, _int,
)
from backtest_agent import load_all_races
from walkforward_winprob import (
    build_db_upto, add_month_to_db, race_relative_features,
    predict_prob, load_model,
)


# ─── 払い戻しパーサ ────────────────────────────────────────

def _parse_sanrenpuku(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-–]\s*(\d+)\s*[-–]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out

def get_sanrenpuku_payout(info: dict) -> dict:
    for h in info['horses']:
        if h.get('三連複払戻'):
            return _parse_sanrenpuku(h.get('三連複払戻', ''))
    return {}


# ─── 分布タイプ ───────────────────────────────────────────

def dist_type(max_prob_pct: float) -> str:
    if max_prob_pct >= 20.0:
        return '突出'
    if max_prob_pct >= 15.0:
        return 'やや集中'
    return '拮抗'


# ─── 動的N決定 ───────────────────────────────────────────

def get_n_by_threshold(ranked: list, threshold: float, min_n=3, max_n=8) -> int:
    """累積確率がthresholdを超える最小N頭数 (min_n ≤ N ≤ max_n)"""
    cum = 0.0
    for i, h in enumerate(ranked):
        cum += h['prob']
        if i + 1 >= min_n and cum >= threshold:
            return i + 1
    return min(len(ranked), max_n)


# ─── バックテスト ─────────────────────────────────────────

THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
DIST_TYPES = ['突出', 'やや集中', '拮抗']

def backtest(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std):
    # {dist_type: {threshold: [invest, ret, hit, total, avg_n_sum]}}
    stats = {
        dt: {t: [0, 0, 0, 0, 0] for t in THRESHOLDS}
        for dt in DIST_TYPES
    }
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

            actual_ranks = {
                h['馬名']: _int(h.get('着順'))
                for h in info['horses']
                if _int(h.get('着順')) is not None
            }
            if not actual_ranks or min(actual_ranks.values()) != 1:
                continue

            sanrenpuku_payouts = get_sanrenpuku_payout(info)
            if not sanrenpuku_payouts:
                continue

            # 実際の1-3着馬番
            rank_map = {v: k for k, v in actual_ranks.items()}
            name2ban = {h['馬名']: h.get('馬番', '').strip() for h in info['horses']}
            top3_bans = frozenset(filter(None, [
                name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]
            ]))
            if len(top3_bans) != 3:
                continue

            # 勝率計算
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
                horse_probs.append({
                    'name':   entry['name'],
                    'prob':   prob,
                    'umaban': entry['umaban'],
                })

            ranked = sorted(horse_probs, key=lambda x: -x['prob'])
            max_p  = ranked[0]['prob'] * 100
            dtype  = dist_type(max_p)

            # 各閾値でBOX評価
            for t in THRESHOLDS:
                n = get_n_by_threshold(ranked, t, min_n=3, max_n=8)
                topn_bans = [e['umaban'] for e in ranked[:n] if e['umaban']]
                combos = list(combinations(topn_bans, 3))
                if not combos:
                    continue
                invest = len(combos) * 100
                ret    = 0
                hit    = 0
                for combo in combos:
                    if frozenset(combo) == top3_bans:
                        payout = sanrenpuku_payouts.get(top3_bans, 0)
                        ret    = payout
                        hit    = 1
                        break

                stats[dtype][t][0] += invest
                stats[dtype][t][1] += ret
                stats[dtype][t][2] += hit
                stats[dtype][t][3] += 1
                stats[dtype][t][4] += n

            race_log.append({'race_id': race_id, 'dtype': dtype, 'max_p': max_p})

        add_month_to_db(horse_db, all_races, ym)

    return stats, race_log


# ─── 表示 ────────────────────────────────────────────────

def print_results(stats, race_log):
    dist_counts = Counter(r['dtype'] for r in race_log)
    total = len(race_log)
    print(f'\n  レース総数: {total}レース')
    for dt in DIST_TYPES:
        print(f'    {dt}: {dist_counts.get(dt,0)}レース '
              f'({dist_counts.get(dt,0)/total*100:.1f}%)')

    for dt in DIST_TYPES:
        thresh_label = '≥20%' if dt == '突出' else '15-20%' if dt == 'やや集中' else '<15%'
        n_races = dist_counts.get(dt, 0)
        print(f'\n{"="*72}')
        print(f'  【{dt}】  max_prob {thresh_label}  ({n_races}レース)')
        print(f'{"="*72}')
        print(f'  {"累積閾値":>7}  {"N頭(平均)":>8}  {"点数(平均)":>9}  {"的中率":>7}  {"ROI":>9}  {"結果":>10}')
        print(f'  {"-"*65}')

        rows = []
        for t in THRESHOLDS:
            invest, ret, hit, total_r, n_sum = stats[dt][t]
            if total_r == 0:
                continue
            roi   = (ret - invest) / invest * 100
            hrate = hit / total_r * 100
            avg_n = n_sum / total_r
            avg_cost = invest / total_r / 100  # 平均点数
            rows.append((t, avg_n, avg_cost, hrate, roi, hit, total_r))

        best_roi = max(r[4] for r in rows) if rows else -999
        for t, avg_n, avg_cost, hrate, roi, hit, total_r in rows:
            mark = ' ◀ 最良' if abs(roi - best_roi) < 0.01 else ''
            print(f'  {t*100:>6.0f}%  {avg_n:>8.2f}  {avg_cost:>9.1f}点  '
                  f'{hrate:>6.1f}%  {roi:>+9.1f}%  {hit:>4}/{total_r:<5}{mark}')

    # サマリー
    print(f'\n{"="*72}')
    print(f'  最適閾値サマリー')
    print(f'{"="*72}')
    print(f'  {"分布タイプ":<8}  {"最良閾値":>7}  {"平均N頭":>7}  {"平均点数":>8}  '
          f'{"的中率":>7}  {"ROI":>9}')
    print(f'  {"-"*65}')
    for dt in DIST_TYPES:
        rows = []
        for t in THRESHOLDS:
            invest, ret, hit, total_r, n_sum = stats[dt][t]
            if total_r == 0:
                continue
            roi   = (ret - invest) / invest * 100
            hrate = hit / total_r * 100
            avg_n = n_sum / total_r
            avg_cost = invest / total_r / 100
            rows.append((roi, t, avg_n, avg_cost, hrate, hit, total_r))
        if not rows:
            continue
        roi, t, avg_n, avg_cost, hrate, hit, total_r = max(rows, key=lambda x: x[0])
        print(f'  {dt:<8}  {t*100:>6.0f}%  {avg_n:>7.2f}頭  {avg_cost:>7.1f}点  '
              f'{hrate:>6.1f}%  {roi:>+9.1f}%  ({hit}/{total_r})')
    print()


# ─── メイン ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',       default='data')
    parser.add_argument('--test-start', default='202401', dest='test_start')
    parser.add_argument('--test-end',   default='',       dest='test_end')
    parser.add_argument('--rnum', nargs='+', type=int, default=[8, 9, 10, 11])
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 72)
    print('  三連複BOX動的点数バックテスト (累積勝率閾値)')
    print('=' * 72)
    print(f'  テスト期間: {args.test_start}〜{args.test_end or "最新"}')
    print(f'  対象R    : {sorted(target_rnums)}')
    print(f'  閾値候補  : {[f"{t*100:.0f}%" for t in THRESHOLDS]}')
    print(f'  BOX範囲   : 3〜8頭')

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


if __name__ == '__main__':
    main()

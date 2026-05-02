"""
backtest_sanrenpuku_ev.py
=========================
三連複BOXを「モデル確率×期待払戻」でEV評価してからベット判断。

N頭決定 : モデル累積確率閾値 T_cum (複数閾値をグリッド探索)
期待払戻 : 市場単勝オッズ → Harvilleで三連複オッズ推定
           est_payout(a,b,c) = 0.75 / P_market_harville(a,b,c in top3) * 100
EV計算   : EV = Σ_{combo ∈ C(N,3)} [ P_model(combo) × est_payout(combo) ] - cost
ベット条件: EV / cost > min_roi (複数閾値をグリッド探索)

出力: 分布タイプ × (T_cum, min_roi) の ROI 表
"""

import os, json, re, argparse
from collections import defaultdict, Counter
from itertools import combinations, permutations

import numpy as np

from score_agent_core import (
    load_models, compute_horse_factors,
    _float, _int,
)
from backtest_agent import load_all_races
from walkforward_winprob import (
    build_db_upto, add_month_to_db, race_relative_features,
    predict_prob, load_model,
)


# ─── Harvilleモデル ────────────────────────────────────────────

def harville_top3_prob(probs: dict, combo: tuple) -> float:
    """
    3頭 combo が任意順で 1-2-3着に入る確率（Harvilleモデル）
    probs: {name: win_prob} 正規化済み
    """
    a, b, c = combo
    pa = probs.get(a, 1e-9)
    pb = probs.get(b, 1e-9)
    pc = probs.get(c, 1e-9)
    total = 0.0
    for x, y, z in permutations([pa, pb, pc]):
        rem1 = 1.0 - x
        rem2 = rem1 - y
        if rem1 <= 0 or rem2 <= 0:
            continue
        total += x * (y / rem1) * (z / rem2)
    return total


def market_probs_from_odds(horses: list) -> dict:
    """
    単勝オッズからHarville用の市場勝率を推定 (正規化)。
    horses: [{'name':..., 'odds':float}, ...]
    """
    raw = {}
    for h in horses:
        o = h.get('odds', 0.0)
        if o and o > 0:
            raw[h['name']] = 1.0 / o
    total = sum(raw.values()) or 1.0
    return {n: v / total for n, v in raw.items()}


def model_probs_normalized(ranked: list) -> dict:
    """モデル勝率を正規化 (Harville計算用)"""
    total = sum(h['prob'] for h in ranked) or 1.0
    return {h['name']: h['prob'] / total for h in ranked}


# ─── 払い戻しパーサ ────────────────────────────────────────────

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


# ─── 分布タイプ ────────────────────────────────────────────────

def dist_type(max_prob_pct: float) -> str:
    if max_prob_pct >= 20.0:
        return '突出'
    if max_prob_pct >= 15.0:
        return 'やや集中'
    return '拮抗'


# ─── N頭決定 (累積閾値) ────────────────────────────────────────

def get_n_by_threshold(ranked: list, threshold: float, min_n=3, max_n=8) -> int:
    cum = 0.0
    for i, h in enumerate(ranked):
        cum += h['prob']
        if i + 1 >= min_n and cum >= threshold:
            return i + 1
    return min(len(ranked), max_n)


# ─── グリッドパラメータ ─────────────────────────────────────────

CUM_THRESHOLDS = [0.55, 0.60, 0.65, 0.70, 0.75]  # N決定閾値
MIN_ROI_LEVELS = [-0.30, -0.20, -0.10, 0.0, 0.10, 0.20]  # EV/cost最低値
DIST_TYPES     = ['突出', 'やや集中', '拮抗']
SANRENPUKU_TAKE = 0.75  # 三連複 払戻率 (25%控除)


# ─── バックテスト ─────────────────────────────────────────────

def backtest(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std):
    # {dist_type: {(T_cum, min_roi): [invest, ret, hit, bet_count, total]}}
    key_pairs = [(t, r) for t in CUM_THRESHOLDS for r in MIN_ROI_LEVELS]
    stats = {
        dt: {kp: [0, 0, 0, 0, 0] for kp in key_pairs}
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
            rank_map  = {v: k for k, v in actual_ranks.items()}
            name2ban  = {h['馬名']: h.get('馬番', '').strip() for h in info['horses']}
            top3_bans = frozenset(filter(None, [
                name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]
            ]))
            if len(top3_bans) != 3:
                continue

            # モデル勝率計算
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
                    'name':   name,
                    'factors': factors,
                    'umaban': h.get('馬番', '').strip(),
                    'odds':   odds,
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
                    'odds':   entry['odds'],
                })

            ranked     = sorted(horse_probs, key=lambda x: -x['prob'])
            max_p      = ranked[0]['prob'] * 100
            dtype      = dist_type(max_p)

            # 市場勝率 & モデル勝率 (Harville用正規化)
            mkt_probs   = market_probs_from_odds(ranked)
            mdl_probs   = model_probs_normalized(ranked)

            race_log.append({'race_id': race_id, 'dtype': dtype, 'max_p': max_p})

            for t_cum in CUM_THRESHOLDS:
                n = get_n_by_threshold(ranked, t_cum, min_n=3, max_n=8)
                top_n     = ranked[:n]
                top_n_names = [e['name'] for e in top_n]
                top_n_bans  = [e['umaban'] for e in top_n if e['umaban']]

                combos = list(combinations(range(n), 3))
                if len(combos) == 0:
                    continue
                cost = len(combos) * 100

                # EV計算
                ev = 0.0
                for ci, cj, ck in combos:
                    na, nb, nc = top_n_names[ci], top_n_names[cj], top_n_names[ck]
                    # モデル確率でこの3頭が1-3着に入る確率
                    p_model = harville_top3_prob(mdl_probs, (na, nb, nc))
                    # 市場確率から推定払戻
                    p_mkt   = harville_top3_prob(mkt_probs, (na, nb, nc))
                    est_pay = (SANRENPUKU_TAKE / p_mkt * 100) if p_mkt > 1e-9 else 0
                    ev += p_model * est_pay - 100  # 期待損益(この1点あたり)

                ev_ratio = ev / cost  # EV / 投資額

                # 実際の的中判定
                top_n_ban_set = set(e['umaban'] for e in top_n if e['umaban'])
                hit = (top3_bans <= top_n_ban_set and len(top3_bans) == 3)
                actual_ret = 0
                if hit:
                    actual_ret = sanrenpuku_payouts.get(top3_bans, 0)

                for min_roi in MIN_ROI_LEVELS:
                    s = stats[dtype][(t_cum, min_roi)]
                    s[4] += 1  # 対象レース数
                    if ev_ratio >= min_roi:
                        s[0] += cost
                        s[1] += actual_ret
                        s[2] += (1 if hit else 0)
                        s[3] += 1  # 実際にベットしたレース数

        add_month_to_db(horse_db, all_races, ym)

    return stats, race_log


# ─── 表示 ─────────────────────────────────────────────────────

def print_results(stats, race_log):
    dist_counts = Counter(r['dtype'] for r in race_log)
    total = len(race_log)
    print(f'\n  レース総数: {total}レース')
    for dt in DIST_TYPES:
        print(f'    {dt}: {dist_counts.get(dt,0)}レース ({dist_counts.get(dt,0)/total*100:.1f}%)')

    for dt in DIST_TYPES:
        thresh_label = '≥20%' if dt == '突出' else '15-20%' if dt == 'やや集中' else '<15%'
        n_races = dist_counts.get(dt, 0)
        print(f'\n{"="*80}')
        print(f'  【{dt}】  max_prob {thresh_label}  ({n_races}レース)')
        print(f'{"="*80}')

        for t_cum in CUM_THRESHOLDS:
            print(f'\n  ── 累積閾値 {t_cum*100:.0f}% ──────────────────────────────────────')
            print(f'  {"EV足切り":>9}  {"ベット率":>7}  {"的中率":>7}  {"ROI":>9}  {"ベット/全"}')
            print(f'  {"-"*60}')

            best_roi = -999
            rows = []
            for min_roi in MIN_ROI_LEVELS:
                s = stats[dt][(t_cum, min_roi)]
                invest, ret, hit, bet_cnt, total_cnt = s
                if bet_cnt == 0:
                    continue
                roi    = (ret - invest) / invest * 100
                hrate  = hit / bet_cnt * 100
                brate  = bet_cnt / total_cnt * 100 if total_cnt > 0 else 0
                rows.append((roi, min_roi, hrate, brate, hit, bet_cnt, total_cnt))
                best_roi = max(best_roi, roi)

            for roi, min_roi, hrate, brate, hit, bet_cnt, total_cnt in rows:
                mark = ' ◀' if abs(roi - best_roi) < 0.01 else ''
                sign = '+' if min_roi >= 0 else ''
                print(f'  EV≥{sign}{min_roi*100:>5.0f}%  {brate:>6.1f}%  {hrate:>6.1f}%  '
                      f'{roi:>+9.1f}%  {bet_cnt}/{total_cnt}{mark}')

    # ─── グローバルサマリー ───
    print(f'\n{"="*80}')
    print(f'  ベスト組み合わせ (分布タイプ × 累積閾値 × EV足切り)')
    print(f'{"="*80}')
    print(f'  {"分布":6}  {"累積T":6}  {"EV足切":8}  {"ベット率":7}  {"的中率":7}  {"ROI":9}  {"ベット数"}')
    print(f'  {"-"*70}')

    for dt in DIST_TYPES:
        best = None
        for t_cum in CUM_THRESHOLDS:
            for min_roi in MIN_ROI_LEVELS:
                s = stats[dt][(t_cum, min_roi)]
                invest, ret, hit, bet_cnt, total_cnt = s
                if bet_cnt < 20:  # 少なすぎは無視
                    continue
                roi   = (ret - invest) / invest * 100
                hrate = hit / bet_cnt * 100
                brate = bet_cnt / total_cnt * 100 if total_cnt > 0 else 0
                if best is None or roi > best[0]:
                    sign = '+' if min_roi >= 0 else ''
                    best = (roi, dt, t_cum, min_roi, hrate, brate, hit, bet_cnt, total_cnt, sign)
        if best:
            roi, dt_, t_cum, min_roi, hrate, brate, hit, bet_cnt, total_cnt, sign = best
            print(f'  {dt_:<6}  {t_cum*100:.0f}%     EV≥{sign}{min_roi*100:>4.0f}%  '
                  f'{brate:>6.1f}%  {hrate:>6.1f}%  {roi:>+9.1f}%  {bet_cnt}/{total_cnt}')
    print()


# ─── メイン ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',       default='data')
    parser.add_argument('--test-start', default='202401', dest='test_start')
    parser.add_argument('--test-end',   default='',       dest='test_end')
    parser.add_argument('--rnum', nargs='+', type=int, default=[8, 9, 10, 11])
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 80)
    print('  三連複BOX EV評価バックテスト')
    print('  N決定: モデル累積確率閾値 / EV判定: モデル確率×Harville推定払戻 vs コスト')
    print('=' * 80)
    print(f'  テスト期間: {args.test_start}〜{args.test_end or "最新"}')
    print(f'  対象R    : {sorted(target_rnums)}')
    print(f'  累積閾値  : {[f"{t*100:.0f}%" for t in CUM_THRESHOLDS]}')
    print(f'  EV足切り  : {[f"{r*100:+.0f}%" for r in MIN_ROI_LEVELS]}')

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

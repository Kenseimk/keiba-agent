"""
backtest_bet_switch.py
======================
突出レース(max_prob≥20%)で、勝率分布の形状から
「馬連」か「三連複top4BOX」かを選択する戦略のバックテスト。

切り替え変数候補:
  A) p2_p3_gap : 2位勝率 - 3位勝率  (大→馬連 / 小→三連複)
  B) top2_conc : (p1+p2)/(p1+p2+p3+p4)  (高→馬連 / 低→三連複)
  C) pop2      : 市場2位人気  (3以上→馬連シグナル / 1-2→三連複)
  D) p1_p2_gap : 1位勝率 - 2位勝率  (大→1頭突出=馬連 / 小→2頭均衡=三連複)

各変数の閾値をグリッドサーチし、最も高いROIの切り替えルールを探す。
比較ベースライン: 常に馬連 / 常に三連複top4
"""

import re, argparse
from collections import Counter
from itertools import combinations

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


# ─── 払い戻しパーサ ────────────────────────────────────────────

def _parse_pair(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-–]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2)])] = int(m.group(3))
    return out

def _parse_sanrenpuku(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-–]\s*(\d+)\s*[-–]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out

def get_payouts(info: dict) -> dict | None:
    for h in info['horses']:
        if h.get('単勝払戻'):
            return {
                'umaren':     _parse_pair(h.get('馬連払戻', '')),
                'sanrenpuku': _parse_sanrenpuku(h.get('三連複払戻', '')),
            }
    return None


# ─── バックテスト ─────────────────────────────────────────────

def backtest(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std):
    """
    Returns:
        records: list of dicts per race with features + results
    """
    records = []

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

            payouts = get_payouts(info)
            if not payouts:
                continue

            # 着順→馬名・馬番
            rank_map  = {v: k for k, v in actual_ranks.items()}
            name2ban  = {h['馬名']: h.get('馬番', '').strip() for h in info['horses']}
            top3_bans = frozenset(filter(None, [
                name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]
            ]))
            winner_ban = name2ban.get(rank_map.get(1, ''), '')
            sec_ban    = name2ban.get(rank_map.get(2, ''), '')

            # モデル勝率
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
                    'pop':    pop,
                })

            if len(race_entries) < 4:
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
                    'pop':    entry['pop'],
                })

            ranked = sorted(horse_probs, key=lambda x: -x['prob'])
            max_p  = ranked[0]['prob'] * 100

            # 突出のみ対象
            if max_p < 20.0:
                continue

            p = [h['prob'] * 100 for h in ranked]  # 上位から %
            p1 = p[0]
            p2 = p[1]
            p3 = p[2]
            p4 = p[3] if len(p) >= 4 else 0

            # ─── 切り替え変数 ───
            p2_p3_gap = p2 - p3                               # 変数A
            top2_conc = (p1 + p2) / (p1 + p2 + p3 + p4 + 1e-9)  # 変数B
            pop2      = ranked[1]['pop']                        # 変数C  (市場2位人気)
            p1_p2_gap = p1 - p2                               # 変数D

            # ─── 馬連的中 ───
            ban1, ban2 = ranked[0]['umaban'], ranked[1]['umaban']
            umaren_key = frozenset([ban1, ban2]) if ban1 and ban2 else frozenset()
            umaren_ret = payouts['umaren'].get(umaren_key, 0) if umaren_key else 0
            umaren_hit = 1 if umaren_ret > 0 else 0

            # ─── 三連複top4 BOX的中 ───
            top4_bans = [e['umaban'] for e in ranked[:4] if e['umaban']]
            san4_cost = len(list(combinations(top4_bans, 3))) * 100
            san4_ret  = 0
            san4_hit  = 0
            if len(top3_bans) == 3 and san4_cost > 0:
                for combo in combinations(top4_bans, 3):
                    if frozenset(combo) == top3_bans:
                        san4_ret = payouts['sanrenpuku'].get(top3_bans, 0)
                        san4_hit = 1 if san4_ret > 0 else 0
                        break

            records.append({
                'race_id':    race_id,
                'max_p':      max_p,
                'p2_p3_gap':  p2_p3_gap,
                'top2_conc':  top2_conc,
                'pop2':       pop2,
                'p1_p2_gap':  p1_p2_gap,
                # 馬連
                'umaren_invest': 100,
                'umaren_ret':    umaren_ret,
                'umaren_hit':    umaren_hit,
                # 三連複top4
                'san4_invest':   san4_cost,
                'san4_ret':      san4_ret,
                'san4_hit':      san4_hit,
            })

        add_month_to_db(horse_db, all_races, ym)

    return records


# ─── 評価 ─────────────────────────────────────────────────────

def eval_switch(records, switch_var: str, threshold: float, high_is_umaren: bool):
    """
    switch_var の値が threshold より (high_is_umaren=True なら高い側) = 馬連
    それ以外 = 三連複top4
    Returns: (invest, ret, hit, n_umaren, n_san4)
    """
    invest = ret = hit = n_u = n_s = 0
    for r in records:
        val = r[switch_var]
        go_umaren = (val >= threshold) if high_is_umaren else (val < threshold)
        if go_umaren:
            invest += r['umaren_invest']
            ret    += r['umaren_ret']
            hit    += r['umaren_hit']
            n_u    += 1
        else:
            invest += r['san4_invest']
            ret    += r['san4_ret']
            hit    += r['san4_hit']
            n_s    += 1
    return invest, ret, hit, n_u, n_s


def roi(invest, ret):
    return (ret - invest) / invest * 100 if invest > 0 else -100.0


# ─── グリッドサーチ ───────────────────────────────────────────

SWITCH_CONFIGS = [
    # (var_name, 表示名, thresholds, high_is_umaren)
    ('p2_p3_gap',  '2位-3位差(%)',   [1,2,3,4,5,6,7,8], True),   # 差が大→馬連
    ('top2_conc',  'top2集中度',     [0.50,0.55,0.60,0.65,0.70,0.75], True),  # 高→馬連
    ('pop2',       '市場2位人気',    [2,3,4,5],         False),   # pop2<閾値→馬連シグナルなし→三連複
    ('p1_p2_gap',  '1位-2位差(%)',   [2,4,6,8,10,12],   True),   # 差が大→1頭突出→馬連
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',       default='data')
    parser.add_argument('--test-start', default='202401', dest='test_start')
    parser.add_argument('--test-end',   default='',       dest='test_end')
    parser.add_argument('--rnum', nargs='+', type=int, default=[8, 9, 10, 11])
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 72)
    print('  馬連 vs 三連複top4 切り替え戦略 バックテスト')
    print('  対象: 突出レース (max_prob ≥ 20%) のみ')
    print('=' * 72)
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

    records = backtest(
        all_races, horse_db, yms, target_rnums,
        jstats, dc_db, w, b, mean, std
    )

    n = len(records)
    print(f'\n  突出レース数: {n}レース\n')

    # ─── ベースライン ───
    u_inv = sum(r['umaren_invest'] for r in records)
    u_ret = sum(r['umaren_ret']    for r in records)
    u_hit = sum(r['umaren_hit']    for r in records)
    s_inv = sum(r['san4_invest']   for r in records)
    s_ret = sum(r['san4_ret']      for r in records)
    s_hit = sum(r['san4_hit']      for r in records)

    print('=' * 72)
    print('  ベースライン (常に同じ賭け方)')
    print('=' * 72)
    print(f'  {"戦略":<16} {"的中率":>7}  {"ROI":>9}  {"的中/総数"}')
    print(f'  {"-"*50}')
    print(f'  {"常に馬連":<16} {u_hit/n*100:>6.1f}%  {roi(u_inv,u_ret):>+9.1f}%  {u_hit}/{n}')
    print(f'  {"常に三連複top4":<16} {s_hit/n*100:>6.1f}%  {roi(s_inv,s_ret):>+9.1f}%  {s_hit}/{n}')

    # ─── 切り替えグリッドサーチ ───
    all_results = []
    for var, label, thresholds, high_is_umaren in SWITCH_CONFIGS:
        direction = '≥T→馬連' if high_is_umaren else '<T→馬連'
        print(f'\n{"="*72}')
        print(f'  切り替え変数: {label}  ({direction}, <T→三連複)')
        print(f'{"="*72}')
        print(f'  {"閾値":>8}  {"馬連N":>6}  {"三連N":>6}  {"的中率":>7}  {"ROI":>9}  対ベースライン差')
        print(f'  {"-"*65}')

        base_roi_u = roi(u_inv, u_ret)
        rows = []
        for th in thresholds:
            inv, ret_v, hit, nu, ns = eval_switch(records, var, th, high_is_umaren)
            r_val = roi(inv, ret_v)
            diff  = r_val - base_roi_u
            hrate = hit / n * 100
            rows.append((r_val, th, nu, ns, hrate, diff, inv, ret_v, hit))
            all_results.append({
                'var': var, 'label': label, 'threshold': th,
                'high_is_umaren': high_is_umaren,
                'roi': r_val, 'n_umaren': nu, 'n_san4': ns,
                'hit': hit, 'invest': inv, 'ret': ret_v,
            })

        best_roi_row = max(rows, key=lambda x: x[0])
        for r_val, th, nu, ns, hrate, diff, inv, ret_v, hit in rows:
            mark = ' ◀' if abs(r_val - best_roi_row[0]) < 0.01 else ''
            sign = '+' if diff >= 0 else ''
            print(f'  T={th:>6}  馬連{nu:>4}  三連{ns:>4}  {hrate:>6.1f}%  '
                  f'{r_val:>+9.1f}%  ({sign}{diff:.1f}%){mark}')

    # ─── 総合ベスト ───
    all_results.sort(key=lambda x: -x['roi'])
    print(f'\n{"="*72}')
    print(f'  総合ベスト TOP5')
    print(f'{"="*72}')
    print(f'  {"変数":<12}  {"閾値":>8}  {"馬連N":>5}  {"三連N":>5}  {"ROI":>9}  対馬連差')
    print(f'  {"-"*65}')
    base_roi_u = roi(u_inv, u_ret)
    for res in all_results[:5]:
        diff = res['roi'] - base_roi_u
        sign = '+' if diff >= 0 else ''
        direction = '≥T→馬連' if res['high_is_umaren'] else '<T→馬連'
        print(f'  {res["label"]:<12}  T={res["threshold"]:>6}  '
              f'{res["n_umaren"]:>5}  {res["n_san4"]:>5}  '
              f'{res["roi"]:>+9.1f}%  ({sign}{diff:.1f}%)')

    # ─── 推奨ルール確認 ───
    best = all_results[0]
    print(f'\n  推奨ルール: {best["label"]} {"≥" if best["high_is_umaren"] else "<"} '
          f'{best["threshold"]} → 馬連 / それ以外 → 三連複top4')
    inv2, ret2, hit2, nu2, ns2 = eval_switch(
        records, best['var'], best['threshold'], best['high_is_umaren']
    )
    print(f'  馬連選択: {nu2}レース  三連複選択: {ns2}レース')
    print(f'  的中率: {hit2/n*100:.1f}%  ROI: {roi(inv2,ret2):+.1f}%')
    print()


if __name__ == '__main__':
    main()

"""
backtest_san3_cond.py
=====================
三連複top4 BOXが収益になる「条件」を探索する。

突出レース (max_prob>=20%) を対象に、以下の変数で絞り込み条件をグリッドサーチ:
  N_HORSES   : 出走頭数 (少頭数ほどカバーしやすい)
  TOP4_CUM   : top4累積勝率 (高いほどtop4が支配的)
  P3_P4_GAP  : 3位-4位の勝率差 (大きいほど4位の信頼度高い)
  P4_P5_GAP  : 4位-5位の勝率差 (大きいほど5位以降が弱い)
  DIST       : 距離 (長距離ほど波乱が少ない?)
  COURSE     : 芝/ダート

各条件について:
  - 対象レース数, カバー率(top4に1-3着3頭が揃う率), ROI を表示
"""
import sys, re, argparse
sys.stdout.reconfigure(encoding='utf-8')
from collections import defaultdict
from itertools import combinations

import numpy as np

from score_agent_core import load_models, compute_horse_factors, _float, _int
from backtest_agent import load_all_races
from walkforward_winprob import (
    build_db_upto, add_month_to_db,
    race_relative_features, predict_prob, load_model,
)


def _parse_sanrenpuku(raw: str) -> dict:
    out = {}
    for part in raw.split('|'):
        m = re.match(r'(\d+)\s*[-\u2013]\s*(\d+)\s*[-\u2013]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out

def get_san_payout(info):
    for h in info['horses']:
        if h.get('三連複払戻'):
            return _parse_sanrenpuku(h.get('三連複払戻', ''))
    return {}


# ─── メインバックテスト ──────────────────────────────────────────

def run(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std):
    """
    各レースの特徴量と三連複top4結果を収集して返す。
    """
    records = []

    for ym in yms:
        month_races = sorted(
            r for r in all_races
            if r[:6] == ym and int(r[10:12]) in target_rnums
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

            san_pays = get_san_payout(info)
            if not san_pays:
                continue

            rank_map  = {v: k for k, v in actual_ranks.items()}
            name2ban  = {h['馬名']: h.get('馬番', '').strip() for h in info['horses']}
            top3_bans = frozenset(filter(None, [
                name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]
            ]))
            if len(top3_bans) != 3:
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
                    'name':   name,
                    'factors': factors,
                    'umaban': h.get('馬番', '').strip(),
                })

            if len(race_entries) < 5:
                continue

            normed = race_relative_features([e['factors'] for e in race_entries])
            horse_probs = []
            for entry, fv_rel in zip(race_entries, normed):
                fv_n = (fv_rel - mean) / (std + 1e-8)
                prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
                horse_probs.append({'name': entry['name'], 'prob': prob, 'umaban': entry['umaban']})

            ranked   = sorted(horse_probs, key=lambda x: -x['prob'])
            n_horses = len(ranked)
            max_p    = ranked[0]['prob'] * 100
            if max_p < 20.0:
                continue  # 突出のみ

            probs    = [h['prob'] * 100 for h in ranked]
            p1, p2, p3, p4 = probs[0], probs[1], probs[2], probs[3]
            p5       = probs[4] if len(probs) >= 5 else 0.0
            gap      = p1 - p2
            p3_p4_gap = p3 - p4
            p4_p5_gap = p4 - p5
            top4_cum  = p1 + p2 + p3 + p4

            # 三連複 top4 BOX
            top4_bans = [e['umaban'] for e in ranked[:4] if e['umaban']]
            combos    = list(combinations(top4_bans, 3))
            if not combos:
                continue
            invest = len(combos) * 100  # 4点 = 400円

            # カバー判定 (top4に1-3着3頭が揃うか)
            top4_names = {e['name'] for e in ranked[:4]}
            top3_names = {rank_map.get(i, '') for i in [1, 2, 3]}
            covered    = len(top3_names & top4_names)

            ret = 0
            for combo in combos:
                if frozenset(combo) == top3_bans:
                    ret = san_pays.get(top3_bans, 0)
                    break

            records.append({
                'race_id':   race_id,
                'dist':      dist,
                'course':    course,
                'n_horses':  n_horses,
                'max_p':     max_p,
                'gap':       gap,
                'top4_cum':  top4_cum,
                'p3_p4_gap': p3_p4_gap,
                'p4_p5_gap': p4_p5_gap,
                'invest':    invest,
                'ret':       ret,
                'hit':       1 if ret > 0 else 0,
                'covered':   covered,
            })

        add_month_to_db(horse_db, all_races, ym)

    return records


# ─── 条件グリッドサーチ ──────────────────────────────────────────

def grid_search(records):
    n_total = len(records)
    print(f'\n  全突出レース: {n_total}件')
    base_inv = sum(r['invest'] for r in records)
    base_ret = sum(r['ret'] for r in records)
    base_roi = (base_ret - base_inv) / base_inv * 100
    base_cov = sum(1 for r in records if r['covered'] == 3) / n_total * 100
    base_hit = sum(r['hit'] for r in records) / n_total * 100
    print(f'  ベースライン(条件なし): カバー率{base_cov:.1f}% 的中率{base_hit:.1f}% ROI{base_roi:+.1f}%')

    results = []

    # ─── 1. 頭数 ───
    print(f'\n{"="*70}')
    print(f'  [A] 頭数別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for max_n in [8, 9, 10, 11, 12, 13, 14]:
        sub = [r for r in records if r['n_horses'] <= max_n]
        if len(sub) < 10: continue
        _show(f'頭数≤{max_n}', sub, results)

    # ─── 2. top4累積確率 ───
    print(f'\n{"="*70}')
    print(f'  [B] top4累積確率別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for th in [45, 50, 55, 60, 65, 70]:
        sub = [r for r in records if r['top4_cum'] >= th]
        if len(sub) < 10: continue
        _show(f'top4_cum≥{th}%', sub, results)

    # ─── 3. p3-p4差 ───
    print(f'\n{"="*70}')
    print(f'  [C] 3位-4位確率差別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for th in [2, 3, 4, 5, 6]:
        sub = [r for r in records if r['p3_p4_gap'] >= th]
        if len(sub) < 10: continue
        _show(f'p3-p4≥{th}%', sub, results)

    # ─── 4. p4-p5差 ───
    print(f'\n{"="*70}')
    print(f'  [D] 4位-5位確率差別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for th in [1, 2, 3, 4, 5]:
        sub = [r for r in records if r['p4_p5_gap'] >= th]
        if len(sub) < 10: continue
        _show(f'p4-p5≥{th}%', sub, results)

    # ─── 5. 距離 ───
    print(f'\n{"="*70}')
    print(f'  [E] 距離別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for lo, hi, label in [(1000,1400,'短距離<1400'),(1400,1800,'マイル1400-1800'),(1800,2200,'中距離1800-2200'),(2200,4000,'長距離≥2200')]:
        sub = [r for r in records if lo <= r['dist'] < hi]
        if len(sub) < 10: continue
        _show(label, sub, results)

    # ─── 6. 芝/ダート ───
    print(f'\n{"="*70}')
    print(f'  [F] コース別')
    print(f'{"="*70}')
    print(f'  {"条件":<20} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9} {"払戻中央値":>10}')
    print(f'  {"-"*60}')
    for c in ['芝', 'ダート']:
        sub = [r for r in records if r['course'] == c]
        if len(sub) < 10: continue
        _show(c, sub, results)

    # ─── 7. 組み合わせベスト ───
    print(f'\n{"="*70}')
    print(f'  [G] 複合条件 (ROIプラスの組み合わせ探索)')
    print(f'{"="*70}')
    print(f'  {"条件":<32} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9}')
    print(f'  {"-"*65}')

    combos_cond = [
        ('頭数≤10 + p4-p5≥3',    lambda r: r['n_horses']<=10 and r['p4_p5_gap']>=3),
        ('頭数≤10 + top4_cum≥55', lambda r: r['n_horses']<=10 and r['top4_cum']>=55),
        ('頭数≤12 + p3-p4≥4',    lambda r: r['n_horses']<=12 and r['p3_p4_gap']>=4),
        ('頭数≤12 + p4-p5≥3',    lambda r: r['n_horses']<=12 and r['p4_p5_gap']>=3),
        ('頭数≤12 + top4_cum≥60', lambda r: r['n_horses']<=12 and r['top4_cum']>=60),
        ('芝 + p4-p5≥3',          lambda r: r['course']=='芝'  and r['p4_p5_gap']>=3),
        ('芝 + 頭数≤12',          lambda r: r['course']=='芝'  and r['n_horses']<=12),
        ('芝 + top4_cum≥55',      lambda r: r['course']=='芝'  and r['top4_cum']>=55),
        ('長距離 + 頭数≤14',       lambda r: r['dist']>=2000   and r['n_horses']<=14),
        ('長距離 + p4-p5≥2',      lambda r: r['dist']>=2000   and r['p4_p5_gap']>=2),
        ('p3-p4≥4 + p4-p5≥3',   lambda r: r['p3_p4_gap']>=4 and r['p4_p5_gap']>=3),
        ('top4_cum≥60 + 頭数≤12', lambda r: r['top4_cum']>=60 and r['n_horses']<=12),
        ('top4_cum≥55 + p4-p5≥2', lambda r: r['top4_cum']>=55 and r['p4_p5_gap']>=2),
    ]
    combo_results = []
    for label, fn in combos_cond:
        sub = [r for r in records if fn(r)]
        if len(sub) < 15: continue
        inv = sum(r['invest'] for r in sub)
        ret_v = sum(r['ret']    for r in sub)
        hits  = sum(r['hit']    for r in sub)
        cov   = sum(1 for r in sub if r['covered'] == 3)
        roi   = (ret_v - inv) / inv * 100 if inv > 0 else -100
        combo_results.append((roi, label, len(sub), cov/len(sub)*100, hits/len(sub)*100, inv, ret_v))

    combo_results.sort(key=lambda x: -x[0])
    for roi, label, n, cov_r, hit_r, inv, ret_v in combo_results[:10]:
        mark = ' ★' if roi > 0 else ''
        print(f'  {label:<32} {n:>5}件  {cov_r:>6.1f}%  {hit_r:>6.1f}%  {roi:>+8.1f}%{mark}')

    # ─── 総合ベスト ───
    print(f'\n{"="*70}')
    print(f'  総合ベスト (ROI順 単独条件)')
    print(f'{"="*70}')
    results.sort(key=lambda x: -x['roi'])
    print(f'  {"条件":<22} {"件数":>5} {"カバー率":>8} {"的中率":>7} {"ROI":>9}')
    print(f'  {"-"*55}')
    for r in results[:10]:
        mark = ' ★' if r['roi'] > 0 else ''
        print(f'  {r["label"]:<22} {r["n"]:>5}件  {r["cov_r"]:>6.1f}%  {r["hit_r"]:>6.1f}%  {r["roi"]:>+8.1f}%{mark}')


def _show(label, sub, results):
    inv   = sum(r['invest'] for r in sub)
    ret_v = sum(r['ret']    for r in sub)
    hits  = sum(r['hit']    for r in sub)
    cov   = sum(1 for r in sub if r['covered'] == 3)
    roi   = (ret_v - inv) / inv * 100 if inv > 0 else -100
    cov_r = cov / len(sub) * 100
    hit_r = hits / len(sub) * 100
    pay_vals = sorted([r['ret'] for r in sub if r['ret'] > 0])
    median_pay = pay_vals[len(pay_vals)//2] if pay_vals else 0
    mark = ' ★' if roi > 0 else ''
    print(f'  {label:<20} {len(sub):>5}件  {cov_r:>6.1f}%  {hit_r:>6.1f}%  {roi:>+8.1f}%  {median_pay:>8,}円{mark}')
    results.append({'label': label, 'n': len(sub), 'cov_r': cov_r, 'hit_r': hit_r, 'roi': roi})


# ─── メイン ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',       default='data')
    parser.add_argument('--test-start', default='202401', dest='test_start')
    parser.add_argument('--test-end',   default='',       dest='test_end')
    parser.add_argument('--rnum', nargs='+', type=int, default=[8, 9, 10, 11])
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 70)
    print('  三連複top4 BOX 有効条件探索')
    print('=' * 70)
    print(f'  テスト期間: {args.test_start}〜{args.test_end or "最新"}')
    print(f'  対象R    : {sorted(target_rnums)}')
    print(f'  対象     : 突出レース (max_prob≥20%) のみ')

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

    records = run(all_races, horse_db, yms, target_rnums, jstats, dc_db, w, b, mean, std)
    print(f'  突出レース: {len(records)}件')

    grid_search(records)


if __name__ == '__main__':
    main()

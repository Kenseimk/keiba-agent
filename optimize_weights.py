"""
optimize_weights.py  スコア重みのグリッドサーチ最適化
======================================================
各因子の生スコアを事前計算（キャッシュ）し、重みの組み合わせ探索を高速化。

  Phase1: 全レースの全馬の因子スコアを1回だけ計算してキャッシュ
  Phase2: 重みを変えてキャッシュから再集計（486通り × 1111レース）

使い方:
  python optimize_weights.py              # 全2025年データで最適化
  python optimize_weights.py --year 2025  # 年指定
  python optimize_weights.py --quick      # 粗いグリッドで高速探索
"""

import os, sys, glob, csv, re, argparse, itertools, time
from collections import defaultdict

from score_agent_core import (
    load_models, odds_fallback_score,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    parse_avg_pos, parse_bw_change, _float, _int,
)
from backtest_agent import load_all_races, build_horse_db_upto

# ─── グリッド定義 ─────────────────────────────────────
GRID_FINE = {
    'recent_rank':   [2.0, 2.5, 3.0, 3.5, 4.0],
    'agari_rank':    [1.0, 1.5, 2.0, 2.5, 3.0],
    'jockey':        [1.0, 1.5, 2.0, 2.5],
    'course_fit':    [0.5, 1.0, 1.5, 2.0],
    'running_style': [0.5, 1.0, 1.5, 2.0],
    'weight_change': [0.0, 0.3, 0.5],
}

GRID_QUICK = {
    'recent_rank':   [2.0, 3.0, 4.0],
    'agari_rank':    [1.0, 2.0, 3.0],
    'jockey':        [1.0, 1.5, 2.0],
    'course_fit':    [0.5, 1.0, 1.5],
    'running_style': [0.5, 1.0, 1.5],
    'weight_change': [0.0, 0.5],
}

FACTOR_KEYS = ['recent_rank', 'agari_rank', 'jockey', 'course_fit', 'running_style', 'weight_change']


# ─── Phase1: 全レースの因子スコアを事前計算 ────────────
def precompute_factor_scores(target_ids, all_races, jstats, dc_db):
    """
    各レースの各馬の因子スコア (6次元) を事前計算。
    course/distはバックテスト時不明のためダート/1800m固定。

    戻り値:
      race_cache = {
        race_id: {
          'winner': str,   # 実際の1着馬名
          'actual_ranks': {horse_name: rank},
          'horses': [
            {'name': str, 'factors': [f1..f6]},
            ...
          ]
        }
      }
    """
    print(f"Phase1: 因子スコア事前計算 ({len(target_ids)} レース)...")
    course = 'ダート'
    dist   = 1800

    race_cache = {}
    for i, race_id in enumerate(target_ids):
        if i % 100 == 0:
            print(f"  {i}/{len(target_ids)}  ({i/len(target_ids)*100:.0f}%)", end='\r', flush=True)

        info       = all_races[race_id]
        horses_raw = info['horses']

        actual_ranks = {}
        for h in horses_raw:
            r = _int(h.get('着順'))
            if r is not None:
                actual_ranks[h['馬名']] = r
        if not actual_ranks or len(actual_ranks) < 3:
            continue

        horse_db = build_horse_db_upto(all_races, race_id)

        horse_factors = []
        for h in horses_raw:
            name   = h['馬名']
            jockey = h.get('騎手', '')
            pop    = _int(h.get('人気'), 99)
            odds   = _float(h.get('単勝オッズ'), 0.0)
            rank   = _int(h.get('着順'))
            if rank is None:
                continue

            history = horse_db.get(name, [])
            if not history:
                fb = odds_fallback_score(pop, odds)
                f_rr = fb
                f_ar = fb * 0.9
                f_rs = 5.0
                f_wc = 5.0
            else:
                f_rr = score_recent_rank(history)
                f_ar = score_agari_rank(history)
                f_rs = score_running_style(history, course, dist)
                f_wc = score_weight_change(history)
            f_js = score_jockey(jockey, jstats)
            f_cf = score_course_fit(name, course, dist, dc_db)

            horse_factors.append({
                'name':    name,
                'factors': [f_rr, f_ar, f_js, f_cf, f_rs, f_wc],
            })

        if len(horse_factors) < 3:
            continue

        winner = min(actual_ranks, key=actual_ranks.get)
        race_cache[race_id] = {
            'winner':       winner,
            'actual_ranks': actual_ranks,
            'horses':       horse_factors,
        }

    print(f"\n  完了: {len(race_cache)} レース分キャッシュ")
    return race_cache


# ─── Phase2: 重みを変えて的中率を集計 ────────────────
def evaluate_weights_fast(weights_vec, race_cache):
    """
    事前計算済みキャッシュに重みを適用して的中率を計算。
    weights_vec: [w_rr, w_ar, w_js, w_cf, w_rs, w_wc]
    """
    total = hit1 = hit3 = sanpuku = 0

    for info in race_cache.values():
        winner       = info['winner']
        actual_ranks = info['actual_ranks']
        horses       = info['horses']

        scored = []
        for h in horses:
            s = sum(f * w for f, w in zip(h['factors'], weights_vec))
            scored.append((h['name'], s))

        scored.sort(key=lambda x: x[1], reverse=True)
        if len(scored) < 3:
            continue

        pred1, pred2, pred3 = scored[0][0], scored[1][0], scored[2][0]
        rp1 = actual_ranks.get(pred1, 99)
        rp2 = actual_ranks.get(pred2, 99)
        rp3 = actual_ranks.get(pred3, 99)

        total += 1
        if pred1 == winner:
            hit1 += 1
        if winner in (pred1, pred2, pred3):
            hit3 += 1
        if rp1 <= 3 and rp2 <= 3 and rp3 <= 3:
            sanpuku += 1

    if total == 0:
        return (0, 0, 0, 0)
    return (total, hit1/total, hit3/total, sanpuku/total)


# ─── メイン ────────────────────────────────────────────
def run_optimize(data_dir='data', year_filter=None, quick=False):
    print('=' * 72)
    print('  スコアエージェント 重み最適化')
    print('=' * 72)

    print('\n[1/4] モデル・データ読み込み...')
    jstats, dc_db = load_models(data_dir)
    all_races     = load_all_races(data_dir)

    target_ids = sorted(all_races.keys())
    if year_filter:
        target_ids = [r for r in target_ids if r.startswith(year_filter)]
    target_ids = [r for r in target_ids if 8 <= int(r[10:12]) <= 11]
    print(f'  検証対象: {len(target_ids)} レース')

    # Phase1: 事前計算
    print('\n[2/4] 因子スコア事前計算...')
    t0 = time.time()
    race_cache = precompute_factor_scores(target_ids, all_races, jstats, dc_db)
    print(f'  所要時間: {time.time()-t0:.1f}秒')

    # Phase2: グリッドサーチ
    grid = GRID_QUICK if quick else GRID_FINE
    combos = list(itertools.product(*[grid[k] for k in FACTOR_KEYS]))
    print(f'\n[3/4] グリッドサーチ: {len(combos):,} 通り')

    best = {'sanpuku': (-1, None), 'hit3': (-1, None), 'hit1': (-1, None)}

    t0 = time.time()
    for i, vals in enumerate(combos):
        if i % max(1, len(combos)//20) == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i+1) * (len(combos)-i-1) if i > 0 else 0
            print(f'  {i/len(combos)*100:5.1f}%  ETA:{eta:.0f}秒', end='\r', flush=True)

        total, r1, r3, rs = evaluate_weights_fast(list(vals), race_cache)

        if rs > best['sanpuku'][0]:
            best['sanpuku'] = (rs, vals)
        if r3 > best['hit3'][0]:
            best['hit3'] = (r3, vals)
        if r1 > best['hit1'][0]:
            best['hit1'] = (r1, vals)

    print(f'\n  所要時間: {time.time()-t0:.1f}秒')

    # 現在の重みで評価
    current_vals = [3.0, 2.0, 1.5, 1.5, 1.5, 0.5]
    _, cr1, cr3, crs = evaluate_weights_fast(current_vals, race_cache)

    print(f'\n[4/4] 結果')
    print(f'\n  現在の重み: {dict(zip(FACTOR_KEYS, current_vals))}')
    print(f'    1着: {cr1*100:.1f}%  3頭内: {cr3*100:.1f}%  三連複: {crs*100:.1f}%')

    for label, key in [('三連複最大化', 'sanpuku'), ('3頭内的中率最大化', 'hit3'), ('1着的中率最大化', 'hit1')]:
        score, vals = best[key]
        w = dict(zip(FACTOR_KEYS, vals))
        _, r1, r3, rs = evaluate_weights_fast(list(vals), race_cache)
        print(f'\n  [{label}]')
        print(f'    1着: {r1*100:.1f}%  3頭内: {r3*100:.1f}%  三連複: {rs*100:.1f}%')
        print(f'    重み: recent={w["recent_rank"]}  agari={w["agari_rank"]}  '
              f'jockey={w["jockey"]}  course={w["course_fit"]}  '
              f'style={w["running_style"]}  bw={w["weight_change"]}')

    print('\n' + '=' * 72)
    return best


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',  default='data')
    parser.add_argument('--year',  help='例: 2025')
    parser.add_argument('--quick', action='store_true')
    args = parser.parse_args()
    run_optimize(data_dir=args.data, year_filter=args.year, quick=args.quick)

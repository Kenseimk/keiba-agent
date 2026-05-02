"""
optimize_weights_v2.py  WIN_V2 モデルの重みをグリッドサーチで最適化

Phase1: 全レースの 12 因子スコアをキャッシュ（horse_db 累積）
Phase2: numpy ベクトル化で重み組み合わせを高速探索

最適化: 2015-2022
検証  : 2023-2024

使い方:
    python optimize_weights_v2.py            # 最適化＋検証
    python optimize_weights_v2.py --rebuild  # キャッシュ再構築
    python optimize_weights_v2.py --top 30
    python optimize_weights_v2.py --min-bets 200
"""

import sys, os, csv, glob, re, time, pickle, argparse, itertools
from collections import defaultdict
import numpy as np

from score_agent_core import (
    load_models,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    score_dist_fit, score_track_fit, score_last_margin,
    score_rest_interval, score_venue_fit, score_win_rate,
    parse_avg_pos, parse_bw_change, parse_margin, _float, _int,
)
from backtest_agent import load_all_races, parse_tansho

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}
CACHE_FILE = 'optimize_weights_v2_cache.pkl'

FACTOR_KEYS = [
    'recent_rank', 'agari_rank', 'jockey', 'course_fit',
    'running_style', 'weight_change',
    'dist_fit', 'track_fit', 'last_margin',
    'rest_interval', 'venue_fit', 'win_rate',
]
N_FACTORS = len(FACTOR_KEYS)

# ─── Phase1: キャッシュ構築 ───────────────────────────
def build_cache(all_races, jstats, dc_db, year_start, year_end):
    all_ids_sorted = sorted(r for r in all_races.keys() if r[:4].isdigit())
    target_ids = {r for r in all_ids_sorted
                  if year_start <= int(r[:4]) <= year_end
                  and r[4:6] in JRA_VENUES}

    horse_db = defaultdict(list)
    cache = []
    done = 0
    t0 = time.time()
    total = len(target_ids)

    print(f'  全 {len(all_ids_sorted):,} レース / 対象 {total:,} レース')

    for race_id in all_ids_sorted:
        info = all_races[race_id]
        horses_raw = info['horses']

        if race_id in target_ids:
            actual_ranks = {}
            for h in horses_raw:
                r = _int(h.get('着順'))
                if r is not None:
                    actual_ranks[h['馬名']] = r

            if actual_ranks:
                course      = info.get('course') or 'ダート'
                dist        = info.get('dist') or 1800
                track_cond  = info.get('track_cond', '')
                venue_code  = race_id[4:6]
                race_ym     = race_id[:6]
                tansho      = parse_tansho(info.get('tansho_raw', ''))

                horses_data = []
                for h in horses_raw:
                    name    = h['馬名']
                    jockey  = h.get('騎手', '')
                    history = horse_db.get(name, [])
                    umaban  = h.get('馬番', '').strip()
                    rank    = actual_ranks.get(name)

                    factors = [
                        score_recent_rank(history),
                        score_agari_rank(history),
                        score_jockey(jockey, jstats),
                        score_course_fit(name, course, dist, dc_db),
                        score_running_style(history, course, dist),
                        score_weight_change(history),
                        score_dist_fit(history, dist),
                        score_track_fit(history, track_cond),
                        score_last_margin(history),
                        score_rest_interval(history, race_ym),
                        score_venue_fit(history, venue_code),
                        score_win_rate(history),
                    ]

                    payout = tansho.get(umaban, 0) if rank == 1 else 0
                    horses_data.append((factors, payout))

                if horses_data:
                    cache.append({
                        'year':    int(race_id[:4]),
                        'factors': [hd[0] for hd in horses_data],
                        'payouts': [hd[1] for hd in horses_data],
                    })

            done += 1
            if done % 500 == 0:
                elapsed = time.time() - t0
                print(f'  {done}/{total} ({done/total*100:.0f}%) {elapsed:.0f}秒',
                      end='\r', flush=True)

        # horse_db を常に更新
        agari_list = []
        for h in horses_raw:
            a = _float(h.get('上がり3F'))
            if a is not None:
                agari_list.append((h['馬名'], a))
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i+1 for i,(name,_) in enumerate(agari_list)}

        for h in horses_raw:
            name = h['馬名']
            rank = _int(h.get('着順'))
            if rank is None: continue
            margin_raw = h.get('着差', '').strip()
            record = {
                'race_id':     race_id,
                'race_ym':     race_id[:6],
                'venue_code':  race_id[4:6],
                'race_num':    _int(race_id[10:12], 0),
                'rank':        rank,
                'field_size':  len(horses_raw),
                'jockey':      h.get('騎手', ''),
                'odds':        _float(h.get('単勝オッズ')),
                'pop':         _int(h.get('人気')),
                'agari':       _float(h.get('上がり3F')),
                'agari_rank':  agari_rank_map.get(name, -1),
                'agari_field': len(agari_list),
                'avg_pos':     parse_avg_pos(h.get('通過順', '')),
                'bw_chg':      parse_bw_change(h.get('馬体重', '')),
                'dist':        _int(h.get('距離')),
                'track_cond':  h.get('馬場状態', '').strip(),
                'margin':      parse_margin(margin_raw, rank),
            }
            horse_db[name].append(record)
            if len(horse_db[name]) > 10:
                horse_db[name] = horse_db[name][-10:]

    print(f'\n  キャッシュ完了: {len(cache):,} レース  {time.time()-t0:.0f}秒')
    return cache


# ─── numpy テンソルに変換 ──────────────────────────────
def build_tensors(cache, year_start, year_end):
    """
    対象年のレースを numpy テンソルに変換。
    factors_tensor: (N_races, max_horses, N_factors)  float32
    payouts_arr:    (N_races, max_horses)              int32
    mask_arr:       (N_races, max_horses)              bool  (有効馬=True)
    """
    filtered = [rd for rd in cache if year_start <= rd['year'] <= year_end]
    if not filtered:
        return None, None, None, 0

    max_h = max(len(rd['factors']) for rd in filtered)
    N = len(filtered)

    factors_tensor = np.zeros((N, max_h, N_FACTORS), dtype=np.float32)
    payouts_arr    = np.zeros((N, max_h), dtype=np.int32)
    mask_arr       = np.zeros((N, max_h), dtype=bool)

    for i, rd in enumerate(filtered):
        n = len(rd['factors'])
        factors_tensor[i, :n, :] = rd['factors']
        payouts_arr[i, :n]       = rd['payouts']
        mask_arr[i, :n]          = True

    print(f'  テンソル変換完了: {N:,}レース / 最大{max_h}頭'
          f'  ({factors_tensor.nbytes / 1e6:.1f} MB)')
    return factors_tensor, payouts_arr, mask_arr, N


# ─── Phase2: 重み評価（numpy 高速版） ────────────────
def evaluate_weights_numpy(factors_tensor, payouts_arr, mask_arr, weights_vec):
    """
    numpy ベクトル化で全レースを一括スコアリング。
    """
    w = np.array(weights_vec, dtype=np.float32)

    # scores: (N, max_h) = matmul of (N, max_h, 12) with (12,)
    scores = np.einsum('rhi,i->rh', factors_tensor, w)

    # マスク外（パディング）は -inf に
    scores[~mask_arr] = -np.inf

    # 各レースで最高スコア馬のインデックス
    best_idx = np.argmax(scores, axis=1)  # (N,)

    race_idx = np.arange(len(best_idx))
    payouts  = payouts_arr[race_idx, best_idx]

    total  = len(best_idx)
    invest = total * 100
    ret    = int(payouts.sum())
    hit    = int((payouts > 0).sum())
    return total, hit, invest, ret


# ─── グリッドサーチ ───────────────────────────────────
def grid_search(factors_tensor, payouts_arr, mask_arr, N,
                min_bets=300):
    """
    6 つの基本重みは WIN プリセット固定。
    6 つの新因子重みをグリッドサーチ。
    """
    # 基本重み固定 (recent_rank, agari_rank, jockey, course_fit, running_style, weight_change)
    base = [2.0, 1.0, 2.0, 1.5, 1.0, 0.0]

    # 新因子: dist_fit, track_fit, last_margin, rest_interval, venue_fit, win_rate
    candidates = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5]

    combos = list(itertools.product(candidates, repeat=6))
    print(f'  探索条件数: {len(combos):,} (対象レース {N:,})')

    results = []
    t0 = time.time()

    for i, new_factors in enumerate(combos):
        if i % 2000 == 0:
            print(f'  {i:,}/{len(combos):,} ({i/len(combos)*100:.0f}%) '
                  f'{time.time()-t0:.1f}秒', end='\r', flush=True)

        weights_vec = base + list(new_factors)
        total, hit, invest, ret = evaluate_weights_numpy(
            factors_tensor, payouts_arr, mask_arr, weights_vec)

        if total < min_bets: continue

        roi = ret / invest * 100
        wr  = hit / total * 100
        results.append({
            'total': total, 'hit': hit, 'invest': invest, 'ret': ret,
            'roi': roi, 'wr': wr,
            'dist_fit':      new_factors[0],
            'track_fit':     new_factors[1],
            'last_margin':   new_factors[2],
            'rest_interval': new_factors[3],
            'venue_fit':     new_factors[4],
            'win_rate':      new_factors[5],
        })

    print(f'\n  完了: {len(results):,}件 / {time.time()-t0:.1f}秒')
    results.sort(key=lambda x: -x['roi'])
    return results


# ─── 表示 ────────────────────────────────────────────
def print_results(results, top_n, label, year_start, year_end):
    print()
    print('=' * 90)
    print(f'  {label} ({year_start}〜{year_end}年)  上位{top_n}件')
    print('=' * 90)
    print(f'{"ROI":>7} {"的中率":>7} {"件数":>6} {"損益":>9} '
          f'{"dist":>5} {"track":>5} {"margin":>6} {"rest":>5} {"venue":>5} {"winr":>5}')
    print('-' * 90)
    for r in results[:top_n]:
        print(f'{r["roi"]:>6.1f}%  {r["wr"]:>5.1f}%  {r["total"]:>6,}'
              f'  {r["ret"]-r["invest"]:>+9,}'
              f'  {r["dist_fit"]:>5.1f}  {r["track_fit"]:>5.1f}'
              f'  {r["last_margin"]:>6.1f}  {r["rest_interval"]:>5.1f}'
              f'  {r["venue_fit"]:>5.1f}  {r["win_rate"]:>5.1f}')
    print()


# ─── メイン ──────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebuild',  action='store_true', help='キャッシュ再構築')
    ap.add_argument('--top',      type=int, default=20)
    ap.add_argument('--min-bets', type=int, default=300)
    ap.add_argument('--data',     default='data')
    args = ap.parse_args()

    opt_start, opt_end = 2015, 2022
    val_start, val_end = 2023, 2024

    # キャッシュ読み込み or 構築
    if not args.rebuild and os.path.exists(CACHE_FILE):
        print(f'キャッシュ読み込み: {CACHE_FILE}')
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        print(f'  {len(cache):,} レース')
    else:
        print('Phase1: データ読み込み＆スコア計算...')
        jstats, dc_db = load_models(args.data)
        all_races = load_all_races(args.data)
        print(f'  全 {len(all_races):,} レース')
        cache = build_cache(all_races, jstats, dc_db, opt_start, val_end)
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        print(f'  キャッシュ保存: {CACHE_FILE}')

    # numpy テンソル変換（最適化期間）
    print(f'\nPhase2準備: numpy テンソル変換 ({opt_start}〜{opt_end}年)...')
    ft_opt, pay_opt, mask_opt, N_opt = build_tensors(cache, opt_start, opt_end)

    # Phase2: グリッドサーチ
    print(f'\nPhase2: 重みグリッドサーチ ({opt_start}〜{opt_end}年)...')
    results = grid_search(ft_opt, pay_opt, mask_opt, N_opt, args.min_bets)

    print_results(results, args.top, '最適化期間', opt_start, opt_end)

    if not results:
        print('有効な結果がありませんでした。')
        return

    # 検証期間テンソル
    print(f'検証期間テンソル変換 ({val_start}〜{val_end}年)...')
    ft_val, pay_val, mask_val, N_val = build_tensors(cache, val_start, val_end)

    # 上位10件を検証期間で評価
    print(f'\n検証期間 ({val_start}〜{val_end}年) で上位10件を評価:')
    print()
    print(f'{"最適化ROI":>9} {"検証ROI":>9} {"検証件数":>7} '
          f'{"dist":>5} {"track":>5} {"margin":>6} {"rest":>5} {"venue":>5} {"winr":>5}')
    print('-' * 80)

    for r in results[:10]:
        weights_vec = [2.0, 1.0, 2.0, 1.5, 1.0, 0.0,
                       r['dist_fit'], r['track_fit'], r['last_margin'],
                       r['rest_interval'], r['venue_fit'], r['win_rate']]
        vt, vh, vi, vr = evaluate_weights_numpy(ft_val, pay_val, mask_val, weights_vec)
        v_roi = vr / vi * 100 if vi else 0
        print(f'{r["roi"]:>8.1f}%  {v_roi:>8.1f}%  {vt:>7,}'
              f'  {r["dist_fit"]:>5.1f}  {r["track_fit"]:>5.1f}'
              f'  {r["last_margin"]:>6.1f}  {r["rest_interval"]:>5.1f}'
              f'  {r["venue_fit"]:>5.1f}  {r["win_rate"]:>5.1f}')

    print()

    # 最良の重みセット表示
    best = results[0]
    print('最良重みセット（win_v2 プリセット候補）:')
    print(f'  recent_rank:   2.0')
    print(f'  agari_rank:    1.0')
    print(f'  jockey:        2.0')
    print(f'  course_fit:    1.5')
    print(f'  running_style: 1.0')
    print(f'  weight_change: 0.0')
    print(f'  dist_fit:      {best["dist_fit"]}')
    print(f'  track_fit:     {best["track_fit"]}')
    print(f'  last_margin:   {best["last_margin"]}')
    print(f'  rest_interval: {best["rest_interval"]}')
    print(f'  venue_fit:     {best["venue_fit"]}')
    print(f'  win_rate:      {best["win_rate"]}')
    print()


if __name__ == '__main__':
    main()

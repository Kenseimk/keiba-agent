"""
optimize_filters.py  絞り込み条件のグリッドサーチ
======================================================
両モデル一致レースを対象に、以下の絞り込み条件を探索する:
  - 出走頭数 (min_field)
  - 1位-2位スコアギャップ (min_gap)
  - 予測1位馬のオッズ (odds_min 〜 odds_max)

Phase1: 全レースのスコアを1回だけ計算してキャッシュ
Phase2: フィルター条件を変えて三連複回収率を集計 (高速)

使い方:
  python optimize_filters.py
  python optimize_filters.py --top 20   # 上位20件を表示
"""

import os, sys, re, glob, csv, argparse, itertools, time, pickle
from collections import defaultdict

CACHE_FILE = 'optimize_cache.pkl'

from score_agent_core import (
    load_models, odds_fallback_score,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    WEIGHT_PRESETS, parse_avg_pos, parse_bw_change, _float, _int,
)
from backtest_agent import (
    load_all_races, build_horse_db_upto,
    parse_tansho, parse_sanrenpuku,
)

FACTOR_KEYS = ['recent_rank', 'agari_rank', 'jockey', 'course_fit', 'running_style', 'weight_change']


# ─── Phase1: 全レースのスコアをキャッシュ ──────────────
def build_race_cache(all_races, jstats, dc_db):
    """
    全 R8-R11 レースについて両モデルのスコアを計算し、
    フィルター評価に必要な情報をキャッシュする。

    戻り値のリスト要素:
      {
        'field_size':     int,
        'gap_win':        float,   # winモデル 1位-2位スコア差
        'gap_san':        float,   # sanpukuモデル 1位-2位スコア差
        'pred1_odds':     float,   # sanpukuモデルの予測1位馬オッズ
        'models_agree':   bool,    # 上位3頭が一致
        'is_sanpuku':     bool,    # 実際に三連複的中
        'is_hit1':        bool,    # 実際に1着的中
        'tan_pay':        int,     # 単勝払戻 (的中時)
        'san_pay':        int,     # 三連複払戻 (的中時)
        'ym':             str,     # YYYYMM
      }
    """
    vec_win = [WEIGHT_PRESETS['win'][k]     for k in FACTOR_KEYS]
    vec_san = [WEIGHT_PRESETS['sanpuku'][k] for k in FACTOR_KEYS]

    def _rnum(r):
        try:   return int(r[10:12])
        except: return -1

    target_ids = sorted([r for r in all_races if 8 <= _rnum(r) <= 11])
    total = len(target_ids)
    cache = []

    print(f'  Phase1: {total} レースのスコア計算中...')
    t0 = time.time()

    for i, race_id in enumerate(target_ids):
        if i % 100 == 0:
            print(f'    {i}/{total} ({i/total*100:.0f}%)', end='\r', flush=True)

        info       = all_races[race_id]
        horses_raw = info['horses']

        actual_ranks = {}
        for h in horses_raw:
            r = _int(h.get('着順'))
            if r is not None:
                actual_ranks[h['馬名']] = r
        if len(actual_ranks) < 3:
            continue

        horse_db = build_horse_db_upto(all_races, race_id)
        name_to_umaban = info.get('name_to_umaban', {})

        # 両モデルでスコア計算
        scored_w = []; scored_s = []
        for h in horses_raw:
            name    = h['馬名']
            jockey  = h.get('騎手', '')
            pop     = _int(h.get('人気'), 99)
            odds_h  = _float(h.get('単勝オッズ'), 0.0)
            history = horse_db.get(name, [])

            # コース・距離: CSVに列があれば使用、なければフォールバック
            r_course = info.get('course') or 'ダート'
            r_dist   = info.get('dist')   or 1800

            if not history:
                fb   = odds_fallback_score(pop, odds_h)
                f_rr = fb; f_ar = fb * 0.9; f_rs = 5.0; f_wc = 5.0
            else:
                f_rr = score_recent_rank(history)
                f_ar = score_agari_rank(history)
                f_rs = score_running_style(history, r_course, r_dist)
                f_wc = score_weight_change(history)
            f_js = score_jockey(jockey, jstats)
            f_cf = score_course_fit(name, r_course, r_dist, dc_db)
            factors = [f_rr, f_ar, f_js, f_cf, f_rs, f_wc]

            sw = sum(f * w for f, w in zip(factors, vec_win))
            ss = sum(f * w for f, w in zip(factors, vec_san))
            scored_w.append((name, sw, odds_h))
            scored_s.append((name, ss, odds_h))

        if len(scored_w) < 3:
            continue

        scored_w.sort(key=lambda x: x[1], reverse=True)
        scored_s.sort(key=lambda x: x[1], reverse=True)

        top3_win = {scored_w[j][0] for j in range(3)}
        top3_san = {scored_s[j][0] for j in range(3)}
        models_agree = (top3_win == top3_san)

        # sanpuku ベースで評価
        p1, _, p1_odds = scored_s[0]
        p2, _, _       = scored_s[1]
        p3, _, _       = scored_s[2]

        rp1 = actual_ranks.get(p1, 99)
        rp2 = actual_ranks.get(p2, 99)
        rp3 = actual_ranks.get(p3, 99)
        winner  = min(actual_ranks, key=actual_ranks.get)
        is_hit1    = (p1 == winner)
        is_sanpuku = (rp1 <= 3 and rp2 <= 3 and rp3 <= 3)

        gap_win = scored_w[0][1] - scored_w[1][1]
        gap_san = scored_s[0][1] - scored_s[1][1]

        # 払戻取得
        tan_pay = 0
        if is_hit1:
            tansho = parse_tansho(info.get('tansho_raw', ''))
            tan_pay = tansho.get(name_to_umaban.get(p1, ''), 0)

        san_pay = 0
        if is_sanpuku:
            sp = parse_sanrenpuku(info.get('sanrenpuku_raw', ''))
            i1 = _int(name_to_umaban.get(p1, ''), 0)
            i2 = _int(name_to_umaban.get(p2, ''), 0)
            i3 = _int(name_to_umaban.get(p3, ''), 0)
            if i1 and i2 and i3:
                san_pay = sp.get(tuple(sorted([i1, i2, i3])), 0)

        cache.append({
            'field_size':   len(horses_raw),
            'gap_win':      gap_win,
            'gap_san':      gap_san,
            'pred1_odds':   float(p1_odds or 0),
            'models_agree': models_agree,
            'is_sanpuku':   is_sanpuku,
            'is_hit1':      is_hit1,
            'tan_pay':      tan_pay,
            'san_pay':      san_pay,
            'ym':           info.get('ym', race_id[:6]),
            'track_cond':   info.get('track_cond') or '',
            'dist':         info.get('dist') or 0,
        })

    print(f'\n  完了: {len(cache)} レース  {time.time()-t0:.0f}秒')
    return cache


# ─── Phase2: フィルター適用・評価 ─────────────────────
def evaluate_filter(cache, min_field, min_gap, odds_min, odds_max,
                    track_cond=None, dist_min=0):
    """フィルター条件を適用して回収率等を返す"""
    total = hit_san = tan_inv = tan_ret = san_inv = san_ret = 0

    for r in cache:
        if not r['models_agree']:
            continue
        if r['field_size'] < min_field:
            continue
        if r['gap_san'] < min_gap:
            continue
        if not (odds_min <= r['pred1_odds'] <= odds_max):
            continue
        if track_cond and r['track_cond'] not in track_cond:
            continue
        if dist_min and r['dist'] < dist_min:
            continue

        total   += 1
        tan_inv += 100
        san_inv += 100
        hit_san += r['is_sanpuku']
        tan_ret += r['tan_pay']
        san_ret += r['san_pay']

    if total == 0:
        return None
    return {
        'total':    total,
        'san_rate': hit_san / total * 100,
        'tan_roi':  tan_ret / tan_inv * 100,
        'san_roi':  san_ret / san_inv * 100,
        'san_ret':  san_ret,
        'san_inv':  san_inv,
    }


# ─── メイン ───────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',    default='data')
    parser.add_argument('--top',     type=int, default=30, help='上位N件を表示')
    parser.add_argument('--min-races', type=int, default=100,
                        help='最低レース数 (少なすぎる条件を除外)')
    parser.add_argument('--rebuild',   action='store_true',
                        help='キャッシュを無視して再構築')
    parser.add_argument('--train-end', default='202412',
                        help='訓練データの終了年月 YYYYMM (デフォルト: 202412)')
    args = parser.parse_args()

    print('=' * 72)
    print('  絞り込み条件 グリッドサーチ')
    print('=' * 72)

    if not args.rebuild and os.path.exists(CACHE_FILE):
        print(f'\n[1/3] キャッシュ読み込み中... ({CACHE_FILE})')
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        print(f'  キャッシュ: {len(cache)} レース')
    else:
        print('\n[1/3] データ・モデル読み込み...')
        jstats, dc_db = load_models(args.data)
        all_races     = load_all_races(args.data)
        print(f'  合計 {len(all_races)} レース')

        print('\n[2/3] スコアキャッシュ構築...')
        cache = build_race_cache(all_races, jstats, dc_db)
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        print(f'  キャッシュ保存: {CACHE_FILE}')

    # 基準値比較
    base         = evaluate_filter(cache, min_field=0, min_gap=0, odds_min=0, odds_max=9999)
    base_ryo     = evaluate_filter(cache, min_field=0, min_gap=0, odds_min=0, odds_max=9999,
                                   track_cond=['良'])
    base_d16     = evaluate_filter(cache, min_field=0, min_gap=0, odds_min=0, odds_max=9999,
                                   dist_min=1600)
    base_ryo_d16 = evaluate_filter(cache, min_field=0, min_gap=0, odds_min=0, odds_max=9999,
                                   track_cond=['良'], dist_min=1600)
    print(f'\n  {"条件":<22} {"件数":>6}  {"三連複率":>7}  {"三連複ROI":>10}')
    print(f'  {"-"*52}')
    for label, res in [
        ('フィルターなし',         base),
        ('良馬場のみ',             base_ryo),
        ('距離1600m以上',          base_d16),
        ('良馬場 ＋ 距離1600m以上', base_ryo_d16),
    ]:
        if res:
            print(f'  {label:<22} {res["total"]:>6}  {res["san_rate"]:>6.1f}%  {res["san_roi"]:>9.1f}%')

    # ── 訓練/検証 分割 ──
    TRAIN_END = args.train_end   # 例: '202412'
    train_cache = [r for r in cache if r['ym'] <= TRAIN_END]
    test_cache  = [r for r in cache if r['ym'] >  TRAIN_END]
    print(f'\n  訓練データ: 〜{TRAIN_END}  {len(train_cache)}レース')
    print(f'  検証データ: {TRAIN_END}〜   {len(test_cache)}レース')

    print('\n[3/3] グリッドサーチ（訓練データ）...')

    # グリッド定義
    grid = {
        'min_field': [8, 10, 12, 14],
        'min_gap':   [0, 2, 3, 5, 7, 10],
        'odds_min':  [1.0, 2.0, 3.0, 4.0],
        'odds_max':  [10.0, 15.0, 20.0, 50.0, 999.0],
    }

    combos  = list(itertools.product(
        grid['min_field'], grid['min_gap'],
        grid['odds_min'],  grid['odds_max']
    ))
    results = []

    t0 = time.time()
    for i, (mf, mg, omin, omax) in enumerate(combos):
        if omin >= omax:
            continue
        res = evaluate_filter(train_cache, mf, mg, omin, omax,
                              track_cond=['良'], dist_min=1600)
        if res is None or res['total'] < args.min_races:
            continue
        # 検証データでも評価
        res_test = evaluate_filter(test_cache, mf, mg, omin, omax,
                                   track_cond=['良'], dist_min=1600)
        res.update({'min_field': mf, 'min_gap': mg,
                    'odds_min': omin, 'odds_max': omax,
                    'test': res_test})
        results.append(res)

    results.sort(key=lambda x: x['san_roi'], reverse=True)
    print(f'  完了: {len(results)}条件  {time.time()-t0:.1f}秒')

    # ── 結果表示 ──
    print(f'\n{"="*80}')
    print(f'  三連複ROI 上位 {args.top} 件  訓練:〜{TRAIN_END} / 検証:{TRAIN_END}〜')
    print(f'{"="*80}')
    print(f'  {"頭数≥":>5} {"ギャップ≥":>9} {"オッズ帯":>12}  '
          f'{"訓練件数":>8}  {"訓練ROI":>9}  {"検証件数":>8}  {"検証ROI":>9}')
    print(f'  {"-"*72}')

    for r in results[:args.top]:
        odds_str = f'{r["odds_min"]:.0f}〜{r["odds_max"]:.0f}倍'
        t = r['test']
        test_str = f'{t["total"]:>8}  {t["san_roi"]:>8.1f}%' if t else f'{"--":>8}  {"--":>8}'
        print(f'  {r["min_field"]:>5}  {r["min_gap"]:>9.1f}  {odds_str:>12}  '
              f'{r["total"]:>8}  {r["san_roi"]:>8.1f}%  {test_str}')

    if results:
        # 検証ROIでも上位を表示
        results_by_test = [r for r in results if r['test'] and r['test']['total'] >= 30]
        results_by_test.sort(key=lambda x: x['test']['san_roi'], reverse=True)
        print(f'\n{"="*80}')
        print(f'  検証ROI 上位 {args.top} 件（検証データ30件以上）')
        print(f'{"="*80}')
        print(f'  {"頭数≥":>5} {"ギャップ≥":>9} {"オッズ帯":>12}  '
              f'{"訓練件数":>8}  {"訓練ROI":>9}  {"検証件数":>8}  {"検証ROI":>9}')
        print(f'  {"-"*72}')
        for r in results_by_test[:args.top]:
            odds_str = f'{r["odds_min"]:.0f}〜{r["odds_max"]:.0f}倍'
            t = r['test']
            print(f'  {r["min_field"]:>5}  {r["min_gap"]:>9.1f}  {odds_str:>12}  '
                  f'{r["total"]:>8}  {r["san_roi"]:>8.1f}%  '
                  f'{t["total"]:>8}  {t["san_roi"]:>8.1f}%')

        best = results[0]
        print(f'\n  ★ 訓練データ最良条件:')
        print(f'    頭数 ≥ {best["min_field"]}  ギャップ ≥ {best["min_gap"]}  '
              f'オッズ {best["odds_min"]:.0f}〜{best["odds_max"]:.0f}倍')
        print(f'    訓練: {best["total"]}レース  ROI:{best["san_roi"]:.1f}%')
        t = best['test']
        if t:
            print(f'    検証: {t["total"]}レース  ROI:{t["san_roi"]:.1f}%')

        # 月別内訳（最良条件）
        print(f'\n  ★ 最良条件の月別内訳（全期間）:')
        monthly = defaultdict(lambda: {'total':0,'hit':0,'san_inv':0,'san_ret':0})
        mf = best['min_field']; mg = best['min_gap']
        omin = best['odds_min']; omax = best['odds_max']
        for r2 in cache:
            if not r2['models_agree']: continue
            if r2['field_size'] < mf: continue
            if r2['gap_san'] < mg: continue
            if not (omin <= r2['pred1_odds'] <= omax): continue
            if r2['track_cond'] != '良': continue
            if r2['dist'] < 1600: continue
            ym = r2['ym']
            monthly[ym]['total']   += 1
            monthly[ym]['hit']     += r2['is_sanpuku']
            monthly[ym]['san_inv'] += 100
            monthly[ym]['san_ret'] += r2['san_pay']

        print(f'  {"年月":>6}  {"件数":>5}  {"三連複%":>7}  {"三連複ROI":>10}')
        print(f'  {"-"*36}')
        for ym in sorted(monthly.keys()):
            m = monthly[ym]
            if m['total'] == 0: continue
            roi = m['san_ret'] / m['san_inv'] * 100 if m['san_inv'] else 0
            print(f'  {ym:>6}  {m["total"]:>5}  '
                  f'{m["hit"]/m["total"]*100:>6.1f}%  {roi:>9.1f}%')

    print('\n' + '=' * 72)


if __name__ == '__main__':
    main()

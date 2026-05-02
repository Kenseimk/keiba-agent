"""
validate_tansho_v2.py  score_agent_core の WIN モデルで単勝を検証

backtest_agent.py の build_horse_db_upto は O(N²) で遅いため、
月単位で horse_db を累積して高速化。

使い方:
    python validate_tansho_v2.py               # 2015-2024 全期間
    python validate_tansho_v2.py 2020 2024     # 年範囲指定
    python validate_tansho_v2.py 2015 2022 --val 2023 2024  # 最適化+検証
"""

import sys, io, os, glob, csv, re, argparse, time
from collections import defaultdict

from score_agent_core import (
    load_models, should_exclude,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    score_dist_fit, score_track_fit, score_last_margin,
    score_rest_interval, score_venue_fit, score_win_rate,
    WEIGHT_PRESETS, parse_avg_pos, parse_bw_change, parse_margin, _float, _int,
)
from backtest_agent import load_all_races, parse_tansho

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

# ─── 月単位で horse_db を累積構築 ────────────────────
def build_horse_db_monthly(all_races: dict) -> dict:
    """
    race_id の昇順（= 日付昇順）で horse_db を累積。
    戻り値: {race_id: horse_db_snapshot}  ではなく
            process_race() のたびに呼べる generator 方式。
    実装: {race_id → horse_db_at_that_point} をまとめて返すと
    メモリが爆発するため、イテレータとして提供。
    """
    pass  # 下の run_validation で直接組み込む

# ─── メイン検証 ────────────────────────────────────
def run_validation(all_races, jstats, dc_db, weights,
                   year_start, year_end, label=''):
    target_ids = sorted(all_races.keys())
    target_ids = [r for r in target_ids
                  if r[:4].isdigit()
                  and year_start <= int(r[:4]) <= year_end
                  and r[4:6] in JRA_VENUES]

    # horse_db を累積していく辞書
    horse_db = defaultdict(list)

    total = hit = invest = ret = 0
    pop_stats  = defaultdict(lambda: {'total':0,'hit':0,'invest':0,'ret':0})
    odds_stats = defaultdict(lambda: {'total':0,'hit':0,'invest':0,'ret':0})
    monthly    = defaultdict(lambda: {'total':0,'hit':0,'invest':0,'ret':0})

    all_ids_sorted = sorted(r for r in all_races.keys() if r[:4].isdigit())

    print(f'[{label}] 検証開始 ({year_start}〜{year_end}) '
          f'対象: {len(target_ids)} レース')
    t0 = time.time()
    done = 0

    for race_id in all_ids_sorted:
        info = all_races[race_id]
        horses_raw = info['horses']

        # --- ① 対象期間のレースのみスコア計算・集計 ---
        if year_start <= int(race_id[:4]) <= year_end and race_id[4:6] in JRA_VENUES:
            race_name = info.get('race_name', '')

            # 実際の結果
            actual_ranks = {}
            for h in horses_raw:
                r = _int(h.get('着順'))
                if r is not None:
                    actual_ranks[h['馬名']] = r
            if not actual_ranks:
                pass
            else:
                course = info.get('course') or 'ダート'
                dist   = info.get('dist')   or 1800
                n_field = len(horses_raw)

                # 各馬スコア計算
                track_cond = info.get('track_cond', '')
                venue_code = race_id[4:6]
                race_ym    = race_id[:6]
                scored = []
                for h in horses_raw:
                    name    = h['馬名']
                    jockey  = h.get('騎手', '')
                    history = horse_db.get(name, [])
                    sc = (
                        score_recent_rank(history)    * weights['recent_rank']   +
                        score_agari_rank(history)     * weights['agari_rank']    +
                        score_jockey(jockey, jstats)  * weights['jockey']        +
                        score_course_fit(name, course, dist, dc_db) * weights['course_fit'] +
                        score_running_style(history, course, dist) * weights['running_style'] +
                        score_weight_change(history)  * weights['weight_change'] +
                        score_dist_fit(history, dist)             * weights.get('dist_fit',      0.0) +
                        score_track_fit(history, track_cond)      * weights.get('track_fit',     0.0) +
                        score_last_margin(history)                * weights.get('last_margin',   0.0) +
                        score_rest_interval(history, race_ym)     * weights.get('rest_interval', 0.0) +
                        score_venue_fit(history, venue_code)      * weights.get('venue_fit',     0.0) +
                        score_win_rate(history)                   * weights.get('win_rate',      0.0)
                    )
                    scored.append((sc, h))
                scored.sort(key=lambda x: -x[0])

                best_sc, best_h = scored[0]
                name    = best_h['馬名']
                odds    = _float(best_h.get('単勝オッズ'), 0)
                pop     = _int(best_h.get('人気'), 99)
                umaban  = best_h.get('馬番', '').strip()
                won     = (actual_ranks.get(name) == 1)

                tansho  = parse_tansho(info.get('tansho_raw', ''))
                payout  = tansho.get(umaban, 0) if won else 0

                ym = race_id[:6]
                ym_key = f'{ym[:4]}-{ym[4:]}'

                total   += 1
                invest  += 100
                ret     += payout
                if won: hit += 1

                # 人気別
                pop_stats[pop]['total']  += 1
                pop_stats[pop]['invest'] += 100
                if won:
                    pop_stats[pop]['hit'] += 1
                    pop_stats[pop]['ret'] += payout

                # オッズ帯別
                if odds < 3:     ob = '〜2.9倍'
                elif odds < 5:   ob = '3〜4.9倍'
                elif odds < 10:  ob = '5〜9.9倍'
                elif odds < 20:  ob = '10〜19倍'
                else:            ob = '20倍〜'
                odds_stats[ob]['total']  += 1
                odds_stats[ob]['invest'] += 100
                if won:
                    odds_stats[ob]['hit'] += 1
                    odds_stats[ob]['ret'] += payout

                # 月別
                monthly[ym_key]['total']  += 1
                monthly[ym_key]['invest'] += 100
                if won:
                    monthly[ym_key]['hit'] += 1
                    monthly[ym_key]['ret'] += payout

            done += 1
            if done % 1000 == 0:
                elapsed = time.time() - t0
                print(f'  {done}/{len(target_ids)} ({done/len(target_ids)*100:.0f}%) '
                      f'{elapsed:.0f}秒', end='\r', flush=True)

        # --- ② 常に horse_db を更新（全期間） ---
        # 上がり3F順位
        agari_list = []
        for h in horses_raw:
            a = _float(h.get('上がり3F'))
            if a is not None:
                agari_list.append((h['馬名'], a))
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i+1 for i,(name,_) in enumerate(agari_list)}

        for h in horses_raw:
            name   = h['馬名']
            rank   = _int(h.get('着順'))
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
            # 最新5件に絞る（メモリ節約）
            if len(horse_db[name]) > 10:
                horse_db[name] = horse_db[name][-10:]

    print(f'\n  完了: {time.time()-t0:.0f}秒')
    return total, hit, invest, ret, monthly, pop_stats, odds_stats

# ─── 出力 ──────────────────────────────────────────
def print_results(total, hit, invest, ret, monthly, pop_stats, odds_stats,
                  year_start, year_end, label='WINモデル'):
    roi = ret / invest * 100 if invest else 0
    wr  = hit / total * 100 if total else 0

    print()
    print('=' * 62)
    print(f'  単勝検証結果 [{label}] ({year_start}〜{year_end}年)')
    print('=' * 62)
    print(f'  対象レース数  : {total:,}')
    print(f'  的中数        : {hit:,}')
    print(f'  的中率        : {wr:.1f}%')
    print(f'  投資額合計    : {invest:,}円')
    print(f'  回収額合計    : {ret:,}円')
    print(f'  ROI           : {roi:.1f}%')
    print(f'  損益           : {ret-invest:+,}円')

    print()
    print('── 月別 ─────────────────────────────────────')
    print(f'{"年月":<9} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 45)
    prev_year = None
    for ym in sorted(monthly.keys()):
        m = monthly[ym]
        if not m['total']: continue
        year = ym[:4]
        if prev_year and year != prev_year: print()
        wr_m  = m['hit']  / m['total']  * 100
        roi_m = m['ret']  / m['invest'] * 100 if m['invest'] else 0
        print(f'{ym:<9} {m["total"]:>6,} {m["hit"]:>5,} {wr_m:>6.1f}%  {roi_m:>7.1f}%')
        prev_year = year

    print()
    print('── 人気別 ───────────────────────────────────')
    print(f'{"人気":<6} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 38)
    for pop in sorted(pop_stats.keys()):
        s = pop_stats[pop]
        if not s['total']: continue
        wr_  = s['hit'] / s['total'] * 100
        roi_ = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        print(f'{pop:<6} {s["total"]:>6,} {s["hit"]:>5,} {wr_:>6.1f}%  {roi_:>7.1f}%')

    print()
    print('── オッズ帯別 ───────────────────────────────')
    print(f'{"オッズ帯":<12} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 44)
    for ob in ['〜2.9倍','3〜4.9倍','5〜9.9倍','10〜19倍','20倍〜']:
        if ob not in odds_stats: continue
        s = odds_stats[ob]
        wr_  = s['hit'] / s['total'] * 100
        roi_ = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        print(f'{ob:<12} {s["total"]:>6,} {s["hit"]:>5,} {wr_:>6.1f}%  {roi_:>7.1f}%')
    print()

# ─── エントリーポイント ────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('year_start', nargs='?', type=int, default=2015)
    ap.add_argument('year_end',   nargs='?', type=int, default=2024)
    ap.add_argument('--val',      nargs=2,   type=int, metavar=('VS','VE'))
    ap.add_argument('--model',    default='win', choices=['win','win_v2','sanpuku','default'])
    ap.add_argument('--data',     default='data')
    args = ap.parse_args()

    weights = WEIGHT_PRESETS[args.model]
    print(f'モデル: {args.model}  重み: {weights}')

    print('\n[1/2] モデル・データ読み込み...')
    jstats, dc_db = load_models(args.data)
    all_races = load_all_races(args.data)
    print(f'  全 {len(all_races):,} レース')

    print('\n[2/2] 単勝検証実行...')
    result = run_validation(all_races, jstats, dc_db, weights,
                            args.year_start, args.year_end,
                            label=args.model.upper())
    print_results(*result, args.year_start, args.year_end, label=args.model.upper())

    if args.val:
        vs, ve = args.val
        print(f'\n検証期間 ({vs}〜{ve}年) での評価...')
        result_v = run_validation(all_races, jstats, dc_db, weights,
                                  vs, ve, label=f'{args.model.upper()} 検証')
        print_results(*result_v, vs, ve, label=f'{args.model.upper()} 検証')

if __name__ == '__main__':
    main()

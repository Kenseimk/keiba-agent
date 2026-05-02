# -*- coding: utf-8 -*-
"""
simulate_yearly.py  1年ごとバックテスト (2015〜2024)
horse_dbを年ごとに積み上げてローリング評価
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, should_exclude_uscore

RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
MARKET_ALPHA    = 0.4

NAGASHI_WP_MIN    = 20.0
NAGASHI_ODDS_MAX  = 4.0
NAGASHI_WPSUM_MIN = 45.0
NAGASHI_N_AITE    = 4

KEN_WP_MIN      = 20.0
KEN_ODDS_MAX    = 4.0
KEN_SANKAKU_MIN = 10.0

BET = 100  # 固定100円/枚で年間純粋パフォーマンスを比較


def load_sanrentan_year(data_dir, year):
    db = {}
    ym_start = f'{year}01'
    ym_end   = f'{year}12'
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if ym < ym_start or ym > ym_end:
            continue
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if rid not in db:
                    db[rid] = {}
                san = row.get('三連単払戻', '').strip()
                if san and san != '-':
                    mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', san)
                    if mm:
                        key = (mm.group(1), mm.group(2), mm.group(3))
                        db[rid][key] = int(mm.group(4))
    return db


print('全データ読み込み中...')
races = load_all_csv_races('data')
print(f'全レース: {len(races):,}R')

# 年ごとに処理
YEARS = list(range(2015, 2025))

print()
print(f'{"年":>6}  {"horse_db":>9}  '
      f'{"N-R数":>5} {"N-的中":>5} {"N-ROI":>7} {"N-収支":>9}  '
      f'{"K-R数":>5} {"K-的中":>5} {"K-ROI":>7} {"K-収支":>9}  '
      f'{"合計投資":>9} {"合計収支":>9} {"合ROI":>7}')
print('-' * 120)

horse_db = defaultdict(list)

for year in YEARS:
    ym_start = f'{year}01'
    ym_end   = f'{year}12'

    # horse_db をこの年より前のデータで構築（前年末までを追加）
    _add_races_to_horse_db(horse_db, races, exact_ym=None,
                           upto_ym=None)
    # 毎年の開始時点で前年末まで追加（差分追加方式）
    # 実際には upto_ym で絞り込んで再構築する方が安全
    horse_db_year = defaultdict(list)
    _add_races_to_horse_db(horse_db_year, races, upto_ym=ym_start)
    for n in horse_db_year:
        horse_db_year[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)

    trainer_stats = build_trainer_stats(horse_db_year)
    jockey_stats  = build_jockey_stats(horse_db_year)

    sanrentan_db = load_sanrentan_year('data', year)
    test_rids = sorted(rid for rid, info in races.items()
                       if ym_start <= info['file_ym'] <= ym_end)

    n_races = n_hits = n_cost = n_ret = 0
    k_races = k_hits = k_cost = k_ret = 0

    for race_id in test_rids:
        info = races[race_id]
        if should_exclude_uscore(info['race_name']): continue
        if all(h['odds'] == 0.0 for h in info['horse_list']): continue
        rnum = int(race_id[-2:])
        if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
        if info['n_field'] > MAX_FIELD: continue

        race_obj = make_race_info(info)
        try:
            sc = analyze_race_uscore(race_obj, horse_db_year, None, None,
                                     trainer_stats=trainer_stats,
                                     jockey_stats=jockey_stats,
                                     market_alpha=MARKET_ALPHA)
        except:
            continue
        if not sc: continue

        wp_map   = {h['name']: h['win_prob'] for h in sc}
        odds_map = {h['name']: h['odds']     for h in info['horse_list']}
        uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
        san = sanrentan_db.get(race_id, {})

        # NAGASHI
        if len(sorted_wp) >= 2:
            hn = sorted_wp[0]['name']
            rn = sorted_wp[1]['name']
            if (wp_map.get(hn, 0) >= NAGASHI_WP_MIN and
                    odds_map.get(hn, 99) <= NAGASHI_ODDS_MAX and
                    wp_map.get(hn, 0) + wp_map.get(rn, 0) >= NAGASHI_WPSUM_MIN):
                aite = [h['name'] for h in sorted_wp[2:2+NAGASHI_N_AITE]]
                u_h = uma_map.get(hn, ''); u_r = uma_map.get(rn, '')
                u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
                if u_h and u_r and u_a:
                    tix = set()
                    for perm in permutations([u_h, u_r]):
                        for a in u_a:
                            tix.add((*perm, a))
                    cost = len(tix) * BET
                    ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
                    n_races += 1
                    n_cost  += cost
                    n_ret   += ret
                    if ret > 0: n_hits += 1

        # NAGASHI-堅
        if len(sorted_wp) >= 3:
            hn2 = sorted_wp[0]['name']
            rn2 = sorted_wp[1]['name']
            an2 = sorted_wp[2]['name']
            if (wp_map.get(hn2, 0) >= KEN_WP_MIN and
                    odds_map.get(hn2, 99) <= KEN_ODDS_MAX and
                    odds_map.get(an2, 0) >= KEN_SANKAKU_MIN):
                u_h2 = uma_map.get(hn2, '')
                u_r2 = uma_map.get(rn2, '')
                u_a2 = uma_map.get(an2, '')
                if u_h2 and u_r2 and u_a2:
                    tix2 = {(u_h2, u_r2, u_a2), (u_h2, u_a2, u_r2)}
                    cost2 = len(tix2) * BET
                    ret2  = sum(san.get(t, 0) * BET // 100 for t in tix2)
                    k_races += 1
                    k_cost  += cost2
                    k_ret   += ret2
                    if ret2 > 0: k_hits += 1

    n_roi = n_ret / n_cost * 100 if n_cost else 0
    k_roi = k_ret / k_cost * 100 if k_cost else 0
    total_cost = n_cost + k_cost
    total_ret  = n_ret  + k_ret
    total_roi  = total_ret / total_cost * 100 if total_cost else 0
    total_pnl  = total_ret - total_cost
    hdb_size   = sum(len(v) for v in horse_db_year.values())

    n_hr = f'{n_hits/n_races*100:.0f}%' if n_races else '-'
    k_hr = f'{k_hits/k_races*100:.0f}%' if k_races else '-'

    mark = '+' if total_pnl >= 0 else '-'
    print(f'{year:>6}年  {hdb_size:>9,}  '
          f'{n_races:>5} {n_hr:>5} {n_roi:>6.0f}% {n_ret-n_cost:>+9,}  '
          f'{k_races:>5} {k_hr:>5} {k_roi:>6.0f}% {k_ret-k_cost:>+9,}  '
          f'{total_cost:>9,} {total_pnl:>+9,} {total_roi:>6.0f}%', flush=True)

print('-' * 120)
print('※ 固定100円/枚 / 8-11R / 頭数<=14 / NAGASHI:◎+○wp>=45% / 堅:▲odds>=10')

# -*- coding: utf-8 -*-
"""
simulate_yearly2.py  戦略別・期間別バックテスト
  - NAGASHI / NAGASHI-堅 を別々に評価
  - 学習期間 2015-2022 / テスト期間 2023-2024 で分けて集計
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

BET = 100

TRAIN_YEARS = list(range(2015, 2023))  # 2015-2022
TEST_YEARS  = [2023, 2024]


def load_sanrentan_year(data_dir, year):
    db = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if not (f'{year}01' <= ym <= f'{year}12'): continue
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if rid not in db: db[rid] = {}
                san = row.get('三連単払戻', '').strip()
                if san and san != '-':
                    mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', san)
                    if mm:
                        key = (mm.group(1), mm.group(2), mm.group(3))
                        db[rid][key] = int(mm.group(4))
    return db


print('全データ読み込み中...', flush=True)
races = load_all_csv_races('data')

# 年ごとの結果を収集
results = {}  # year -> {nagashi: {...}, ken: {...}}

for year in TRAIN_YEARS + TEST_YEARS:
    ym_start = f'{year}01'
    ym_end   = f'{year}12'
    print(f'{year}年 処理中...', flush=True)

    horse_db_year = defaultdict(list)
    _add_races_to_horse_db(horse_db_year, races, upto_ym=ym_start)
    for n in horse_db_year:
        horse_db_year[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)

    trainer_stats = build_trainer_stats(horse_db_year)
    jockey_stats  = build_jockey_stats(horse_db_year)
    sanrentan_db  = load_sanrentan_year('data', year)
    test_rids     = sorted(rid for rid, info in races.items()
                           if ym_start <= info['file_ym'] <= ym_end)

    n = {'races': 0, 'hits': 0, 'cost': 0, 'ret': 0}
    k = {'races': 0, 'hits': 0, 'cost': 0, 'ret': 0}

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
        except: continue
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
                u_a = [uma_map.get(n2, '') for n2 in aite if uma_map.get(n2, '')]
                if u_h and u_r and u_a:
                    tix = set()
                    for perm in permutations([u_h, u_r]):
                        for a in u_a: tix.add((*perm, a))
                    cost = len(tix) * BET
                    ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
                    n['races'] += 1; n['cost'] += cost; n['ret'] += ret
                    if ret > 0: n['hits'] += 1

        # NAGASHI-堅
        if len(sorted_wp) >= 3:
            hn2 = sorted_wp[0]['name']
            rn2 = sorted_wp[1]['name']
            an2 = sorted_wp[2]['name']
            if (wp_map.get(hn2, 0) >= KEN_WP_MIN and
                    odds_map.get(hn2, 99) <= KEN_ODDS_MAX and
                    odds_map.get(an2, 0) >= KEN_SANKAKU_MIN):
                u_h2 = uma_map.get(hn2, ''); u_r2 = uma_map.get(rn2, ''); u_a2 = uma_map.get(an2, '')
                if u_h2 and u_r2 and u_a2:
                    tix2 = {(u_h2, u_r2, u_a2), (u_h2, u_a2, u_r2)}
                    cost2 = len(tix2) * BET
                    ret2  = sum(san.get(t, 0) * BET // 100 for t in tix2)
                    k['races'] += 1; k['cost'] += cost2; k['ret'] += ret2
                    if ret2 > 0: k['hits'] += 1

    results[year] = {'nagashi': n, 'ken': k}


def fmt(s):
    r = s['races']; h = s['hits']; c = s['cost']; ret = s['ret']
    roi = ret / c * 100 if c else 0
    pnl = ret - c
    hr  = f'{h/r*100:.0f}%' if r else '-'
    mark = '✓' if pnl >= 0 else '✗'
    return r, hr, roi, pnl, mark


def print_strategy(name, key, years, label):
    print(f'\n{"="*60}')
    print(f'【{name}】{label}')
    print(f'{"="*60}')
    print(f'{"年":>6}  {"R数":>4}  {"的中率":>6}  {"ROI":>7}  {"収支":>10}')
    print('-' * 42)
    tot_r = tot_c = tot_ret = tot_h = 0
    for year in years:
        s = results[year][key]
        r, hr, roi, pnl, mark = fmt(s)
        tot_r   += s['races']; tot_h += s['hits']
        tot_c   += s['cost'];  tot_ret += s['ret']
        print(f'{year:>6}年  {r:>4}  {hr:>6}  {roi:>6.0f}%  {pnl:>+10,}円  {mark}')
    print('-' * 42)
    roi_t = tot_ret / tot_c * 100 if tot_c else 0
    hr_t  = f'{tot_h/tot_r*100:.0f}%' if tot_r else '-'
    mark_t = '✓' if tot_ret >= tot_c else '✗'
    print(f'{"合計":>6}   {tot_r:>4}  {hr_t:>6}  {roi_t:>6.0f}%  {tot_ret-tot_c:>+10,}円  {mark_t}')


print('\n\n' + '='*60)
print('■ NAGASHI（◎+○wp≥45%）')
print('='*60)

# 学習期間
print_strategy('NAGASHI', 'nagashi', TRAIN_YEARS, '学習期間 2015-2022')
print_strategy('NAGASHI', 'nagashi', TEST_YEARS,  'テスト期間 2023-2024')

print('\n\n' + '='*60)
print('■ NAGASHI-堅（▲odds≥10）')
print('='*60)

print_strategy('NAGASHI-堅', 'ken', TRAIN_YEARS, '学習期間 2015-2022')
print_strategy('NAGASHI-堅', 'ken', TEST_YEARS,  'テスト期間 2023-2024')

# 総合サマリー
print('\n\n' + '='*60)
print('■ 総合サマリー（NAGASHI + NAGASHI-堅 合算）')
print('='*60)
print(f'\n{"期間":<16}  {"NAGASHI ROI":>12}  {"堅 ROI":>10}  {"合算 ROI":>10}  {"合算収支":>10}')
print('-' * 66)

for label, years in [('学習 2015-2022', TRAIN_YEARS), ('テスト 2023-2024', TEST_YEARS), ('全期間 2015-2024', TRAIN_YEARS+TEST_YEARS)]:
    nc = nr = kc = kr = 0
    for y in years:
        nc += results[y]['nagashi']['cost']; nr += results[y]['nagashi']['ret']
        kc += results[y]['ken']['cost'];     kr += results[y]['ken']['ret']
    n_roi = nr/nc*100 if nc else 0
    k_roi = kr/kc*100 if kc else 0
    t_roi = (nr+kr)/(nc+kc)*100 if (nc+kc) else 0
    t_pnl = (nr+kr)-(nc+kc)
    mark  = '✓' if t_pnl >= 0 else '✗'
    print(f'{label:<16}  {n_roi:>11.1f}%  {k_roi:>9.1f}%  {t_roi:>9.1f}%  {t_pnl:>+10,}円  {mark}')

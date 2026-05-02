# -*- coding: utf-8 -*-
"""
grid_nagashi.py  NAGASHIパラメータ グリッドサーチ
学習期間(2015-2022)でROI>=100%かつ十分なR数の条件を探し
テスト期間(2023-2024)で検証する
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, should_exclude_uscore

RNUM_MIN     = 8
RNUM_MAX     = 11
MAX_FIELD    = 14
MARKET_ALPHA = 0.4
N_AITE       = 4
BET          = 100

TRAIN_START = '201501'
TRAIN_END   = '202212'
TEST_START  = '202301'
TEST_END    = '202412'

print('全データ読み込み中...', flush=True)
races = load_all_csv_races('data')

# 三連単払戻ロード
def load_sanrentan_range(data_dir, ym_start, ym_end):
    db = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if ym < ym_start or ym > ym_end: continue
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

san_train = load_sanrentan_range('data', TRAIN_START, TRAIN_END)
san_test  = load_sanrentan_range('data', TEST_START,  TEST_END)

# horse_db を学習期間開始時点で構築（2015以前）
horse_db_train = defaultdict(list)
_add_races_to_horse_db(horse_db_train, races, upto_ym=TRAIN_START)
for n in horse_db_train:
    horse_db_train[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats_train = build_trainer_stats(horse_db_train)
jockey_stats_train  = build_jockey_stats(horse_db_train)

# horse_db をテスト期間開始時点で構築（2015-2022）
horse_db_test = defaultdict(list)
_add_races_to_horse_db(horse_db_test, races, upto_ym=TEST_START)
for n in horse_db_test:
    horse_db_test[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats_test = build_trainer_stats(horse_db_test)
jockey_stats_test  = build_jockey_stats(horse_db_test)

# レースデータを事前計算（horse_db は固定で使用）
print('レースデータ事前計算中（学習期間）...', flush=True)
precomp_train = []
for race_id in sorted(rid for rid, info in races.items()
                      if TRAIN_START <= info['file_ym'] <= TRAIN_END):
    info = races[race_id]
    if should_exclude_uscore(info['race_name']): continue
    if all(h['odds'] == 0.0 for h in info['horse_list']): continue
    rnum = int(race_id[-2:])
    if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
    if info['n_field'] > MAX_FIELD: continue
    race_obj = make_race_info(info)
    try:
        sc = analyze_race_uscore(race_obj, horse_db_train, None, None,
                                 trainer_stats=trainer_stats_train,
                                 jockey_stats=jockey_stats_train,
                                 market_alpha=MARKET_ALPHA)
    except: continue
    if not sc: continue
    wp_map   = {h['name']: h['win_prob'] for h in sc}
    odds_map = {h['name']: h['odds']     for h in info['horse_list']}
    uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
    sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
    san = san_train.get(race_id, {})
    precomp_train.append({'wp': wp_map, 'odds': odds_map, 'uma': uma_map,
                          'sorted_wp': sorted_wp, 'san': san})

print(f'学習期間: {len(precomp_train)}R')

print('レースデータ事前計算中（テスト期間）...', flush=True)
precomp_test = []
for race_id in sorted(rid for rid, info in races.items()
                      if TEST_START <= info['file_ym'] <= TEST_END):
    info = races[race_id]
    if should_exclude_uscore(info['race_name']): continue
    if all(h['odds'] == 0.0 for h in info['horse_list']): continue
    rnum = int(race_id[-2:])
    if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
    if info['n_field'] > MAX_FIELD: continue
    race_obj = make_race_info(info)
    try:
        sc = analyze_race_uscore(race_obj, horse_db_test, None, None,
                                 trainer_stats=trainer_stats_test,
                                 jockey_stats=jockey_stats_test,
                                 market_alpha=MARKET_ALPHA)
    except: continue
    if not sc: continue
    wp_map   = {h['name']: h['win_prob'] for h in sc}
    odds_map = {h['name']: h['odds']     for h in info['horse_list']}
    uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
    sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
    san = san_test.get(race_id, {})
    precomp_test.append({'wp': wp_map, 'odds': odds_map, 'uma': uma_map,
                         'sorted_wp': sorted_wp, 'san': san})

print(f'テスト期間: {len(precomp_test)}R\n')


def eval_params(data, wp_min, odds_max, wpsum_min):
    races_ok = hits = cost = ret = 0
    for d in data:
        sw = d['sorted_wp']
        if len(sw) < 2: continue
        hn = sw[0]['name']
        rn = sw[1]['name']
        if d['wp'].get(hn, 0) < wp_min: continue
        if d['odds'].get(hn, 99) > odds_max: continue
        if d['wp'].get(hn, 0) + d['wp'].get(rn, 0) < wpsum_min: continue
        aite = [h['name'] for h in sw[2:2+N_AITE]]
        u_h = d['uma'].get(hn, ''); u_r = d['uma'].get(rn, '')
        u_a = [d['uma'].get(n, '') for n in aite if d['uma'].get(n, '')]
        if not u_h or not u_r or not u_a: continue
        tix = set()
        for perm in permutations([u_h, u_r]):
            for a in u_a: tix.add((*perm, a))
        c = len(tix) * BET
        r = sum(d['san'].get(t, 0) * BET // 100 for t in tix)
        races_ok += 1; cost += c; ret += r
        if r > 0: hits += 1
    roi = ret / cost * 100 if cost else 0
    return races_ok, hits, roi, ret - cost


# グリッドサーチ
print('グリッドサーチ中...', flush=True)

wp_mins    = [15, 18, 20, 22, 25]
odds_maxes = [3.0, 3.5, 4.0, 4.5, 5.0]
wpsum_mins = [35, 40, 45, 50, 55]

candidates = []
for wp_min in wp_mins:
    for odds_max in odds_maxes:
        for wpsum_min in wpsum_mins:
            tr, th, t_roi, t_pnl = eval_params(precomp_train, wp_min, odds_max, wpsum_min)
            if tr < 50: continue  # 最低50R必要
            if t_roi < 100: continue  # 学習期間ROI>=100%
            te, teh, e_roi, e_pnl = eval_params(precomp_test, wp_min, odds_max, wpsum_min)
            candidates.append({
                'wp_min': wp_min, 'odds_max': odds_max, 'wpsum_min': wpsum_min,
                'tr': tr, 'th': th, 't_roi': t_roi, 't_pnl': t_pnl,
                'te': te, 'teh': teh, 'e_roi': e_roi, 'e_pnl': e_pnl,
            })

candidates.sort(key=lambda x: x['e_roi'], reverse=True)

print(f'学習期間ROI>=100% かつ R数>=50 の条件: {len(candidates)}件\n')
print(f'{"◎wp≥":>6} {"◎odds≤":>7} {"◎+○wp≥":>8}  '
      f'{"[学習] R数":>8} {"的中率":>6} {"ROI":>7} {"収支":>9}  '
      f'{"[テスト] R数":>9} {"的中率":>6} {"ROI":>7} {"収支":>9}')
print('-' * 100)

for c in candidates[:20]:
    t_hr = f'{c["th"]/c["tr"]*100:.0f}%' if c["tr"] else '-'
    e_hr = f'{c["teh"]/c["te"]*100:.0f}%' if c["te"] else '-'
    e_mark = '✓' if c['e_pnl'] >= 0 else '✗'
    print(f'{c["wp_min"]:>5}%  {c["odds_max"]:>6.1f}倍  {c["wpsum_min"]:>7}%  '
          f'{c["tr"]:>8}R  {t_hr:>6}  {c["t_roi"]:>6.0f}%  {c["t_pnl"]:>+9,}円  '
          f'{c["te"]:>9}R  {e_hr:>6}  {c["e_roi"]:>6.0f}%  {c["e_pnl"]:>+9,}円  {e_mark}')

if not candidates:
    print('条件を満たす組み合わせなし → 閾値を緩和して再探索')
    print('\n【参考】学習期間ROI上位10件（R数>=30）:')
    all_res = []
    for wp_min in wp_mins:
        for odds_max in odds_maxes:
            for wpsum_min in wpsum_mins:
                tr, th, t_roi, t_pnl = eval_params(precomp_train, wp_min, odds_max, wpsum_min)
                if tr < 30: continue
                te, teh, e_roi, e_pnl = eval_params(precomp_test, wp_min, odds_max, wpsum_min)
                all_res.append((wp_min, odds_max, wpsum_min, tr, t_roi, t_pnl, te, e_roi, e_pnl))
    all_res.sort(key=lambda x: x[4], reverse=True)
    print(f'{"◎wp≥":>6} {"◎odds≤":>7} {"◎+○wp≥":>8}  '
          f'{"[学習]R数":>7} {"ROI":>7} {"収支":>9}  '
          f'{"[テスト]R数":>8} {"ROI":>7} {"収支":>9}')
    print('-' * 85)
    for wp_min, odds_max, wpsum_min, tr, t_roi, t_pnl, te, e_roi, e_pnl in all_res[:15]:
        print(f'{wp_min:>5}%  {odds_max:>6.1f}倍  {wpsum_min:>7}%  '
              f'{tr:>7}R  {t_roi:>6.0f}%  {t_pnl:>+9,}円  '
              f'{te:>8}R  {e_roi:>6.0f}%  {e_pnl:>+9,}円')

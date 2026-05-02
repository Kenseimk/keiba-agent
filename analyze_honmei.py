# -*- coding: utf-8 -*-
"""
analyze_honmei.py  ◎/○ 精度分析
win_prob #1 (◎) / #2 (○) の的中率を年別・条件別に測定し
改善の手がかりを探す
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, should_exclude_uscore

RNUM_MIN     = 8
RNUM_MAX     = 11
MAX_FIELD    = 14
MARKET_ALPHA = 0.4

print('全データ読み込み中...', flush=True)
races = load_all_csv_races('data')

YEARS = list(range(2015, 2025))

# ─── 年別精度計測 ──────────────────────────────
print('\n=== ◎/○ 精度分析 (年別) ===\n')
print(f'{"年":>6}  {"R数":>5}  '
      f'{"◎1着":>6} {"◎連対":>6} {"◎3着内":>7}  '
      f'{"○連対":>6} {"○3着内":>7}  '
      f'{"◎○両連対":>9} {"◎○両3着内":>10}  '
      f'{"市場1位一致":>10}')
print('-' * 100)

yearly = []
for year in YEARS:
    ym_start = f'{year}01'
    ym_end   = f'{year}12'

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=ym_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)

    test_rids = sorted(rid for rid, info in races.items()
                       if ym_start <= info['file_ym'] <= ym_end)

    n_races = 0
    h_win = h_top2 = h_top3 = 0   # ◎
    r_top2 = r_top3 = 0            # ○
    both_top2 = both_top3 = 0      # ◎○両方
    mkt_agree = 0                   # 市場1番人気と◎一致

    for race_id in test_rids:
        info = races[race_id]
        if should_exclude_uscore(info['race_name']): continue
        if all(h['odds'] == 0.0 for h in info['horse_list']): continue
        rnum = int(race_id[-2:])
        if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
        if info['n_field'] > MAX_FIELD: continue

        race_obj = make_race_info(info)
        try:
            sc = analyze_race_uscore(race_obj, horse_db, None, None,
                                     trainer_stats=trainer_stats,
                                     jockey_stats=jockey_stats,
                                     market_alpha=MARKET_ALPHA)
        except: continue
        if not sc or len(sc) < 2: continue

        rank_map = {h['name']: h['rank']  for h in info['horse_list']}
        pop_map  = {h['name']: h['pop']   for h in info['horse_list']}
        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)

        hn = sorted_wp[0]['name']
        rn = sorted_wp[1]['name']
        hr = rank_map.get(hn, 99)
        rr = rank_map.get(rn, 99)

        n_races += 1
        if hr == 1:  h_win  += 1
        if hr <= 2:  h_top2 += 1
        if hr <= 3:  h_top3 += 1
        if rr <= 2:  r_top2 += 1
        if rr <= 3:  r_top3 += 1
        if hr <= 2 and rr <= 2: both_top2 += 1
        if hr <= 3 and rr <= 3: both_top3 += 1
        if pop_map.get(hn, 99) == 1: mkt_agree += 1

    if n_races == 0:
        continue

    row = dict(year=year, n=n_races,
               h_win=h_win, h_top2=h_top2, h_top3=h_top3,
               r_top2=r_top2, r_top3=r_top3,
               both_top2=both_top2, both_top3=both_top3,
               mkt_agree=mkt_agree)
    yearly.append(row)

    print(f'{year:>6}年  {n_races:>5}  '
          f'{h_win/n_races*100:>5.1f}% {h_top2/n_races*100:>5.1f}% {h_top3/n_races*100:>6.1f}%  '
          f'{r_top2/n_races*100:>5.1f}% {r_top3/n_races*100:>6.1f}%  '
          f'{both_top2/n_races*100:>8.1f}% {both_top3/n_races*100:>9.1f}%  '
          f'{mkt_agree/n_races*100:>9.1f}%', flush=True)

print('-' * 100)
# 合計
tot = {k: sum(r[k] for r in yearly) for k in ['n','h_win','h_top2','h_top3',
                                                'r_top2','r_top3','both_top2','both_top3','mkt_agree']}
n = tot['n']
print(f'{"合計":>6}   {n:>5}  '
      f'{tot["h_win"]/n*100:>5.1f}% {tot["h_top2"]/n*100:>5.1f}% {tot["h_top3"]/n*100:>6.1f}%  '
      f'{tot["r_top2"]/n*100:>5.1f}% {tot["r_top3"]/n*100:>6.1f}%  '
      f'{tot["both_top2"]/n*100:>8.1f}% {tot["both_top3"]/n*100:>9.1f}%  '
      f'{tot["mkt_agree"]/n*100:>9.1f}%')

print()
print('【注】◎=win_prob#1 / ○=win_prob#2 / 市場1位一致=◎が市場1番人気と同じ馬か')

# ─── NAGASHIフィルター適用後の精度 ──────────────
print('\n\n=== NAGASHIフィルター適用後の精度比較 ===')
print('(◎wp>=20% / ◎odds<=4.0 / ◎+○wp>=45%)\n')

WP_MIN    = 20.0
ODDS_MAX  = 4.0
WPSUM_MIN = 45.0

horse_db_all = defaultdict(list)
_add_races_to_horse_db(horse_db_all, races, upto_ym='201501')
for n in horse_db_all:
    horse_db_all[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats_all = build_trainer_stats(horse_db_all)
jockey_stats_all  = build_jockey_stats(horse_db_all)

# ※精度分析なのでhorse_dbは2015以前で代表（近似）
# 厳密には年別だが傾向把握に使う
# 全期間まとめて再計測
groups = {
    '全レース（フィルタなし）': lambda wp, odds, sw: True,
    'NAGASHIフィルタあり':
        lambda wp, odds, sw: (
            wp.get(sw[0]['name'], 0) >= WP_MIN and
            odds.get(sw[0]['name'], 99) <= ODDS_MAX and
            wp.get(sw[0]['name'], 0) + wp.get(sw[1]['name'], 0) >= WPSUM_MIN
        ),
    '◎wp>=25% 強化版':
        lambda wp, odds, sw: (
            wp.get(sw[0]['name'], 0) >= 25.0 and
            odds.get(sw[0]['name'], 99) <= 4.0 and
            wp.get(sw[0]['name'], 0) + wp.get(sw[1]['name'], 0) >= 45.0
        ),
    '◎+○wp>=50% 強化版':
        lambda wp, odds, sw: (
            wp.get(sw[0]['name'], 0) >= 20.0 and
            odds.get(sw[0]['name'], 99) <= 4.0 and
            wp.get(sw[0]['name'], 0) + wp.get(sw[1]['name'], 0) >= 50.0
        ),
}

print(f'{"条件":<24}  {"R数":>5}  {"◎1着":>6} {"◎連対":>6} {"◎3着内":>7}  {"○連対":>6} {"○3着内":>7}  {"◎○両連対":>9}')
print('-' * 88)

# 学習期間(2015-2022)のみで集計（horse_dbは2015以前で近似）
horse_db_2015 = defaultdict(list)
_add_races_to_horse_db(horse_db_2015, races, upto_ym='201501')
for nm in horse_db_2015:
    horse_db_2015[nm].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
tr2015 = build_trainer_stats(horse_db_2015)
jk2015 = build_jockey_stats(horse_db_2015)

# 2019-2022 で測定（horse_dbがある程度育った後）
test_rids2 = sorted(rid for rid, info in races.items()
                    if '201901' <= info['file_ym'] <= '202212')

cache = []
for race_id in test_rids2:
    info = races[race_id]
    if should_exclude_uscore(info['race_name']): continue
    if all(h['odds'] == 0.0 for h in info['horse_list']): continue
    rnum = int(race_id[-2:])
    if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
    if info['n_field'] > MAX_FIELD: continue
    race_obj = make_race_info(info)
    try:
        sc = analyze_race_uscore(race_obj, horse_db_2015, None, None,
                                 trainer_stats=tr2015, jockey_stats=jk2015,
                                 market_alpha=MARKET_ALPHA)
    except: continue
    if not sc or len(sc) < 2: continue
    wp_map   = {h['name']: h['win_prob'] for h in sc}
    odds_map = {h['name']: h['odds']     for h in info['horse_list']}
    rank_map = {h['name']: h['rank']     for h in info['horse_list']}
    sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
    cache.append({'wp': wp_map, 'odds': odds_map, 'rank': rank_map, 'sw': sorted_wp})

for gname, gfunc in groups.items():
    cnt = hw = ht2 = ht3 = rt2 = rt3 = bt2 = 0
    for d in cache:
        if len(d['sw']) < 2: continue
        if not gfunc(d['wp'], d['odds'], d['sw']): continue
        hn = d['sw'][0]['name']; rn = d['sw'][1]['name']
        hr = d['rank'].get(hn, 99); rr = d['rank'].get(rn, 99)
        cnt += 1
        if hr == 1: hw  += 1
        if hr <= 2: ht2 += 1
        if hr <= 3: ht3 += 1
        if rr <= 2: rt2 += 1
        if rr <= 3: rt3 += 1
        if hr <= 2 and rr <= 2: bt2 += 1
    if cnt == 0: continue
    print(f'{gname:<24}  {cnt:>5}  '
          f'{hw/cnt*100:>5.1f}% {ht2/cnt*100:>5.1f}% {ht3/cnt*100:>6.1f}%  '
          f'{rt2/cnt*100:>5.1f}% {rt3/cnt*100:>6.1f}%  '
          f'{bt2/cnt*100:>8.1f}%')

print()
print('【目標】◎○両連対率を現状より5%以上改善 → NAGASHIの的中率向上へ')

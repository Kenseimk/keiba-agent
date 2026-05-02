# -*- coding: utf-8 -*-
import sys, io, os

from uscore_backtest import load_all_csv_races, make_race_info, _add_races_to_horse_db
from uscore import analyze_race_uscore, should_exclude_uscore
from collections import defaultdict

DATA_DIR = 'data'
races = load_all_csv_races(DATA_DIR)
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202601')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)

test_months = ['202601', '202602', '202603']
month_races = {rid: info for rid, info in races.items() if info['file_ym'] in test_months}

print("test races: %d" % len(month_races))
print()
print(f"{'alpha':>6}  {'temp':>5}  {'bets':>6}  {'hits':>5}  {'wr%':>6}  {'ROI%':>7}")
print("-" * 50)

for alpha in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
    for temp in [1.0, 1.5, 2.0]:
        bets = []
        for race_id in sorted(month_races.keys()):
            info = month_races[race_id]
            if should_exclude_uscore(info['race_name']):
                continue
            if all(h['odds'] == 0.0 for h in info['horse_list']):
                continue
            race_info_obj = make_race_info(info)
            try:
                res = analyze_race_uscore(race_info_obj, horse_db, None, None,
                                          market_alpha=alpha, temperature=temp)
            except Exception:
                continue
            if not res:
                continue
            rank_map = {h['name']: h['rank'] for h in info['horse_list']}
            odds_map = {h['name']: h['odds'] for h in info['horse_list']}
            top = res[0]
            if top['u_score'] >= 100:
                hit = (rank_map.get(top['name'], 99) == 1)
                bets.append({'odds': odds_map.get(top['name'], top['odds']), 'hit': hit})

        n = len(bets)
        h = sum(1 for b in bets if b['hit'])
        paid = sum(b['odds'] for b in bets if b['hit'])
        roi = paid / n * 100 if n else 0.0
        wr = h / n * 100 if n else 0.0
        print(f"  {alpha:.1f}    {temp:.1f}   {n:6d}  {h:5d}  {wr:6.1f}  {roi:7.1f}")

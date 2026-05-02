# -*- coding: utf-8 -*-
import sys, csv, json, re
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, assign_marks
from uscore_backtest import _add_races_to_horse_db, load_all_csv_races

races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202603')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)

results_db = {}
with open('data/raceresults_202603.csv', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        rid  = row['race_id']
        name = row['馬名']
        rank = int(row['着順']) if row['着順'].isdigit() else 99
        if rid not in results_db:
            results_db[rid] = {}
        results_db[rid][name] = rank

n_total = 0
h1 = h2 = h3 = 0
r1 = r2 = r3 = 0

for date in ['20260328','20260329']:
    with open(f'data/races_{date}.json', encoding='utf-8') as f:
        data = json.load(f)
    for race in data.get('all_races',[]):
        rid  = race.get('race_id','')
        rnum = int(rid[-2:]) if rid else 0
        n_h  = race.get('n_horses', 99)
        if rnum < 8 or rnum > 11 or n_h > 14 or rid not in results_db:
            continue
        try:
            scored = analyze_race_uscore(race, horse_db, None, None,
                                         trainer_stats=trainer_stats, jockey_stats=jockey_stats)
        except:
            continue
        if not scored:
            continue
        marks = assign_marks(scored)
        wp_map = {h['name']: h['win_prob'] for h in scored}
        honmei_n = next((n for n,m in marks.items() if m=='◎'), None)
        retan_n  = next((n for n,m in marks.items() if m=='○'), None)
        if not honmei_n or not retan_n:
            continue
        wp_sum = wp_map.get(honmei_n,0) + wp_map.get(retan_n,0)
        odds_map = {h['name']: h.get('odds',99) for h in scored}
        if wp_sum < 35 or odds_map.get(honmei_n,99) > 4.0:
            continue

        n_total += 1
        hrank = results_db[rid].get(honmei_n, 99)
        rrank = results_db[rid].get(retan_n,  99)
        if hrank == 1: h1 += 1
        if hrank <= 2: h2 += 1
        if hrank <= 3: h3 += 1
        if rrank == 1: r1 += 1
        if rrank <= 2: r2 += 1
        if rrank <= 3: r3 += 1

print(f'対象レース: {n_total}R (フィルター後)')
print()
print(f'◎ 1着:  {h1}/{n_total} ({h1/n_total*100:.1f}%)')
print(f'◎ 2着以内: {h2}/{n_total} ({h2/n_total*100:.1f}%)')
print(f'◎ 3着以内: {h3}/{n_total} ({h3/n_total*100:.1f}%)')
print()
print(f'○ 1着:  {r1}/{n_total} ({r1/n_total*100:.1f}%)')
print(f'○ 2着以内: {r2}/{n_total} ({r2/n_total*100:.1f}%)')
print(f'○ 3着以内: {r3}/{n_total} ({r3/n_total*100:.1f}%)')

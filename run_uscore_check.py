# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import json
from uscore import (
    analyze_race_uscore, load_uscore_db,
    build_trainer_stats, build_jockey_stats,
    print_uscore_result,
)
from fetch_oikiri import build_oikiri_db

horse_db     = load_uscore_db()
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)
oikiri_db     = build_oikiri_db('20260405')

with open('data/races_20260405.json', encoding='utf-8') as f:
    data = json.load(f)

for race in data['all_races']:
    results = analyze_race_uscore(
        race, horse_db, None, None,
        trainer_stats=trainer_stats,
        jockey_stats=jockey_stats,
        oikiri_db=oikiri_db,
    )
    race['rnum'] = race.get('race_id', '')[-2:].lstrip('0')
    print_uscore_result(race, results)

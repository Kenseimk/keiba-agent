# -*- coding: utf-8 -*-
"""
analyze_niban.py  ○ 選択方法の比較分析
4種類の○選択方法の精度を比較する
  A: win_prob #2 (現行)
  B: model_prob #2 (市場に依存しないモデル純粋評価)
  C: place_prob #2 (歴史的連対実績)
  D: 市場2番人気 (純粋な市場評価)
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

WP_MIN    = 20.0
ODDS_MAX  = 4.0
WPSUM_MIN = 45.0

print('全データ読み込み中...', flush=True)
races = load_all_csv_races('data')

PERIODS = [
    ('学習 2015-2022', '201501', '202212'),
    ('テスト 2023-2024', '202301', '202412'),
]

for period_name, ym_start, ym_end in PERIODS:
    print(f'\n{"="*70}')
    print(f'【{period_name}】')
    print(f'{"="*70}')

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=ym_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)

    test_rids = sorted(rid for rid, info in races.items()
                       if ym_start <= info['file_ym'] <= ym_end)

    # 各方法の集計
    methods = {
        'A: win_prob#2（現行）':      {'cnt':0,'r_top2':0,'r_top3':0,'both_top2':0,'both_top3':0,'mkt2':0},
        'B: model_prob#2（純モデル）': {'cnt':0,'r_top2':0,'r_top3':0,'both_top2':0,'both_top3':0,'mkt2':0},
        'C: place_prob#2（連対実績）': {'cnt':0,'r_top2':0,'r_top3':0,'both_top2':0,'both_top3':0,'mkt2':0},
        'D: 市場2番人気（純市場）':    {'cnt':0,'r_top2':0,'r_top3':0,'both_top2':0,'both_top3':0,'mkt2':0},
    }

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

        wp_map    = {h['name']: h['win_prob']   for h in sc}
        mp_map    = {h['name']: h['model_prob'] for h in sc}
        pp_map    = {h['name']: h['place_prob'] for h in sc}
        odds_map  = {h['name']: h['odds']       for h in info['horse_list']}
        pop_map   = {h['name']: h['pop']        for h in info['horse_list']}
        rank_map  = {h['name']: h['rank']       for h in info['horse_list']}

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
        sorted_mp = sorted(sc, key=lambda h: h['model_prob'], reverse=True)
        sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)
        sorted_mk = sorted(info['horse_list'], key=lambda h: h['pop'])

        hn = sorted_wp[0]['name']  # ◎ = win_prob #1 (共通)

        # NAGASHIフィルター
        if wp_map.get(hn, 0) < WP_MIN: continue
        if odds_map.get(hn, 99) > ODDS_MAX: continue

        hr = rank_map.get(hn, 99)

        # A: win_prob #2
        rn_a = sorted_wp[1]['name'] if len(sorted_wp) > 1 else None
        # B: model_prob #2 (◎以外)
        rn_b = next((h['name'] for h in sorted_mp if h['name'] != hn), None)
        # C: place_prob #2 (◎以外)
        rn_c = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
        # D: 市場2番人気 (◎以外)
        rn_d = next((h['name'] for h in sorted_mk if h['name'] != hn and h['pop'] == 2), None)
        if rn_d is None:
            rn_d = next((h['name'] for h in sorted_mk if h['name'] != hn), None)

        for key, rn in [('A: win_prob#2（現行）', rn_a),
                         ('B: model_prob#2（純モデル）', rn_b),
                         ('C: place_prob#2（連対実績）', rn_c),
                         ('D: 市場2番人気（純市場）', rn_d)]:
            if rn is None: continue
            # wpsum フィルター (Aのみ: 現行ロジックに合わせる)
            if key == 'A: win_prob#2（現行）':
                if wp_map.get(hn, 0) + wp_map.get(rn, 0) < WPSUM_MIN: continue

            rr = rank_map.get(rn, 99)
            mkt_rn = pop_map.get(rn, 99)
            m = methods[key]
            m['cnt'] += 1
            if rr <= 2: m['r_top2'] += 1
            if rr <= 3: m['r_top3'] += 1
            if hr <= 2 and rr <= 2: m['both_top2'] += 1
            if hr <= 3 and rr <= 3: m['both_top3'] += 1
            if mkt_rn == 2: m['mkt2'] += 1

    print(f'\n{"方法":<26}  {"R数":>5}  '
          f'{"○連対率":>8} {"○3着内":>8}  '
          f'{"◎○両連対":>10} {"◎○両3着内":>11}  '
          f'{"○=市場2番人気率":>14}')
    print('-' * 96)
    for mname, m in methods.items():
        c = m['cnt']
        if c == 0: continue
        print(f'{mname:<26}  {c:>5}  '
              f'{m["r_top2"]/c*100:>7.1f}%  {m["r_top3"]/c*100:>7.1f}%  '
              f'{m["both_top2"]/c*100:>9.1f}%  {m["both_top3"]/c*100:>10.1f}%  '
              f'{m["mkt2"]/c*100:>13.1f}%')

print()
print('【目標】◎○両連対率を高め、かつ○=市場2番人気率を下げてオッズ優位を作る')

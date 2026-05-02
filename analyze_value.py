# -*- coding: utf-8 -*-
"""
analyze_value.py  乖離馬（Value）分析
model_prob - market_prob の差が大きい馬を○に選んだ場合の精度・回収率を測定
「市場が低評価だがモデルが高評価」= 市場の見落とし → 配当が高くなる
"""
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, should_exclude_uscore

RNUM_MIN     = 8
RNUM_MAX     = 11
MAX_FIELD    = 14
MARKET_ALPHA = 0.4

WP_MIN   = 20.0
ODDS_MAX = 4.0

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

    # 乖離度ごとのバケツ集計
    # 乖離 = model_prob - market_prob (◎以外の馬の中でtop)
    buckets = {
        '乖離≥+10%': defaultdict(int),
        '乖離+5~10%': defaultdict(int),
        '乖離±5%':   defaultdict(int),
        '乖離-5~-10%': defaultdict(int),
        '乖離≤-10%': defaultdict(int),
    }

    # ○候補別: (乖離値, 実際の着順, ◎着順, ○オッズ)
    records = []

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

        wp_map   = {h['name']: h['win_prob']    for h in sc}
        mp_map   = {h['name']: h['model_prob']  for h in sc}
        mkt_map  = {h['name']: h['market_prob'] for h in sc}
        odds_map = {h['name']: h['odds']        for h in info['horse_list']}
        pop_map  = {h['name']: h['pop']         for h in info['horse_list']}
        rank_map = {h['name']: h['rank']        for h in info['horse_list']}

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
        hn = sorted_wp[0]['name']

        # ◎フィルター
        if wp_map.get(hn, 0) < WP_MIN: continue
        if odds_map.get(hn, 99) > ODDS_MAX: continue

        hr = rank_map.get(hn, 99)

        # ◎以外の全馬について乖離を計算
        others = [h for h in sc if h['name'] != hn]
        for h in others:
            name = h['name']
            mp   = mp_map.get(name, 0)
            mkt  = mkt_map.get(name, 0)
            diff = mp - mkt  # 正 = モデルが市場より高評価
            rr   = rank_map.get(name, 99)
            od   = odds_map.get(name, 99)
            pk   = pop_map.get(name, 99)
            records.append({'diff': diff, 'hr': hr, 'rr': rr, 'odds': od, 'pop': pk})

    # 乖離別の精度集計
    print(f'\n{"乖離区分":<14}  {"候補数":>6}  {"○連対率":>8} {"○3着内":>8}  '
          f'{"◎○両連対":>10}  {"平均odds":>8}  {"平均人気":>8}')
    print('-' * 74)

    thresholds = [
        ('乖離≥+15%',   lambda d: d >= 15),
        ('乖離+10~15%', lambda d: 10 <= d < 15),
        ('乖離+5~10%',  lambda d: 5 <= d < 10),
        ('乖離+2~5%',   lambda d: 2 <= d < 5),
        ('乖離±2%',     lambda d: -2 <= d < 2),
        ('乖離-2~-5%',  lambda d: -5 <= d < -2),
        ('乖離-5~-10%', lambda d: -10 <= d < -5),
        ('乖離≤-10%',   lambda d: d < -10),
    ]

    for label, cond in thresholds:
        sub = [r for r in records if cond(r['diff'] * 100)]
        if not sub: continue
        n   = len(sub)
        rt2 = sum(1 for r in sub if r['rr'] <= 2)
        rt3 = sum(1 for r in sub if r['rr'] <= 3)
        bt2 = sum(1 for r in sub if r['hr'] <= 2 and r['rr'] <= 2)
        avg_od = sum(r['odds'] for r in sub) / n
        avg_pk = sum(r['pop'] for r in sub) / n
        print(f'{label:<14}  {n:>6}  {rt2/n*100:>7.1f}%  {rt3/n*100:>7.1f}%  '
              f'{bt2/n*100:>9.1f}%  {avg_od:>8.1f}倍  {avg_pk:>8.1f}番人気')

    # 乖離上位1頭を○にした場合のシミュレーション
    print(f'\n--- 乖離上位1頭を○に選んだ場合 (しきい値別) ---')
    print(f'{"最低乖離":>8}  {"R数":>5}  {"○連対率":>8} {"○3着内":>8}  '
          f'{"◎○両連対":>10}  {"平均○odds":>10}  {"平均○人気":>10}')
    print('-' * 75)

    race_cache = {}
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
        wp_map   = {h['name']: h['win_prob']    for h in sc}
        mp_map   = {h['name']: h['model_prob']  for h in sc}
        mkt_map  = {h['name']: h['market_prob'] for h in sc}
        odds_map = {h['name']: h['odds']        for h in info['horse_list']}
        pop_map  = {h['name']: h['pop']         for h in info['horse_list']}
        rank_map = {h['name']: h['rank']        for h in info['horse_list']}
        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
        hn = sorted_wp[0]['name']
        if wp_map.get(hn, 0) < WP_MIN: continue
        if odds_map.get(hn, 99) > ODDS_MAX: continue
        # ◎以外で乖離最大の馬
        others = [(h['name'], mp_map.get(h['name'],0) - mkt_map.get(h['name'],0))
                  for h in sc if h['name'] != hn]
        if not others: continue
        others.sort(key=lambda x: -x[1])
        best_name, best_diff = others[0]
        race_cache[race_id] = {
            'hn': hn, 'hr': rank_map.get(hn,99),
            'rn': best_name, 'rr': rank_map.get(best_name,99),
            'diff': best_diff,
            'r_odds': odds_map.get(best_name,99),
            'r_pop': pop_map.get(best_name,99),
        }

    for min_diff in [0, 2, 5, 8, 10, 15]:
        sub = [v for v in race_cache.values() if v['diff']*100 >= min_diff]
        if not sub: continue
        n   = len(sub)
        rt2 = sum(1 for r in sub if r['rr'] <= 2)
        rt3 = sum(1 for r in sub if r['rr'] <= 3)
        bt2 = sum(1 for r in sub if r['hr'] <= 2 and r['rr'] <= 2)
        avg_od = sum(r['r_odds'] for r in sub) / n
        avg_pk = sum(r['r_pop'] for r in sub) / n
        print(f'{min_diff:>7}%  {n:>5}  {rt2/n*100:>7.1f}%  {rt3/n*100:>7.1f}%  '
              f'{bt2/n*100:>9.1f}%  {avg_od:>10.1f}倍  {avg_pk:>10.1f}番人気')

print()
print('【読み方】乖離 = model_prob - market_prob')
print('  正 = モデルが市場より高評価（市場が見落としている馬）')
print('  負 = モデルが市場より低評価')

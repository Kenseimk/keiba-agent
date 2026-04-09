# -*- coding: utf-8 -*-
"""
backtest_NAGASHI_compare.py  NAGASHI 印選択方式比較
=====================================================
4方式を比較:
  A: 全てwin_prob順 (現行)
  B: ◎=win_prob#1 / ○=model_prob#2 (市場と独立)
  C: ◎=EV最高 / ○=EV2番目
  D: ◎=win_prob#1 / ○=model_prob#2 / ◎-○乖離フィルター

フィルター共通:
  - 8-11R / 頭数≤14
  - ◎ win_prob >= 20% / ◎ odds <= 4.0
  - ◎+○ (各方式のwp合計) >= 35%

実行:
  python backtest_NAGASHI_compare.py
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    analyze_race_uscore, build_trainer_stats, build_jockey_stats,
    should_exclude_uscore,
)

RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
HONMEI_WP_MIN   = 20.0
HONMEI_ODDS_MAX = 4.0
WP_SUM_MIN      = 35.0
MARKET_ALPHA    = 0.4
BET             = 100
N_AITE          = 4


def load_sanrentan(data_dir, start_ym, end_ym):
    db = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if ym < start_ym or ym > end_ym:
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


def calc_tickets(u_h, u_r, u_a):
    axis = [u_h, u_r]
    tix  = set()
    for perm in permutations(axis):
        for a in u_a:
            tix.add((*perm, a))
    return tix


def run():
    print('=== NAGASHI 印選択方式比較 ===')
    print('テスト期間: 202501〜202603\n')

    races = load_all_csv_races('data')
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym='202501')
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'horse_db: {len(horse_db):,}頭\n')

    sanrentan_db = load_sanrentan('data', '202501', '202603')
    test_rids = sorted(rid for rid, info in races.items()
                       if '202501' <= info['file_ym'] <= '202603')

    # 方式の集計
    variants = {
        'A_WP全て':       {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0, 'pop_honmei': [], 'pop_retan': []},
        'B_○model独立':   {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0, 'pop_honmei': [], 'pop_retan': []},
        'C_○place連対':   {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0, 'pop_honmei': [], 'pop_retan': []},
        'D_○model_乖離3': {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0, 'pop_honmei': [], 'pop_retan': []},
        'E_○model_乖離5': {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0, 'pop_honmei': [], 'pop_retan': []},
    }

    for race_id in test_rids:
        info = races[race_id]
        if should_exclude_uscore(info['race_name']):
            continue
        if all(h['odds'] == 0.0 for h in info['horse_list']):
            continue

        rnum = int(race_id[-2:])
        if not (RNUM_MIN <= rnum <= RNUM_MAX):
            continue
        if info['n_field'] > MAX_FIELD:
            continue

        race_obj = make_race_info(info)
        try:
            sc = analyze_race_uscore(
                race_obj, horse_db, None, None,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
                market_alpha=MARKET_ALPHA,
            )
        except:
            continue
        if not sc:
            continue

        wp_map   = {h['name']: h['win_prob']    for h in sc}
        mp_map   = {h['name']: h['model_prob']  for h in sc}
        mkt_map  = {h['name']: h['market_prob'] for h in sc}
        pp_map   = {h['name']: h['place_prob']  for h in sc}
        odds_map = {h['name']: h['odds']        for h in info['horse_list']}
        uma_map  = {h['name']: h['umaban']      for h in info['horse_list']}
        pop_map  = {h['name']: h['pop']         for h in info['horse_list']}

        sorted_wp  = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
        sorted_mp  = sorted(sc, key=lambda h: h['model_prob'], reverse=True)
        sorted_pp  = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

        san = sanrentan_db.get(race_id, {})

        # ── 共通ベース: ◎ = win_prob #1 ──────────────────
        hn_wp = sorted_wp[0]['name']

        def apply_bet(vkey, hn, rn):
            if wp_map.get(hn, 0) < HONMEI_WP_MIN:
                return
            if odds_map.get(hn, 99) > HONMEI_ODDS_MAX:
                return
            if wp_map.get(hn, 0) + wp_map.get(rn, 0) < WP_SUM_MIN:
                return
            u_h = uma_map.get(hn, '')
            u_r = uma_map.get(rn, '')
            if not u_h or not u_r:
                return
            aite_names = [h['name'] for h in sorted_wp if h['name'] not in (hn, rn)][:N_AITE]
            u_a = [uma_map.get(n, '') for n in aite_names if uma_map.get(n, '')]
            if not u_a:
                return
            tix  = calc_tickets(u_h, u_r, u_a)
            cost = len(tix) * BET
            ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
            hit  = int(ret > 0)
            v = variants[vkey]
            v['cost']  += cost
            v['ret']   += ret
            v['races'] += 1
            v['hits']  += hit
            v['pop_honmei'].append(pop_map.get(hn, 99))
            v['pop_retan'].append(pop_map.get(rn, 99))

        # A: 全てwin_prob
        rn_wp = sorted_wp[1]['name'] if len(sorted_wp) > 1 else None
        if rn_wp:
            apply_bet('A_WP全て', hn_wp, rn_wp)

        # B: ◎=win_prob#1 / ○=model_prob#2 (◎以外)
        mp_others = [h['name'] for h in sorted_mp if h['name'] != hn_wp]
        if mp_others:
            rn_b = mp_others[0]
            apply_bet('B_○model独立', hn_wp, rn_b)

        # C: ◎=win_prob#1 / ○=place_prob#2 (連対特化スコア)
        pp_others = [h['name'] for h in sorted_pp if h['name'] != hn_wp]
        if pp_others:
            rn_c = pp_others[0]
            apply_bet('C_○place連対', hn_wp, rn_c)

        # D: ◎=win_prob#1 / ○=model_prob#2 / ○乖離≥3% (○のmodel>market)
        # ○がモデルで高評価だが市場で低評価 → 穴○狙い
        disagree_h = mp_map.get(hn_wp, 0) - mkt_map.get(hn_wp, 0)
        if mp_others:
            rn_d = mp_others[0]
            disagree_r = mp_map.get(rn_d, 0) - mkt_map.get(rn_d, 0)
            if disagree_r >= 3.0:
                apply_bet('D_○model_乖離3', hn_wp, rn_d)

        # E: 乖離≥5%
        if mp_others:
            rn_e = mp_others[0]
            disagree_re = mp_map.get(rn_e, 0) - mkt_map.get(rn_e, 0)
            if disagree_re >= 5.0:
                apply_bet('E_○model_乖離5', hn_wp, rn_e)

    # 結果表示
    print(f'{"方式":<18} {"R数":>5} {"的中":>5} {"的中率":>7} {"投資":>9} {"回収":>9} {"収支":>10} {"ROI":>7}')
    print('-' * 76)
    for vkey, v in variants.items():
        n = v['races']; h = v['hits']; c = v['cost']; r = v['ret']
        if n == 0:
            print(f'{vkey:<18}     -     -       -          -          -           -       -')
            continue
        roi = r / c * 100 if c else 0
        print(f'{vkey:<18} {n:>5} {h:>5} {h/n*100:>6.1f}%  {c:>9,} {r:>9,} {r-c:>+10,} {roi:>6.1f}%')

    print()
    print('◎/○の市場人気分布:')
    print(f'{"方式":<18} {"◎avg_pop":>10} {"○avg_pop":>10} {"◎=1番人気%":>12} {"○=2番人気%":>12}')
    print('-' * 60)
    for vkey, v in variants.items():
        hp = v['pop_honmei']; rp = v['pop_retan']
        if not hp:
            print(f'{vkey:<18}          -          -            -            -')
            continue
        avg_h = sum(hp) / len(hp)
        avg_r = sum(rp) / len(rp)
        pct_h1 = sum(1 for p in hp if p == 1) / len(hp) * 100
        pct_r2 = sum(1 for p in rp if p == 2) / len(rp) * 100
        print(f'{vkey:<18} {avg_h:>10.1f} {avg_r:>10.1f} {pct_h1:>11.1f}% {pct_r2:>11.1f}%')


if __name__ == '__main__':
    run()

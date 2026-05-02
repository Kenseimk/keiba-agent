# -*- coding: utf-8 -*-
"""
compare_strategies.py
A (1軸×6頭) vs B (2軸×5頭 place_prob○) vs B2 (2軸×5頭 複合スコア○) を4月2026データで比較
"""
import os, sys, json, csv, re, itertools
from collections import defaultdict
os.environ['PYTHONIOENCODING'] = 'utf-8'

import lightgbm as lgb
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats
from ml_features import race_ml_probs
from predict_ml_sanrenpuku import (
    load_models, json_to_info, RNUM_MIN, RNUM_MAX,
    MAX_FIELD, EXCLUDE_KEYWORDS, VENUE_MAP,
    TURF_WP_MIN, TURF_WP_MAX, HONMEI_WP_MIN, WP_SUM_MIN
)

DATA_DIR   = 'data'
RESULT_CSVS = ['data/raceresults_202604.csv',
               'data/raceresults_202604_extra.csv']
APRIL_JSON = [
    'data/races_20260405.json',
    'data/races_20260406.json',
    'data/races_20260412.json',
    'data/races_20260413.json',
    'data/races_20260418.json',
]
BET = 100


def load_results(paths):
    ranks   = defaultdict(dict)
    sanfuku = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row.get('race_id', '').strip()
                try:
                    ranks[rid][int(row.get('馬番', 0))] = int(row.get('着順', 99))
                except Exception:
                    pass
                pay = row.get('三連複払戻', '').strip()
                if pay and rid not in sanfuku:
                    m = re.search(r'(\d[\d,]+)', pay)
                    if m:
                        sanfuku[rid] = int(m.group(1).replace(',', ''))
    return ranks, sanfuku


def get_candidates(race_id, info, horse_db, trainer_stats, jockey_stats,
                   turf_win, turf_pl, dirt_win, dirt_pl):
    is_dirt = 'ダ' in str(info.get('course', ''))
    mw = dirt_win if is_dirt else turf_win
    mp = dirt_pl  if is_dirt else turf_pl
    sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats, mw, mp)
    if not sc or len(sc) < 4:
        return None, None, None, None, None, None, None, None

    wp_map  = {h['name']: h['win_prob']   for h in sc}
    pp_map  = {h['name']: h['place_prob'] for h in sc}
    uma_map = {h['name']: h['umaban']     for h in sc}

    sorted_wp = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
    sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)
    sorted_cs = sorted(sc, key=lambda h: 0.5*h['win_prob'] + 0.5*h['place_prob'], reverse=True)

    honmei = sorted_wp[0]['name']

    # B用: place_prob 最高(◎除く)
    renka_b = next((h['name'] for h in sorted_pp if h['name'] != honmei), None)
    # B2用: 複合スコア(win+place)/2 最高(◎除く)
    renka_b2 = next((h['name'] for h in sorted_cs if h['name'] != honmei), None)

    if not renka_b:
        return None, None, None, None, None, None, None, None

    wp_h   = wp_map.get(honmei, 0)
    wp_r   = wp_map.get(renka_b, 0)
    wp_sum = wp_h + wp_r

    if wp_sum < WP_SUM_MIN:
        return None, None, None, None, None, None, None, None
    if is_dirt:
        if wp_h < HONMEI_WP_MIN:
            return None, None, None, None, None, None, None, None
    else:
        if not (TURF_WP_MIN <= wp_h < TURF_WP_MAX):
            return None, None, None, None, None, None, None, None

    # 相手候補: place_prob順、◎を除く
    aite_cands = [h['name'] for h in sorted_pp if h['name'] != honmei]

    return honmei, renka_b, renka_b2, aite_cands, uma_map, is_dirt, wp_h, wp_r


def tickets_A(honmei, aite_cands, uma_map, n=6):
    """1軸×n頭 = C(n,2)通"""
    u_h = uma_map.get(honmei, '')
    aite6 = aite_cands[:n]
    u_aite = [uma_map.get(a, '') for a in aite6 if uma_map.get(a, '')]
    combos = list(itertools.combinations(u_aite, 2))
    return [(u_h, a, b) for a, b in combos], aite6


def tickets_B(honmei, renka, aite_cands, uma_map, n=5):
    """2軸×n頭 = n通"""
    u_h = uma_map.get(honmei, '')
    u_r = uma_map.get(renka,  '')
    aite5 = [a for a in aite_cands if a != renka][:n]
    u_aite = [uma_map.get(a, '') for a in aite5 if uma_map.get(a, '')]
    return [(u_h, u_r, ua) for ua in u_aite], aite5


def check_hit(combo_ubs, race_ranks):
    top3 = {ub for ub, r in race_ranks.items() if r <= 3}
    for u1, u2, u3 in combo_ubs:
        try:
            if {int(u1), int(u2), int(u3)} == top3:
                return True
        except Exception:
            pass
    return False


def run():
    print('=' * 84)
    print('  戦略比較: A(1軸×6) vs B(2軸×5 place_prob○) vs B2(2軸×5 複合スコア○)  4月2026')
    print('=' * 84)

    turf_win, turf_pl, dirt_win, dirt_pl = load_models()

    print('過去データ読み込み中...')
    races = load_all_csv_races(DATA_DIR)
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym='202604')
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'  horse_db: {len(horse_db):,}頭\n')

    ranks, sanfuku = load_results(RESULT_CSVS)

    stats = {k: {'bet': 0, 'ret': 0, 'hit': 0, 'total': 0} for k in ('A', 'B', 'B2')}

    print(f'{"レース":<20} {"◎(wp)":>6} {"○B(pp)":>6} {"○B2(cs)":>7} '
          f'{"A結果":<20} {"B結果":<20} {"B2結果"}')
    print('-' * 100)

    for json_path in APRIL_JSON:
        if not os.path.exists(json_path):
            continue
        with open(json_path, encoding='utf-8') as f:
            race_data = json.load(f)
        date_str = race_data.get('date', '')
        file_ym  = date_str[:6] if date_str else '202604'

        for race in race_data.get('all_races', []):
            race_id   = race.get('race_id', '')
            race_name = race.get('race_name', '')
            rnum = int(race_id[-2:]) if race_id else 0

            if not (RNUM_MIN <= rnum <= RNUM_MAX):
                continue
            if any(kw in race_name for kw in EXCLUDE_KEYWORDS):
                continue
            if race.get('n_horses', 0) > MAX_FIELD:
                continue
            if all(float(o.get('odds', 0) or 0) == 0.0
                   for o in race.get('odds', [])):
                continue

            info   = json_to_info(race, file_ym)
            result = get_candidates(
                race_id, info, horse_db, trainer_stats, jockey_stats,
                turf_win, turf_pl, dirt_win, dirt_pl)

            if result[0] is None:
                continue

            honmei, renka_b, renka_b2, aite_cands, uma_map, is_dirt, wp_h, wp_r = result
            venue = VENUE_MAP.get(race_id[4:6], '?')
            label = f'{venue}{rnum}R {race_name[:8]}'

            race_ranks = ranks.get(race_id, {})
            payout     = sanfuku.get(race_id, 0)

            def eval_strategy(combos, bet_n):
                for k_stat in ('A', 'B', 'B2'):
                    pass  # will be done per-strategy below
                if not race_ranks:
                    return '結果なし', 0
                if check_hit(combos, race_ranks):
                    ret = (payout * BET // 100) if payout else 0
                    return (f'★¥{ret:,}' if payout else '★(払戻不明)'), ret
                else:
                    top3 = sorted(ub for ub, r in race_ranks.items() if r <= 3)
                    return f'外れ{top3}', 0

            # Strategy A
            combos_a, _ = tickets_A(honmei, aite_cands, uma_map, n=6)
            bet_a = len(combos_a) * BET
            stats['A']['total'] += 1
            stats['A']['bet']   += bet_a
            res_a, ret_a = eval_strategy(combos_a, len(combos_a))
            if race_ranks and check_hit(combos_a, race_ranks):
                stats['A']['hit'] += 1
                stats['A']['ret'] += ret_a

            # Strategy B (place_prob ○)
            combos_b, _ = tickets_B(honmei, renka_b, aite_cands, uma_map, n=5)
            bet_b = len(combos_b) * BET
            stats['B']['total'] += 1
            stats['B']['bet']   += bet_b
            res_b, ret_b = eval_strategy(combos_b, len(combos_b))
            if race_ranks and check_hit(combos_b, race_ranks):
                stats['B']['hit'] += 1
                stats['B']['ret'] += ret_b

            # Strategy B2 (composite score ○)
            combos_b2, _ = tickets_B(honmei, renka_b2, aite_cands, uma_map, n=5)
            bet_b2 = len(combos_b2) * BET
            stats['B2']['total'] += 1
            stats['B2']['bet']   += bet_b2
            res_b2, ret_b2 = eval_strategy(combos_b2, len(combos_b2))
            if race_ranks and check_hit(combos_b2, race_ranks):
                stats['B2']['hit'] += 1
                stats['B2']['ret'] += ret_b2

            u_h  = uma_map.get(honmei, '?')
            u_rb = uma_map.get(renka_b, '?')
            u_rb2= uma_map.get(renka_b2, '?')
            print(f'{label:<20} {u_h}({wp_h:.1f}%) {u_rb}({wp_r:.1f}%) '
                  f'{u_rb2:>3}  '
                  f'{res_a:<22} {res_b:<22} {res_b2}')

    print('=' * 84)
    print(f'  {"戦略":<6} {"R数":>4} {"的中":>8}  {"投資":>8}  {"回収":>8}  {"ROI":>7}  {"損益":>9}')
    print('-' * 84)
    for k in ('A', 'B', 'B2'):
        s = stats[k]
        roi = s['ret'] / s['bet'] * 100 if s['bet'] else 0
        pl  = s['ret'] - s['bet']
        hit_r = f'{s["hit"]}/{s["total"]}'
        pct   = s['hit'] / s['total'] * 100 if s['total'] else 0
        label = {'A': 'A (1軸×6)', 'B': 'B (2軸×5 pp)', 'B2': 'B2(2軸×5 cs)'}[k]
        print(f'  {label:<12} {s["total"]:>4}R  {hit_r}({pct:.0f}%)'
              f'  ¥{s["bet"]:>7,}  ¥{s["ret"]:>7,}  {roi:>6.1f}%  {pl:>+9,}円')
    print('=' * 84)


def pp_map_str(v):
    return f'{v:.0%}'


if __name__ == '__main__':
    run()

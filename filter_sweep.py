# -*- coding: utf-8 -*-
"""
filter_sweep.py
フィルター条件を変えて4月2026データでROIを比較
"""
import os, sys, json, csv, re, itertools
from collections import defaultdict
os.environ['PYTHONIOENCODING'] = 'utf-8'

import lightgbm as lgb
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats
from ml_features import race_ml_probs
from predict_ml_sanrenpuku import (
    load_models, json_to_info, VENUE_MAP, EXCLUDE_KEYWORDS, N_AITE, BET
)

DATA_DIR    = 'data'
RESULT_CSVS = ['data/raceresults_202604.csv', 'data/raceresults_202604_extra.csv']
APRIL_JSON  = [
    'data/races_20260405.json', 'data/races_20260406.json',
    'data/races_20260412.json', 'data/races_20260413.json',
    'data/races_20260418.json',
]

# ── テストするフィルター設定 ──────────────────────────────────
CONFIGS = [
    dict(name='[現在] 芝25-30% ダ≥20% ◎○≥45% 8-11R ≤14頭',
         turf_min=25, turf_max=30, dirt_min=20, wp_sum=45,
         rmin=8, rmax=11, max_field=14),

    dict(name='[A] 芝20-35% (幅拡大)',
         turf_min=20, turf_max=35, dirt_min=20, wp_sum=45,
         rmin=8, rmax=11, max_field=14),

    dict(name='[B] 芝上限撤廃 ≥20%',
         turf_min=20, turf_max=99, dirt_min=20, wp_sum=45,
         rmin=8, rmax=11, max_field=14),

    dict(name='[C] ◎○合計 ≥40% (緩和)',
         turf_min=25, turf_max=30, dirt_min=20, wp_sum=40,
         rmin=8, rmax=11, max_field=14),

    dict(name='[D] レース範囲 7-12R',
         turf_min=25, turf_max=30, dirt_min=20, wp_sum=45,
         rmin=7, rmax=12, max_field=14),

    dict(name='[E] 頭数上限 16頭',
         turf_min=25, turf_max=30, dirt_min=20, wp_sum=45,
         rmin=8, rmax=11, max_field=16),

    dict(name='[F] A+C+D+E (全部緩め)',
         turf_min=20, turf_max=99, dirt_min=20, wp_sum=40,
         rmin=7, rmax=12, max_field=16),
]


def load_results(paths):
    ranks, sanfuku = defaultdict(dict), {}
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


def run_config(cfg, all_races_by_file, horse_db, trainer_stats, jockey_stats,
               turf_win, turf_pl, dirt_win, dirt_pl, ranks, sanfuku):
    bet_total = ret_total = hit = total = no_result = 0
    details = []

    for file_ym, races in all_races_by_file:
        for race in races:
            race_id   = race.get('race_id', '')
            race_name = race.get('race_name', '')
            rnum = int(race_id[-2:]) if race_id else 0

            if not (cfg['rmin'] <= rnum <= cfg['rmax']):
                continue
            if any(kw in race_name for kw in EXCLUDE_KEYWORDS):
                continue
            if race.get('n_horses', 0) > cfg['max_field']:
                continue
            if all(float(o.get('odds', 0) or 0) == 0.0
                   for o in race.get('odds', [])):
                continue

            info    = json_to_info(race, file_ym)
            is_dirt = 'ダ' in str(info.get('course', ''))
            mw = dirt_win if is_dirt else turf_win
            mp = dirt_pl  if is_dirt else turf_pl

            sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats, mw, mp)
            if not sc or len(sc) < 4:
                continue

            wp_map  = {h['name']: h['win_prob']   for h in sc}
            pp_map  = {h['name']: h['place_prob'] for h in sc}
            uma_map = {h['name']: h['umaban']     for h in sc}

            sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
            sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

            hn = sorted_wp[0]['name']
            rn = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
            if not rn:
                continue

            wp_h   = wp_map.get(hn, 0)
            wp_r   = wp_map.get(rn, 0)
            wp_sum = wp_h + wp_r

            if wp_sum < cfg['wp_sum']:
                continue
            if is_dirt:
                if wp_h < cfg['dirt_min']:
                    continue
            else:
                if not (cfg['turf_min'] <= wp_h < cfg['turf_max']):
                    continue

            aite_cands = [h['name'] for h in sorted_pp if h['name'] != hn]
            aite = aite_cands[:N_AITE]
            u_h = uma_map.get(hn, '')
            u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
            if not u_h or len(u_a) < 2:
                continue

            combos = [(u_h, a, b) for a, b in itertools.combinations(u_a, 2)]
            n_tick = len(combos)
            total += 1
            bet_total += n_tick * BET

            race_ranks = ranks.get(race_id, {})
            payout     = sanfuku.get(race_id, 0)

            if not race_ranks:
                no_result += 1
                details.append((race_id, race_name, rnum, wp_h, None, 0))
                continue

            top3 = {ub for ub, r in race_ranks.items() if r <= 3}
            hit_flag = any(
                {int(u1), int(u2), int(u3)} == top3
                for u1, u2, u3 in combos
                if all(x.isdigit() for x in (u1, u2, u3))
            )
            if hit_flag:
                hit += 1
                ret = (payout * BET // 100) if payout else 0
                ret_total += ret
            else:
                ret = 0
            details.append((race_id, race_name, rnum, wp_h, hit_flag, ret))

    roi = ret_total / bet_total * 100 if bet_total else 0
    pl  = ret_total - bet_total
    pct = hit / (total - no_result) * 100 if (total - no_result) > 0 else 0
    return dict(total=total, hit=hit, no_result=no_result,
                bet=bet_total, ret=ret_total,
                roi=roi, pl=pl, pct=pct, details=details)


def main():
    print('=' * 80)
    print('  フィルター条件スイープ  4月2026データ')
    print('=' * 80)

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

    # 全JSONを事前ロード
    all_races_by_file = []
    for jp in APRIL_JSON:
        if not os.path.exists(jp):
            continue
        with open(jp, encoding='utf-8') as f:
            rd = json.load(f)
        ym = rd.get('date', '')[:6] or '202604'
        all_races_by_file.append((ym, rd.get('all_races', [])))

    print(f'{"設定":<44} {"R数":>4} {"的中率":>7} {"投資":>8} {"回収":>8} {"ROI":>7} {"損益":>9}')
    print('-' * 88)

    results = []
    for cfg in CONFIGS:
        r = run_config(cfg, all_races_by_file, horse_db, trainer_stats, jockey_stats,
                       turf_win, turf_pl, dirt_win, dirt_pl, ranks, sanfuku)
        results.append((cfg, r))
        nr = r['no_result']
        nr_note = f'(結果なし{nr}R含)' if nr else ''
        print(f'  {cfg["name"]:<42} {r["total"]:>4}R  '
              f'{r["hit"]}/{r["total"]-r["no_result"]}({r["pct"]:.0f}%)'
              f'  ¥{r["bet"]:>6,}  ¥{r["ret"]:>6,}  {r["roi"]:>6.1f}%  {r["pl"]:>+8,}円 {nr_note}')

    print('=' * 80)
    print()

    # 現在との差分でヒット増加分を表示
    base = results[0][1]
    print('  [現在比較: 増加レース内訳]')
    print(f'  {"設定":<44} {"増R":>4}  {"的中増":>6}  {"損益差":>10}')
    print('-' * 70)
    for cfg, r in results[1:]:
        d_total = r['total']   - base['total']
        d_hit   = r['hit']     - base['hit']
        d_pl    = r['pl']      - base['pl']
        print(f'  {cfg["name"]:<44} {d_total:>+4}R  {d_hit:>+6}件  {d_pl:>+10,}円')
    print('=' * 80)

    # ベスト設定の詳細を表示
    best_cfg, best_r = max(results, key=lambda x: x[1]['roi'])
    if best_cfg['name'] != results[0][0]['name']:
        print(f'\n  ★ ROI最高: {best_cfg["name"]}  ROI {best_r["roi"]:.1f}%')
        print('  　的中レース:')
        for rid, rname, rnum, wp, hit_flag, ret in best_r['details']:
            if hit_flag:
                venue = VENUE_MAP.get(rid[4:6], '?')
                print(f'    {venue}{rnum}R {rname[:12]}  ◎wp{wp:.1f}%  ¥{ret:,}')


if __name__ == '__main__':
    main()

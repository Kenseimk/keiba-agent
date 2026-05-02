# -*- coding: utf-8 -*-
"""
backtest_SANRENTAN_ml.py  三連単 × LightGBM ML モデル バックテスト
==================================================================
◎ = ML win_prob  #1
○ = ML place_prob #2 (◎以外)
ながし相手 = ML place_prob 上位N頭 (◎○以外)

買い目:
  - 三連単C1: ◎○二軸 × 相手N頭
  - 1着-2着-(◎,○)の順列 × 相手3着 = 2×N通り
  - dynamic_aite: ◎ wp>=25% → 3頭, else → 4頭

実行:
  python backtest_SANRENTAN_ml.py
  python backtest_SANRENTAN_ml.py --start 202501 --end 202603
  python backtest_SANRENTAN_ml.py --verbose
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'

import lightgbm as lgb
from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats, should_exclude_uscore
from ml_features import race_ml_probs

# ── パラメータ ────────────────────────────────────
RNUM_MIN      = 8
RNUM_MAX      = 11
MAX_FIELD     = 14
HONMEI_WP_MIN = 20.0
WP_SUM_MIN    = 45.0
N_AITE        = 4
BET           = 100


def load_sanrentan(data_dir, start_ym, end_ym):
    """三連単払戻DB: {race_id: {(u1,u2,u3): 払戻額}}"""
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
                val = row.get('三連単払戻', '').strip()
                if val and val != '-':
                    mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', val)
                    if mm:
                        key = (mm.group(1), mm.group(2), mm.group(3))
                        db[rid][key] = int(mm.group(4))
    return db


def run(test_start='202501', test_end='202603', verbose=False,
        dynamic_aite=True, n_aite=None, wp_sum_min=None):
    _n_aite     = n_aite     if n_aite     is not None else N_AITE
    _wp_sum_min = wp_sum_min if wp_sum_min is not None else WP_SUM_MIN

    print(f'=== 三連単 ML バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'条件: {RNUM_MIN}-{RNUM_MAX}R / 頭数≤{MAX_FIELD} / '
          f'◎wp≥{HONMEI_WP_MIN}% / ◎+○wp≥{_wp_sum_min}%')
    dyn_str = ' [dynamic: wp≥25%→3頭, else→4頭]' if dynamic_aite else ''
    print(f'買い目: 三連単C1 ◎○軸×place上位{_n_aite}頭{dyn_str} = 2×N通り')
    print(f'モデル: 芝/ダート別アンサンブル\n')

    print('LightGBM モデル読み込み中...', flush=True)
    _turf_win = ([lgb.Booster(model_file='ml_model_win_turf.txt')] +
                 [lgb.Booster(model_file=f'ml_model_win_turf_e{i}.txt') for i in range(3)])
    _turf_pl  = ([lgb.Booster(model_file='ml_model_place_turf.txt')] +
                 [lgb.Booster(model_file=f'ml_model_place_turf_e{i}.txt') for i in range(3)])
    _dirt_win = ([lgb.Booster(model_file='ml_model_win_dirt.txt')] +
                 [lgb.Booster(model_file=f'ml_model_win_dirt_e{i}.txt') for i in range(3)])
    _dirt_pl  = ([lgb.Booster(model_file='ml_model_place_dirt.txt')] +
                 [lgb.Booster(model_file=f'ml_model_place_dirt_e{i}.txt') for i in range(3)])
    print('  → 芝×4 / ダート×4 読み込み完了\n')

    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R')

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=test_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'horse_db: {len(horse_db):,}頭\n')

    sanrentan_db = load_sanrentan('data', test_start, test_end)
    test_rids = sorted(rid for rid, info in races.items()
                       if test_start <= info['file_ym'] <= test_end)

    monthly = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0})

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

        is_dirt = 'ダ' in str(info.get('course', ''))
        _mw = _dirt_win if is_dirt else _turf_win
        _mp = _dirt_pl  if is_dirt else _turf_pl

        sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats, _mw, _mp)
        if not sc or len(sc) < 4:
            continue

        wp_map  = {h['name']: h['win_prob']   for h in sc}
        pp_map  = {h['name']: h['place_prob'] for h in sc}
        uma_map = {h['name']: h['umaban']     for h in sc}

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
        sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

        hn = sorted_wp[0]['name']
        rn = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
        if not rn:
            continue

        if wp_map.get(hn, 0) < HONMEI_WP_MIN:
            continue
        if wp_map.get(hn, 0) + wp_map.get(rn, 0) < _wp_sum_min:
            continue

        aite_cands = [h['name'] for h in sorted_pp if h['name'] not in (hn, rn)]
        if dynamic_aite:
            cur_n = 3 if wp_map.get(hn, 0) >= 25 else 4
        else:
            cur_n = _n_aite
        aite = aite_cands[:cur_n]

        u_h = uma_map.get(hn, '')
        u_r = uma_map.get(rn, '')
        u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
        if not u_h or not u_r or not u_a:
            continue

        # 三連単C1: ◎○二軸 × 相手i (1-2着に◎○の順列、3着に相手)
        tix = set()
        for perm in permutations([u_h, u_r]):
            for a in u_a:
                tix.add((*perm, a))

        san  = sanrentan_db.get(race_id, {})
        cost = len(tix) * BET
        ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
        hit  = int(ret > 0)

        ym = info['file_ym']
        monthly[ym]['cost']  += cost
        monthly[ym]['ret']   += ret
        monthly[ym]['races'] += 1
        monthly[ym]['hits']  += hit

        if verbose and hit:
            pay = max(san.get(t, 0) for t in tix if san.get(t, 0) > 0)
            print(f'  ✓ {race_id} {info["race_name"]}  '
                  f'◎{hn}/wp{wp_map[hn]:.0f}%  ○{rn}/pp{pp_map[rn]:.0f}%  '
                  f'→ {ret:,}円 (最高{pay:,}円)')

    print(f'{"月":>8}  {"R数":>4}  {"的中":>4}  {"的中率":>6}  {"投資":>8}  {"回収":>8}  {"収支":>9}  {"ROI":>7}')
    print('-' * 72)
    total = defaultdict(int)
    for ym in sorted(monthly):
        s = monthly[ym]
        n = s['races']; h = s['hits']; c = s['cost']; r = s['ret']
        roi = r / c * 100 if c else 0
        mark = '✓' if roi >= 100 else '✗'
        print(f'{ym:>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  {c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}% {mark}')
        for k in ['races', 'hits', 'cost', 'ret']:
            total[k] += s[k]

    print('-' * 72)
    n = total['races']; h = total['hits']; c = total['cost']; r = total['ret']
    roi = r / c * 100 if c else 0
    print(f'{"合計":>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  {c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}%')
    print()
    black = sum(1 for s in monthly.values() if s['ret'] > s['cost'])
    red   = len(monthly) - black
    print(f'黒字月: {black}ヶ月  赤字月: {red}ヶ月')
    if monthly:
        avg_cost = total['cost'] // len(monthly)
        avg_bal  = (total['ret'] - total['cost']) // len(monthly)
        print(f'月平均投資: {avg_cost:,}円  月平均収支: {avg_bal:+,}円')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',       default='202501')
    parser.add_argument('--end',         default='202603')
    parser.add_argument('--verbose',     action='store_true')
    parser.add_argument('--no_dynamic',  action='store_true')
    parser.add_argument('--n_aite',      type=int,   default=None)
    parser.add_argument('--wp_sum_min',  type=float, default=None)
    args = parser.parse_args()
    run(args.start, args.end, args.verbose,
        dynamic_aite=not args.no_dynamic,
        n_aite=args.n_aite,
        wp_sum_min=args.wp_sum_min)

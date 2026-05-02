# -*- coding: utf-8 -*-
"""
backtest_NAGASHI_ml.py  NAGASHI × LightGBM ML モデル バックテスト
================================================================
市場オッズに依存しない LightGBM モデルを使用した NAGASHI 戦略

◎ = ML win_prob  #1
○ = ML place_prob #2 (◎以外)
▲☆△ = ML win_prob 3〜6位

フィルター条件:
  - 8R〜11R
  - 出走頭数 <= 14頭
  - ◎ win_prob >= 20%
  - ◎ + ○ win_prob 合計 >= 45%
  (◎ オッズ上限なし)

買い目:
  - 三連単C1: ◎○二軸 × ▲☆△ながし (win_prob 3〜6位 計4頭)
  - 計8通 × 100円 = 800円/R

バックテスト期間: デフォルト 202301〜202412 (テスト期間)

実行:
  python backtest_NAGASHI_ml.py
  python backtest_NAGASHI_ml.py --start 201501 --end 202412
  python backtest_NAGASHI_ml.py --verbose
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'

import lightgbm as lgb
from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import build_trainer_stats, build_jockey_stats, should_exclude_uscore
from ml_features import race_ml_probs

# ── パラメータ ────────────────────────────────────
RNUM_MIN      = 8
RNUM_MAX      = 11
MAX_FIELD     = 14
HONMEI_WP_MIN = 20.0   # ◎ win_prob 下限
WP_SUM_MIN    = 45.0   # ◎+○ win_prob合計下限
N_AITE        = 4      # ながし相手頭数 (win_prob 3〜6位)
BET           = 100


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


def run(test_start='202301', test_end='202412', verbose=False):
    print(f'=== NAGASHI ML バックテスト (オッズ上限なし) ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'条件: {RNUM_MIN}-{RNUM_MAX}R / 頭数≤{MAX_FIELD} / '
          f'◎wp≥{HONMEI_WP_MIN}% / ◎odds上限なし / ◎+○wp≥{WP_SUM_MIN}%')
    print(f'○選択: place_prob #2 (◎以外)')
    print(f'買い目: 三連単C1 ◎○二軸×▲☆△({N_AITE}頭) = 8通/R')
    print(f'モデル: ml_model_win.txt / ml_model_place.txt (市場非依存 LightGBM)\n')

    print('LightGBM モデル読み込み中...', flush=True)
    model_win   = lgb.Booster(model_file='ml_model_win_bin.txt')
    model_place = lgb.Booster(model_file='ml_model_place.txt')
    print('  → 読み込み完了\n')

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

        # ML 予測
        sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats,
                           model_win, model_place)
        if not sc or len(sc) < 4:
            continue

        wp_map   = {h['name']: h['win_prob']   for h in sc}
        pp_map   = {h['name']: h['place_prob'] for h in sc}
        odds_map = {h['name']: h['odds']       for h in sc}
        uma_map  = {h['name']: h['umaban']     for h in sc}

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
        sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

        # ◎ = win_prob #1
        hn = sorted_wp[0]['name']
        # ○ = place_prob #2 (◎以外)
        rn = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
        if not rn:
            continue

        # フィルター (オッズ上限なし)
        if wp_map.get(hn, 0) < HONMEI_WP_MIN:
            continue
        if wp_map.get(hn, 0) + wp_map.get(rn, 0) < WP_SUM_MIN:
            continue

        # ながし相手: win_prob 3〜(N_AITE+2)位 (◎○以外)
        aite_cands = [h['name'] for h in sorted_wp if h['name'] not in (hn, rn)]
        aite = aite_cands[:N_AITE]

        u_h = uma_map.get(hn, '')
        u_r = uma_map.get(rn, '')
        u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
        if not u_h or not u_r or not u_a:
            continue

        # 三連単C1
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
            pay = max(san.get(t, 0) for t in tix)
            print(f'  ✓ {race_id} {info["race_name"]}  '
                  f'◎{hn}/wp{wp_map[hn]:.0f}%/odds{odds_map.get(hn,0):.1f}  '
                  f'○{rn}/pp{pp_map[rn]:.0f}%  '
                  f'→ {ret:,}円 (配当{pay:,}円)')

    # 月別集計
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

    print()
    print('【参考: オッズ上限あり (≤4.0) 2023-2024 ROI: 56.0%  2024-2025 ROI: 70.4%】')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',   default='202301')
    parser.add_argument('--end',     default='202412')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    run(args.start, args.end, args.verbose)

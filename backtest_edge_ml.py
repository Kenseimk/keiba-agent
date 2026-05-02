# -*- coding: utf-8 -*-
"""
backtest_edge_ml.py  エッジフィルター × 三連複 バックテスト
============================================================
edge = (ML win_prob / 100) × 単勝オッズ

edge >= threshold の場合のみ購入。
モデルが市場より高く評価している（割安）レースのみに絞る。

各テスト期間にはウォークフォワードモデルを使用（完全ブラインド）:
  2023: models/wf_2023
  2024: models/wf_2024
  2025: models/wf_2025
  2026: models/wf_2026

実行:
  python backtest_edge_ml.py
  python backtest_edge_ml.py --threshold 1.5
  python backtest_edge_ml.py --scan   # 複数閾値をスキャン
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import lightgbm as lgb
from collections import defaultdict

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats, should_exclude_uscore
from ml_features import race_ml_probs

# ── パラメータ ────────────────────────────────────
RNUM_MIN      = 8
RNUM_MAX      = 11
MAX_FIELD     = 14
HONMEI_WP_MIN = 15.0   # 最低 ML win_prob（緩め）
N_AITE        = 4
BET           = 100

# ウォークフォワードモデルと期間の対応
WF_PERIODS = [
    ('202301', '202312', 'models/wf_2023'),
    ('202401', '202412', 'models/wf_2024'),
    ('202501', '202512', 'models/wf_2025'),
    ('202601', '202603', 'models/wf_2026'),
]


def load_models(model_dir):
    def mp(name):
        return os.path.join(model_dir, name)
    turf_win = ([lgb.Booster(model_file=mp('ml_model_win_turf.txt'))] +
                [lgb.Booster(model_file=mp(f'ml_model_win_turf_e{i}.txt')) for i in range(3)])
    turf_pl  = ([lgb.Booster(model_file=mp('ml_model_place_turf.txt'))] +
                [lgb.Booster(model_file=mp(f'ml_model_place_turf_e{i}.txt')) for i in range(3)])
    dirt_win = ([lgb.Booster(model_file=mp('ml_model_win_dirt.txt'))] +
                [lgb.Booster(model_file=mp(f'ml_model_win_dirt_e{i}.txt')) for i in range(3)])
    dirt_pl  = ([lgb.Booster(model_file=mp('ml_model_place_dirt.txt'))] +
                [lgb.Booster(model_file=mp(f'ml_model_place_dirt_e{i}.txt')) for i in range(3)])
    return turf_win, turf_pl, dirt_win, dirt_pl


def load_sanrenpuku(data_dir, start_ym, end_ym):
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
                val = row.get('三連複払戻', '').strip()
                if val and val != '-':
                    mm = re.search(r'(\d+)\s*-\s*(\d+)\s*-\s*(\d+):(\d+)', val)
                    if mm:
                        key = frozenset([mm.group(1), mm.group(2), mm.group(3)])
                        db[rid][key] = int(mm.group(4))
    return db


def run_period(races, start_ym, end_ym, model_dir, threshold, dynamic_aite=True, verbose=False):
    """1期間のバックテスト実行"""
    turf_win, turf_pl, dirt_win, dirt_pl = load_models(model_dir)

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=start_ym)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)

    puku_db   = load_sanrenpuku('data', start_ym, end_ym)
    test_rids = sorted(rid for rid, info in races.items()
                       if start_ym <= info['file_ym'] <= end_ym)

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
        _mw = dirt_win if is_dirt else turf_win
        _mp = dirt_pl  if is_dirt else turf_pl

        sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats, _mw, _mp)
        if not sc or len(sc) < 4:
            continue

        wp_map   = {h['name']: h['win_prob']   for h in sc}
        pp_map   = {h['name']: h['place_prob'] for h in sc}
        odds_map = {h['name']: h['odds']       for h in sc}
        uma_map  = {h['name']: h['umaban']     for h in sc}

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
        sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

        hn = sorted_wp[0]['name']

        # ML win_prob 下限
        if wp_map.get(hn, 0) < HONMEI_WP_MIN:
            continue

        # エッジフィルター: MLが市場より高評価の場合のみ
        honmei_odds = odds_map.get(hn, 0)
        if honmei_odds <= 0:
            continue
        edge = (wp_map[hn] / 100.0) * honmei_odds
        if edge < threshold:
            continue

        # ○: place_prob #2 (◎以外)
        rn = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
        if not rn:
            continue

        # ながし相手: place_prob 上位 (◎○以外)
        aite_cands = [h['name'] for h in sorted_pp if h['name'] not in (hn, rn)]
        cur_n = (3 if wp_map.get(hn, 0) >= 25 else 4) if dynamic_aite else N_AITE
        aite = aite_cands[:cur_n]

        u_h = uma_map.get(hn, '')
        u_r = uma_map.get(rn, '')
        u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
        if not u_h or not u_r or not u_a:
            continue

        tix  = [frozenset([u_h, u_r, a]) for a in u_a]
        puku = puku_db.get(race_id, {})
        cost = len(tix) * BET
        ret  = sum(puku.get(t, 0) * BET // 100 for t in tix)
        hit  = int(ret > 0)

        ym = info['file_ym']
        monthly[ym]['cost']  += cost
        monthly[ym]['ret']   += ret
        monthly[ym]['races'] += 1
        monthly[ym]['hits']  += hit

        if verbose and hit:
            pay = max(puku.get(t, 0) for t in tix if puku.get(t, 0) > 0)
            print(f'  ✓ {race_id} {info["race_name"]}  '
                  f'◎{hn}/wp{wp_map[hn]:.0f}%/odds{honmei_odds:.1f}/edge{edge:.2f}  '
                  f'→ {ret:,}円 (配当{pay:,}円)')

    return monthly


def print_monthly(monthly, label):
    print(f'\n=== {label} ===')
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
    print(f'{"合計":>8}  {n:>4}  {h:>4}  {h/n*100 if n else 0:>5.1f}%  {c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}%')
    return total


def run(threshold=1.2, scan=False, verbose=False):
    print('全データ読み込み中...', flush=True)
    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R\n')

    thresholds = [0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0] if scan else [threshold]

    for thr in thresholds:
        print(f'\n{"="*60}')
        print(f'エッジ閾値: {thr}  (edge = ML_wp/100 × 単勝オッズ ≥ {thr})')
        print(f'{"="*60}')

        all_monthly = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0})

        for start_ym, end_ym, model_dir in WF_PERIODS:
            if not os.path.exists(os.path.join(model_dir, 'ml_model_win_turf.txt')):
                print(f'  [SKIP] モデルなし: {model_dir}')
                continue
            print(f'\n  期間 {start_ym}-{end_ym} / モデル: {model_dir}')
            monthly = run_period(races, start_ym, end_ym, model_dir, thr, verbose=verbose)
            for ym, s in monthly.items():
                for k in ['cost', 'ret', 'races', 'hits']:
                    all_monthly[ym][k] += s[k]

        total = print_monthly(all_monthly, f'全期間合計 (edge≥{thr})')
        n = total['races']; c = total['cost']; r = total['ret']
        roi = r / c * 100 if c else 0
        black = sum(1 for s in all_monthly.values() if s['ret'] > s['cost'])
        print(f'黒字月: {black}/{len(all_monthly)}ヶ月  '
              f'月平均投資: {c//len(all_monthly) if all_monthly else 0:,}円  '
              f'総ROI: {roi:.1f}%')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold', type=float, default=1.2)
    parser.add_argument('--scan',    action='store_true', help='複数閾値をスキャン')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    run(args.threshold, args.scan, args.verbose)

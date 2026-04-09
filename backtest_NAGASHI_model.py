# -*- coding: utf-8 -*-
"""
backtest_NAGASHI_model.py  NAGASHI-モデル独自選択バックテスト
=============================================================
戦略: market_alphaは混合win_prob計算に使うが、
     ◎○▲の割り当てはmodel_probのみで行う
     → 市場人気に引きずられない独自選択

フィルター条件 (通常NAGASHIと同じ):
  - 8R〜11R
  - 出走頭数 <= 14頭
  - ◎ model_prob >= 15%
  - ◎ odds <= 6.0倍 (market_alphaベースでない条件)
  - ◎ + ○ model_prob合計 >= 30%

買い目:
  - 三連単C1: ◎○二軸 × ▲☆△ながし

バックテスト期間: 202501〜202603

実行:
  python backtest_NAGASHI_model.py
  python backtest_NAGASHI_model.py --start 202501 --end 202603 --verbose
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    analyze_race_uscore, build_trainer_stats, build_jockey_stats,
    should_exclude_uscore,
)

# ── パラメータ ────────────────────────────────────
RNUM_MIN          = 8
RNUM_MAX          = 11
MAX_FIELD         = 14
HONMEI_MP_MIN     = 15.0   # ◎ model_prob 下限
HONMEI_ODDS_MAX   = 6.0    # ◎ オッズ上限
MP_SUM_MIN        = 30.0   # ◎+○ model_prob合計下限
DISAGREE_MIN      = 0.0    # ◎ (model_prob - market_prob) 最小乖離 (0=制限なし)
MARKET_ALPHA      = 0.4
BET               = 100
N_AITE            = 4      # ▲☆△合計の対象馬数 (win_prob 3〜N+2位)


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


def run(test_start='202501', test_end='202603', verbose=False,
        disagree_min=DISAGREE_MIN):
    print(f'=== NAGASHI-モデル独自選択 バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'◎○▲の割り当て: model_prob順 (市場人気に依存しない)')
    print(f'条件: {RNUM_MIN}-{RNUM_MAX}R / 頭数≤{MAX_FIELD} / '
          f'◎mp≥{HONMEI_MP_MIN}% / ◎odds≤{HONMEI_ODDS_MAX} / '
          f'◎+○mp≥{MP_SUM_MIN}%', end='')
    if disagree_min > 0:
        print(f' / 乖離≥{disagree_min:+.0f}%')
    else:
        print()
    print()

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

    monthly   = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0})
    pop_dist  = defaultdict(int)   # ◎の市場人気分布
    disagree_list = []             # 乖離度ログ

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

        # ◎○▲をmodel_probで決める（市場人気に依存しない）
        sorted_mp  = sorted(sc, key=lambda h: h['model_prob'], reverse=True)
        # ながし相手はwin_prob順（3位以下）
        sorted_wp  = sorted(sc, key=lambda h: h['win_prob'], reverse=True)

        mp_map   = {h['name']: h['model_prob']  for h in sc}
        mkt_map  = {h['name']: h['market_prob'] for h in sc}
        odds_map = {h['name']: h['odds']        for h in info['horse_list']}
        uma_map  = {h['name']: h['umaban']      for h in info['horse_list']}
        pop_map  = {h['name']: h['pop']         for h in info['horse_list']}

        hn = sorted_mp[0]['name']  # ◎ = model_prob 1位
        rn = sorted_mp[1]['name']  # ○ = model_prob 2位

        # フィルター
        if mp_map.get(hn, 0) < HONMEI_MP_MIN:
            continue
        if odds_map.get(hn, 99) > HONMEI_ODDS_MAX:
            continue
        if mp_map.get(hn, 0) + mp_map.get(rn, 0) < MP_SUM_MIN:
            continue

        # 乖離フィルター
        disagree = mp_map.get(hn, 0) - mkt_map.get(hn, 0)
        if disagree < disagree_min:
            continue

        # ながし相手: win_prob 3位以下からN_AITE頭
        aite_names = [h['name'] for h in sorted_wp if h['name'] not in (hn, rn)][:N_AITE]
        if not aite_names:
            continue

        u_h = uma_map.get(hn, '')
        u_r = uma_map.get(rn, '')
        u_a = [uma_map.get(n, '') for n in aite_names if uma_map.get(n, '')]
        if not u_h or not u_r or not u_a:
            continue

        # 三連単C1: ◎○二軸 × 相手
        axis = [u_h, u_r]
        tix  = set()
        for perm in permutations(axis):
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

        # ◎の市場人気を記録
        h_pop = pop_map.get(hn, 99)
        pop_label = str(h_pop) if h_pop <= 5 else '6+'
        pop_dist[pop_label] += 1
        disagree_list.append(disagree)

        if verbose and hit:
            won = [t for t in tix if t in san]
            print(f'  ✓ {race_id} {info["race_name"]}  '
                  f'◎{hn}/mp{mp_map[hn]:.1f}%/pop{pop_map[hn]}  '
                  f'○{rn}/mp{mp_map[rn]:.1f}%/pop{pop_map.get(rn,99)}  '
                  f'乖離{disagree:+.1f}%  → {ret:,}円')

    # ◎人気分布
    total_races = sum(monthly[ym]['races'] for ym in monthly)
    print(f'◎の市場人気分布 (対象{total_races}R):')
    for k in ['1', '2', '3', '4', '5', '6+']:
        cnt = pop_dist.get(k, 0)
        pct = cnt / total_races * 100 if total_races else 0
        bar = '█' * int(pct / 2)
        print(f'  {k}番人気: {cnt:3d}R ({pct:5.1f}%) {bar}')
    if disagree_list:
        avg_d = sum(disagree_list) / len(disagree_list)
        print(f'  ◎平均乖離(model-market): {avg_d:+.1f}%\n')

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
        print(f'月平均投資: {c//len(monthly):,}円  月平均収支: {(r-c)//len(monthly):+,}円')
        print(f'月平均R数: {n/len(monthly):.1f}R  月平均通数: {c//len(monthly)//BET:.1f}通')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',   default='202501')
    parser.add_argument('--end',     default='202603')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--disagree', type=float, default=0.0,
                        help='min disagreement model_prob - market_prob in pct')
    args = parser.parse_args()
    run(args.start, args.end, args.verbose, args.disagree)

# -*- coding: utf-8 -*-
"""
simulate_cap_sweep.py  掛け金上限スイープ
上限を100円刻みで上げてバランス最適点を探す
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats, should_exclude_uscore

RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
MARKET_ALPHA    = 0.4

NAGASHI_WP_MIN    = 20.0
NAGASHI_ODDS_MAX  = 4.0
NAGASHI_WPSUM_MIN = 45.0
NAGASHI_N_AITE    = 4

KEN_WP_MIN      = 20.0
KEN_ODDS_MAX    = 4.0
KEN_SANKAKU_MIN = 10.0

MONTHLY_ADD     = 20000
PROFIT_RATE     = 0.70
SANRENTAN_RATIO = 0.01
INIT_BANKROLL   = 70000


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


print('データ読み込み中...')
races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202501')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)

sanrentan_db = load_sanrentan('data', '202501', '202512')
test_rids = sorted(rid for rid, info in races.items()
                   if '202501' <= info['file_ym'] <= '202512')

# 月別の枚数・100円あたり配当を事前計算
monthly_nagashi = defaultdict(lambda: {'cost': 0, 'ret': 0})
monthly_ken     = defaultdict(lambda: {'cost': 0, 'ret': 0})

print('バックテスト実行中...')
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
    except:
        continue
    if not sc: continue

    wp_map   = {h['name']: h['win_prob'] for h in sc}
    odds_map = {h['name']: h['odds']     for h in info['horse_list']}
    uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
    sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
    ym = info['file_ym']
    san = sanrentan_db.get(race_id, {})

    if len(sorted_wp) >= 2:
        hn = sorted_wp[0]['name']
        rn = sorted_wp[1]['name']
        if (wp_map.get(hn, 0) >= NAGASHI_WP_MIN and
                odds_map.get(hn, 99) <= NAGASHI_ODDS_MAX and
                wp_map.get(hn, 0) + wp_map.get(rn, 0) >= NAGASHI_WPSUM_MIN):
            aite = [h['name'] for h in sorted_wp[2:2+NAGASHI_N_AITE]]
            u_h = uma_map.get(hn, ''); u_r = uma_map.get(rn, '')
            u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
            if u_h and u_r and u_a:
                tix = set()
                for perm in permutations([u_h, u_r]):
                    for a in u_a:
                        tix.add((*perm, a))
                monthly_nagashi[ym]['cost'] += len(tix)
                monthly_nagashi[ym]['ret']  += sum(san.get(t, 0) for t in tix)

    if len(sorted_wp) >= 3:
        hn2 = sorted_wp[0]['name']
        rn2 = sorted_wp[1]['name']
        an2 = sorted_wp[2]['name']
        if (wp_map.get(hn2, 0) >= KEN_WP_MIN and
                odds_map.get(hn2, 99) <= KEN_ODDS_MAX and
                odds_map.get(an2, 0) >= KEN_SANKAKU_MIN):
            u_h2 = uma_map.get(hn2, '')
            u_r2 = uma_map.get(rn2, '')
            u_a2 = uma_map.get(an2, '')
            if u_h2 and u_r2 and u_a2:
                tix2 = {(u_h2, u_r2, u_a2), (u_h2, u_a2, u_r2)}
                monthly_ken[ym]['cost'] += len(tix2)
                monthly_ken[ym]['ret']  += sum(san.get(t, 0) for t in tix2)

months = sorted(set(list(monthly_nagashi.keys()) + list(monthly_ken.keys())))


def simulate(cap):
    bankroll = INIT_BANKROLL
    total_invest = total_ret = 0
    max_dd = 0  # 最大ドローダウン（円）
    peak = INIT_BANKROLL

    for ym in months:
        bankroll += MONTHLY_ADD
        bpt = min(max(int(bankroll * SANRENTAN_RATIO // 100) * 100, 100), cap)

        invest = (monthly_nagashi[ym]['cost'] + monthly_ken[ym]['cost']) * bpt
        ret    = (monthly_nagashi[ym]['ret'] * bpt // 100 +
                  monthly_ken[ym]['ret'] * bpt // 100)
        profit = ret - invest
        bankroll += profit

        if profit > 0:
            reinvest = int(profit * PROFIT_RATE // 100) * 100
            bankroll += reinvest

        if bankroll > peak:
            peak = bankroll
        dd = peak - bankroll
        if dd > max_dd:
            max_dd = dd

        total_invest += invest
        total_ret    += ret

    roi = total_ret / total_invest * 100 if total_invest else 0
    net = bankroll - INIT_BANKROLL
    return bankroll, net, roi, max_dd


print()
print(f'=== 掛け金上限スイープ (初期軍資金{INIT_BANKROLL:,}円 / 月補充{MONTHLY_ADD:,}円) ===')
print()
print(f'{"上限/枚":>7}  {"最終残高":>10}  {"純増額":>10}  {"増加率":>7}  {"ROI":>7}  {"最大DD":>10}  {"DD率":>6}  {"評価"}')
print('-' * 85)

caps = list(range(100, 1100, 100)) + [1500, 2000, 3000, 5000]
results = []
for cap in caps:
    final, net, roi, max_dd = simulate(cap)
    dd_rate = max_dd / (INIT_BANKROLL + len(months) * MONTHLY_ADD) * 100
    results.append((cap, final, net, roi, max_dd, dd_rate))

# バランス指標: ROI × log(最終残高) / (1 + DD率/10)
import math
scores = []
for cap, final, net, roi, max_dd, dd_rate in results:
    score = roi * math.log(final) / (1 + dd_rate / 10)
    scores.append(score)
best_score = max(scores)

for i, (cap, final, net, roi, max_dd, dd_rate) in enumerate(results):
    score = scores[i]
    mark = '★ BEST' if score == best_score else ('◎' if score > best_score * 0.95 else '')
    print(f'{cap:>7,}円  {final:>10,}円  {net:>+10,}円  {net/INIT_BANKROLL*100:>6.0f}%  '
          f'{roi:>6.1f}%  {max_dd:>10,}円  {dd_rate:>5.1f}%  {mark}')

print()
print('※ 最大DD = ピーク軍資金からの最大下落額')
print('※ DD率 = 最大DD / (初期+補充合計)')
print('※ BEST = ROI x log(最終残高) / (1+DD率/10) が最大')

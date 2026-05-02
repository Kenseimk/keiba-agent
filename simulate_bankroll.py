# -*- coding: utf-8 -*-
"""
simulate_bankroll.py  軍資金ルール適用シミュレーション
- 月初: 20,000円補充
- 月末: 利益の70%を軍資金に追加
- 三連単の掛け金上限: 軍資金 × 1% (100円単位, 最低100円)
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
BET_CAP         = 1000  # 1枚あたり上限


def bet_per_ticket(balance):
    raw = balance * SANRENTAN_RATIO
    unit = int(raw // 100) * 100
    return min(max(unit, 100), BET_CAP)


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
print(f'horse_db: {len(horse_db):,}頭')

sanrentan_db = load_sanrentan('data', '202501', '202512')
test_rids = sorted(rid for rid, info in races.items()
                   if '202501' <= info['file_ym'] <= '202512')

monthly_nagashi = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0})
monthly_ken     = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0})

print('バックテスト実行中...')
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
        sc = analyze_race_uscore(race_obj, horse_db, None, None,
                                 trainer_stats=trainer_stats,
                                 jockey_stats=jockey_stats,
                                 market_alpha=MARKET_ALPHA)
    except:
        continue
    if not sc:
        continue

    wp_map   = {h['name']: h['win_prob'] for h in sc}
    odds_map = {h['name']: h['odds']     for h in info['horse_list']}
    uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
    sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)
    ym = info['file_ym']
    san = sanrentan_db.get(race_id, {})

    # NAGASHI
    if len(sorted_wp) >= 2:
        hn = sorted_wp[0]['name']
        rn = sorted_wp[1]['name']
        if (wp_map.get(hn, 0) >= NAGASHI_WP_MIN and
                odds_map.get(hn, 99) <= NAGASHI_ODDS_MAX and
                wp_map.get(hn, 0) + wp_map.get(rn, 0) >= NAGASHI_WPSUM_MIN):
            aite = [h['name'] for h in sorted_wp[2:2+NAGASHI_N_AITE]]
            u_h = uma_map.get(hn, '')
            u_r = uma_map.get(rn, '')
            u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
            if u_h and u_r and u_a:
                tix = set()
                for perm in permutations([u_h, u_r]):
                    for a in u_a:
                        tix.add((*perm, a))
                ret_per100 = sum(san.get(t, 0) for t in tix)
                monthly_nagashi[ym]['races'] += 1
                monthly_nagashi[ym]['cost']  += len(tix)
                monthly_nagashi[ym]['ret']   += ret_per100

    # NAGASHI-堅
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
                ret_per100 = sum(san.get(t, 0) for t in tix2)
                monthly_ken[ym]['races'] += 1
                monthly_ken[ym]['cost']  += len(tix2)
                monthly_ken[ym]['ret']   += ret_per100

print()
print('=== 軍資金ルール適用 シミュレーション ===')
print(f'初期軍資金: 70,000円  月初補充: {MONTHLY_ADD:,}円  利益{int(PROFIT_RATE*100)}%再投資')
print(f'三連単掛け金上限: 軍資金 x {int(SANRENTAN_RATIO*100)}% / 枚 (100円単位、上限{BET_CAP}円/枚)')
print()
print(f'{"月":>8}  {"月初残高":>9}  {"補充":>7}  {"掛/枚":>6}  '
      f'{"投資":>8}  {"回収":>8}  {"収支":>9}  {"再投資":>7}  {"月末残高":>9}')
print('-' * 100)

bankroll = 70000
months = sorted(set(list(monthly_nagashi.keys()) + list(monthly_ken.keys())))

total_invest = total_ret = 0
total_reinvest = 0
for ym in months:
    balance_start = bankroll
    bankroll += MONTHLY_ADD

    bpt = bet_per_ticket(bankroll)

    n_invest = monthly_nagashi[ym]['cost'] * bpt
    n_ret    = monthly_nagashi[ym]['ret'] * bpt // 100

    k_invest = monthly_ken[ym]['cost'] * bpt
    k_ret    = monthly_ken[ym]['ret'] * bpt // 100

    invest = n_invest + k_invest
    ret    = n_ret + k_ret
    profit = ret - invest
    bankroll += profit

    reinvest = 0
    if profit > 0:
        reinvest = int(profit * PROFIT_RATE // 100) * 100
        bankroll += reinvest

    total_invest   += invest
    total_ret      += ret
    total_reinvest += reinvest

    mark = '+' if profit >= 0 else ' '
    print(f'{ym:>8}  {balance_start:>9,}  +{MONTHLY_ADD:>6,}  {bpt:>6,}  '
          f'{invest:>8,}  {ret:>8,}  {profit:>+9,}  +{reinvest:>6,}  {bankroll:>9,}')

print('-' * 100)
total_added = len(months) * MONTHLY_ADD + total_reinvest
roi = total_ret / total_invest * 100 if total_invest else 0
print()
print(f'最終軍資金   : {bankroll:,}円')
print(f'初期軍資金   : 70,000円')
print(f'純増額       : +{bankroll - 70000:,}円 ({(bankroll/70000-1)*100:.1f}%)')
print(f'月初補充合計 : {len(months)*MONTHLY_ADD:,}円')
print(f'利益再投資   : {total_reinvest:,}円')
print(f'総投資       : {total_invest:,}円')
print(f'総回収       : {total_ret:,}円')
print(f'ROI          : {roi:.1f}%')

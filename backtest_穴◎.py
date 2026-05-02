# -*- coding: utf-8 -*-
"""
穴◎戦略バックテスト
条件: market_alpha=0.2, ◎が3番人気以下のレースのみ
券種: 単勝 / 馬連◎○ / ワイド◎○ / ワイド3通 / 三連複◎○▲ / 三連単C1
"""
import os, re, csv, glob
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations, combinations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    analyze_race_uscore, assign_marks,
    build_trainer_stats, build_jockey_stats, should_exclude_uscore,
)

# ── horse_db (テスト開始前) ───────────────────
print('CSV読み込み中...', flush=True)
races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202501')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)
print(f'horse_db: {len(horse_db):,}頭')

# ── 払戻DB構築 ─────────────────────────────────
sanrentan_db  = {}
sanrenpuku_db = {}
wide_db       = {}
umaren_db     = {}

for fpath in sorted(glob.glob('data/raceresults_*.csv')):
    m = re.search(r'(\d{6})\.csv', fpath)
    ym = m.group(1) if m else ''
    if ym < '202501' or ym > '202603':
        continue
    with open(fpath, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            rid = row['race_id']
            if rid not in sanrentan_db:
                sanrentan_db[rid]  = {}
                sanrenpuku_db[rid] = {}
                wide_db[rid]       = {}
                umaren_db[rid]     = {}

            san = row.get('三連単払戻','').strip()
            if san and san != '-':
                mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', san)
                if mm:
                    sanrentan_db[rid][(mm.group(1),mm.group(2),mm.group(3))] = int(mm.group(4))

            sp = row.get('三連複払戻','').strip()
            if sp and sp != '-':
                mm = re.search(r'(\d+)\s*-\s*(\d+)\s*-\s*(\d+):(\d+)', sp)
                if mm:
                    sanrenpuku_db[rid][frozenset([mm.group(1),mm.group(2),mm.group(3)])] = int(mm.group(4))

            wd = row.get('ワイド払戻','').strip()
            if wd and wd != '-':
                for part in wd.split('|'):
                    mm = re.search(r'(\d+)\s*-\s*(\d+):(\d+)', part)
                    if mm:
                        wide_db[rid][frozenset([mm.group(1),mm.group(2)])] = int(mm.group(3))

            ur = row.get('馬連払戻','').strip()
            if ur and ur != '-':
                mm = re.search(r'(\d+)\s*-\s*(\d+):(\d+)', ur)
                if mm:
                    umaren_db[rid][frozenset([mm.group(1),mm.group(2)])] = int(mm.group(3))

# ── テスト対象 ────────────────────────────────
TEST_START, TEST_END = '202501', '202603'
ALPHA = 0.2
BET   = 100

bet_types = ['単勝', '馬連◎○', 'ワイド◎○', 'ワイド3通', '三連複◎○▲', '三連単C1']
stats = {k: {'cost':0,'ret':0,'hits':0,'races':0} for k in bet_types}

test_rids = sorted(rid for rid, info in races.items()
                   if TEST_START <= info['file_ym'] <= TEST_END)

skipped = passed = 0

for race_id in test_rids:
    info = races[race_id]
    if should_exclude_uscore(info['race_name']): continue
    if all(h['odds']==0.0 for h in info['horse_list']): continue

    race_obj = make_race_info(info)
    try:
        scored = analyze_race_uscore(race_obj, horse_db, None, None,
                                     trainer_stats=trainer_stats,
                                     jockey_stats=jockey_stats,
                                     market_alpha=ALPHA)
    except:
        continue
    if not scored:
        continue

    marks = assign_marks(scored)
    honmei_n  = next((n for n,m in marks.items() if m=='◎'), None)
    retan_n   = next((n for n,m in marks.items() if m=='○'), None)
    sankaku_n = next((n for n,m in marks.items() if m=='▲'), None)
    hosi_n    = next((n for n,m in marks.items() if m=='☆'), None)
    delta_ns  = [n for n,m in marks.items() if m=='△']

    if not honmei_n: continue

    pop_map  = {h['name']: h['pop']  for h in info['horse_list']}
    rank_map = {h['name']: h['rank'] for h in info['horse_list']}
    odds_map = {h['name']: h['odds'] for h in info['horse_list']}
    uma_map  = {h['name']: h['umaban'] for h in info['horse_list']}

    # ── フィルター: ◎が3番人気以下 ─────────────
    if pop_map.get(honmei_n, 99) <= 2:
        skipped += 1
        continue
    passed += 1

    u_h = uma_map.get(honmei_n, '')
    u_r = uma_map.get(retan_n,  '') if retan_n  else ''
    u_s = uma_map.get(sankaku_n,'') if sankaku_n else ''
    u_ho = uma_map.get(hosi_n,  '') if hosi_n   else ''
    u_d  = [uma_map.get(n,'') for n in delta_ns if uma_map.get(n,'')]

    h_rank = rank_map.get(honmei_n, 99)
    h_odds = odds_map.get(honmei_n, 0)

    def add(bt, cost, ret):
        stats[bt]['cost']  += cost
        stats[bt]['ret']   += ret
        stats[bt]['races'] += 1
        stats[bt]['hits']  += int(ret > 0)

    # 単勝
    add('単勝', BET, int(h_odds*BET) if h_rank==1 else 0)

    # 馬連◎○
    if u_h and u_r:
        key = frozenset([u_h, u_r])
        pay = umaren_db.get(race_id,{}).get(key,0)
        add('馬連◎○', BET, pay*BET//100)

    # ワイド◎○
    if u_h and u_r:
        key = frozenset([u_h, u_r])
        pay = wide_db.get(race_id,{}).get(key,0)
        add('ワイド◎○', BET, pay*BET//100)

    # ワイド3通 ◎○ ◎▲ ○▲
    if u_h and u_r and u_s:
        pairs = [frozenset([u_h,u_r]), frozenset([u_h,u_s]), frozenset([u_r,u_s])]
        wd = wide_db.get(race_id,{})
        ret = sum(wd.get(p,0)*BET//100 for p in pairs)
        stats['ワイド3通']['cost']  += BET*3
        stats['ワイド3通']['ret']   += ret
        stats['ワイド3通']['races'] += 1
        stats['ワイド3通']['hits']  += int(ret > 0)

    # 三連複◎○▲
    if u_h and u_r and u_s:
        key = frozenset([u_h,u_r,u_s])
        pay = sanrenpuku_db.get(race_id,{}).get(key,0)
        add('三連複◎○▲', BET, pay*BET//100)

    # 三連単C1: ◎○二軸×▲☆△
    aite = [u for u in [u_s, u_ho] + u_d if u]
    axis = [u for u in [u_h, u_r] if u]
    tix  = set()
    if len(axis)==2 and aite:
        for perm in permutations(axis):
            for a in aite:
                if a not in perm:
                    tix.add((*perm, a))
    if tix:
        san = sanrentan_db.get(race_id,{})
        cost = len(tix)*BET
        ret  = sum(san.get(t,0)*BET//100 for t in tix)
        stats['三連単C1']['cost']  += cost
        stats['三連単C1']['ret']   += ret
        stats['三連単C1']['races'] += 1
        stats['三連単C1']['hits']  += int(ret > 0)

# ── 結果表示 ──────────────────────────────────
print(f'\nalpha={ALPHA}  ◎が3番人気以下のレースのみ')
print(f'対象: {passed}R  (除外: {skipped}R)')
print()
print(f'{"券種":<12} {"R数":>5} {"的中":>5} {"的中率":>7} {"投資":>9} {"回収":>9} {"収支":>10} {"ROI":>7}')
print('-' * 70)
for bt in bet_types:
    s = stats[bt]
    n = s['races']
    h = s['hits']
    c = s['cost']
    r = s['ret']
    if n == 0: continue
    roi = r/c*100 if c else 0
    print(f'{bt:<12} {n:>5} {h:>5} {h/n*100:>6.1f}%  {c:>9,} {r:>9,} {r-c:>+10,} {roi:>6.1f}%')

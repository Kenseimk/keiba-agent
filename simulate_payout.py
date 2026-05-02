# -*- coding: utf-8 -*-
"""
先週（3/28〜3/29）三連単マルチ流し 収支シミュレーション
1口100円、パターンC（C1 + C2）で買い目を購入
"""
import sys, io, json, csv, re, os
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

from collections import defaultdict
from uscore import (
    analyze_race_uscore, load_uscore_db,
    build_trainer_stats, build_jockey_stats,
    assign_marks,
)
from uscore_backtest import _add_races_to_horse_db, load_all_csv_races

TARGET_DATES = ['20260328', '20260329']
RESULT_CSV   = 'data/raceresults_202603.csv'
BET_YEN      = 100   # 1口

# ── horse_db 構築 ────────────────────────────
print('horse_db 構築中...', flush=True)
races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202603')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)
print(f'horse_db: {len(horse_db):,}頭\n')

# ── CSV から実結果・三連単払戻・馬番を取得 ─────
results_db  = {}   # race_id → {馬名: {'rank':, 'umaban':}}
sanrentan_db = {}  # race_id → {(1着番, 2着番, 3着番): 払戻額}
wide_db      = {}  # race_id → {frozenset(馬番a, 馬番b): 払戻額}

with open(RESULT_CSV, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        rid  = row['race_id']
        name = row['馬名']
        rank = int(row['着順']) if row['着順'].isdigit() else 99
        uma  = row.get('馬番', '').strip()

        if rid not in results_db:
            results_db[rid]   = {}
            sanrentan_db[rid] = {}
            wide_db[rid]      = {}

        results_db[rid][name] = {'rank': rank, 'umaban': uma}

        # 三連単払戻: '1 → 3 → 5:48200'
        san = row.get('三連単払戻', '').strip()
        if san and san != '-':
            m = re.match(r'(\d+)\s*[→＞>]\s*(\d+)\s*[→＞>]\s*(\d+):(\d+)', san)
            if m:
                key = (m.group(1), m.group(2), m.group(3))
                sanrentan_db[rid][key] = int(m.group(4))

        # ワイド払戻: '1-3:1200 / 1-5:800 / 3-5:600' など
        wide = row.get('ワイド払戻', '').strip()
        if wide and wide != '-':
            for part in wide.split('/'):
                wm = re.search(r'(\d+)-(\d+):(\d+)', part)
                if wm:
                    key = frozenset([wm.group(1), wm.group(2)])
                    wide_db[rid][key] = int(wm.group(3))

# ── 馬名 → 馬番 変換ヘルパー ─────────────────
def name_to_umaban(rid, name):
    return results_db.get(rid, {}).get(name, {}).get('umaban', '')

# ── 買い目生成 (パターンC) ──────────────────
def make_tickets_c(marks, rid):
    """
    C1: ◎○ が軸、▲☆△が相手 → マルチ
    C2: ◎▲ が軸、☆△が相手 → マルチ
    戻り値: [(u1, u2, u3), ...] 馬番タプルのリスト（マルチ=全順列）
    """
    from itertools import permutations

    def to_uma(name): return name_to_umaban(rid, name)

    honmei  = [n for n,m in marks.items() if m == '◎']
    rentan  = [n for n,m in marks.items() if m in ('◎','○')]
    sankaku = [n for n,m in marks.items() if m == '▲']
    sante   = [n for n,m in marks.items() if m in ('▲','☆','△')]
    hosi    = [n for n,m in marks.items() if m in ('☆','△')]

    tickets = set()

    # C1: [◎○] × [▲☆△]
    if len(rentan) == 2 and sante:
        axis_uma = [to_uma(n) for n in rentan if to_uma(n)]
        aite_uma = [to_uma(n) for n in sante  if to_uma(n)]
        if len(axis_uma) == 2:
            for perm in permutations(axis_uma):   # 2通り
                for aite in aite_uma:
                    if aite not in perm:
                        tickets.add((*perm, aite))

    # C2: [◎▲] × [☆△]
    c2_axis = honmei + sankaku
    if len(c2_axis) == 2 and hosi:
        axis_uma = [to_uma(n) for n in c2_axis if to_uma(n)]
        aite_uma = [to_uma(n) for n in hosi    if to_uma(n)]
        if len(axis_uma) == 2:
            for perm in permutations(axis_uma):
                for aite in aite_uma:
                    if aite not in perm:
                        tickets.add((*perm, aite))

    return list(tickets)

def make_tickets_wide(marks, rid):
    """ワイド ◎→○▲"""
    honmei = [n for n,m in marks.items() if m == '◎']
    ob     = [n for n,m in marks.items() if m in ('○','▲')]
    def to_uma(n): return name_to_umaban(rid, n)
    tickets = set()
    for h in honmei:
        hu = to_uma(h)
        if not hu: continue
        for o in ob:
            ou = to_uma(o)
            if ou and ou != hu:
                tickets.add(frozenset([hu, ou]))
    return list(tickets)

# ── メインループ ─────────────────────────────
total_cost   = 0
total_return = 0
race_log     = []

for date in TARGET_DATES:
    fname = f'data/races_{date}.json'
    with open(fname, encoding='utf-8') as f:
        data = json.load(f)
    race_list = data.get('all_races', []) if isinstance(data, dict) else data

    for race in race_list:
        rid = race.get('race_id','')
        if not rid or rid not in results_db:
            continue

        try:
            scored = analyze_race_uscore(
                race, horse_db, None, None,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
            )
        except:
            continue
        if not scored:
            continue

        marks   = assign_marks(scored)
        tickets = make_tickets_c(marks, rid)
        if not tickets:
            continue

        cost    = len(tickets) * BET_YEN
        payout  = 0
        hit_tickets = []

        san_pay = sanrentan_db.get(rid, {})
        for t in tickets:
            if t in san_pay:
                p = san_pay[t] * BET_YEN // 100
                payout += p
                hit_tickets.append((t, p))

        total_cost   += cost
        total_return += payout

        rname = race.get('race_name','')
        log = {
            'date': date, 'rid': rid, 'race_name': rname,
            'tickets': len(tickets), 'cost': cost,
            'payout': payout, 'hit': hit_tickets,
        }
        race_log.append(log)

        mark_str = '  '.join(f'{m}{n[:5]}' for n,m in marks.items() if m in ('◎','○','▲'))
        hit_str  = f'  ★ {payout:,}円 ({", ".join(str(h[0]) for h in hit_tickets)})' if hit_tickets else ''
        print(f'{date} {rid[-4:]} {rname[:12]:12}  {len(tickets):2}通 {cost:,}円投資{hit_str}')
        print(f'       {mark_str}')

# ── 集計 ─────────────────────────────────────
print()
print('=' * 60)
print(f'【収支シミュレーション結果】パターンC 三連単マルチ 1口{BET_YEN}円')
print('=' * 60)
n_races = len(race_log)
n_hit   = sum(1 for r in race_log if r['hit'])
print(f'対象レース  : {n_races}R')
print(f'的中レース  : {n_hit}R  ({n_hit/n_races*100:.1f}%)')
print(f'総投資額    : {total_cost:,}円')
print(f'総回収額    : {total_return:,}円')
print(f'収支        : {total_return - total_cost:+,}円')
print(f'回収率      : {total_return / total_cost * 100:.1f}%' if total_cost else '')
print()

# 的中レースの詳細
print('--- 的中レース ---')
for r in race_log:
    if r['hit']:
        for combo, pay in r['hit']:
            print(f"  {r['date']} {r['race_name']}: {combo} → {pay:,}円  (投資{r['cost']:,}円)")

# 平均買い目数
avg_tickets = sum(r['tickets'] for r in race_log) / n_races if n_races else 0
print(f'\n平均買い目数: {avg_tickets:.1f}通/レース')
print(f'1レース平均投資: {total_cost//n_races if n_races else 0:,}円')

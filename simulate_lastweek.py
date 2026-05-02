# -*- coding: utf-8 -*-
"""
先週（3/28〜3/29）レース シミュレーション
予測: races_20260328/29.json + uscore
結果: raceresults_202603.csv と照合
"""
import sys, io, json, csv, re, os
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

from collections import defaultdict
from uscore import (
    analyze_race_uscore, load_uscore_db,
    build_trainer_stats, build_jockey_stats,
    assign_marks, format_sanrentan_tickets,
    print_uscore_result,
)
from uscore_backtest import _add_races_to_horse_db, load_all_csv_races, _build_race_records
from fetch_oikiri import build_oikiri_db

# ── 設定 ─────────────────────────────────
TARGET_DATES = ['20260328', '20260329']
RESULT_CSV   = 'data/raceresults_202603.csv'
N_DELTA      = 4

# ── horse_db 構築（202603 より前のデータ）────
print('horse_db 構築中...', flush=True)
races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202603')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)
print(f'horse_db: {len(horse_db):,}頭\n')

# ── 実際の着順を CSV から取得 ────────────────
results_db = {}   # race_id → {馬名: rank}
odds_db    = {}   # race_id → {馬名: 単勝払戻}
with open(RESULT_CSV, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        rid  = row['race_id']
        name = row['馬名']
        rank = int(row['着順']) if row['着順'].isdigit() else 99
        if rid not in results_db:
            results_db[rid] = {}
            odds_db[rid]    = {}
        results_db[rid][name] = rank
        # 単勝払戻（1着馬のもの）
        pay = row.get('単勝払戻', '')
        if pay and rank == 1:
            m = re.search(r'(\d+)', pay.replace(',', ''))
            if m:
                odds_db[rid][name] = int(m.group(1)) / 100.0

# ── 的中判定ヘルパー ─────────────────────────
def check_patterns(marks, act1, act2, act3):
    honmei = {n for n, m in marks.items() if m == '◎'}
    rentan = {n for n, m in marks.items() if m in ('◎','○')}
    sante  = {n for n, m in marks.items() if m in ('▲','☆','△')}
    hosi   = {n for n, m in marks.items() if m in ('☆','△')}
    sankaku= {n for n, m in marks.items() if m == '▲'}
    c2axis = honmei | sankaku

    top2   = {act1, act2}
    top3   = {act1, act2, act3}
    hit_a  = act1 in honmei
    hit_b  = bool(honmei & top2) and bool({n for n,m in marks.items() if m in ('○','▲')} & top2)
    hit_c1 = ({act1,act2} == rentan) and act3 in sante
    hit_c2 = ({act1,act2} == c2axis and len(c2axis)==2) and act3 in hosi
    hit_c  = hit_c1 or hit_c2
    hit_d3 = act1 in honmei and act2 in hosi and act3 in hosi
    hit_d  = hit_c or hit_d3
    return dict(A=hit_a, B=hit_b, C1=hit_c1, C2=hit_c2, C=hit_c, D=hit_d)

# ── メインループ ─────────────────────────────
totals = defaultdict(lambda: {'races':0, 'hits': defaultdict(int)})
all_races_sim = []

for date in TARGET_DATES:
    fname = f'data/races_{date}.json'
    with open(fname, encoding='utf-8') as f:
        data = json.load(f)
    race_list = data.get('all_races', []) if isinstance(data, dict) else data
    if not race_list:
        print(f'{date}: レースなし')
        continue

    print(f'\n{"#"*72}')
    print(f'# {date}  ({len(race_list)}レース)')
    print(f'{"#"*72}')

    for race in race_list:
        rid = race.get('race_id','')
        if not rid or rid not in results_db:
            continue

        # 予測
        try:
            scored = analyze_race_uscore(
                race, horse_db, None, None,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
            )
        except Exception as e:
            print(f'  [SKIP] {rid}: {e}')
            continue
        if not scored:
            continue

        race['rnum'] = rid[-2:].lstrip('0') or rid[-2:]
        marks = assign_marks(scored)

        # 実結果
        rank_map = results_db[rid]
        placed   = sorted([(n,r) for n,r in rank_map.items() if r in (1,2,3)], key=lambda x:x[1])
        if len(placed) < 3:
            continue
        act1, act2, act3 = placed[0][0], placed[1][0], placed[2][0]
        win_odds = next((o for n,o in odds_db.get(rid,{}).items()), 0.0)

        hits = check_patterns(marks, act1, act2, act3)

        # 表示
        print_uscore_result(race, scored)

        # 結果照合
        print(f'  【実際の結果】')
        print(f'  1着: {act1}  2着: {act2}  3着: {act3}')
        print(f'  印:  ◎={next((n for n,m in marks.items() if m=="◎"),"?")}  '
              f'○={next((n for n,m in marks.items() if m=="○"),"?")}  '
              f'▲={next((n for n,m in marks.items() if m=="▲"),"?")}')
        hit_strs = [p for p, h in hits.items() if h]
        if hit_strs:
            print(f'  ✅ 的中パターン: {" / ".join(hit_strs)}')
        else:
            print(f'  ❌ 不的中')

        totals[date]['races'] += 1
        for p, h in hits.items():
            if h:
                totals[date]['hits'][p] += 1

        all_races_sim.append({'date':date,'rid':rid,'marks':marks,
                               'actual':(act1,act2,act3),'hits':hits})

# ── 集計 ─────────────────────────────────────
print(f'\n{"="*72}')
print('集計')
print(f'{"="*72}')
patterns = ['A','B','C1','C2','C','D']
descs = {'A':'単勝◎','B':'ワイド◎→○▲','C1':'三連単◎○→▲△☆',
         'C2':'三連単◎▲→△☆','C':'パターンC','D':'パターンD'}

grand = {'races':0, 'hits': defaultdict(int)}
for date in TARGET_DATES:
    t = totals[date]
    n = t['races']
    grand['races'] += n
    print(f'\n{date}  ({n}R)')
    for p in patterns:
        h = t['hits'][p]
        grand['hits'][p] += h
        rate = h/n*100 if n else 0
        bar = '●'*h + '○'*(n-h) if n<=20 else ''
        print(f'  {descs[p]:15s}: {h:2d}/{n:2d} ({rate:5.1f}%)  {bar}')

n = grand['races']
print(f'\n合計  ({n}R)')
for p in patterns:
    h = grand['hits'][p]
    rate = h/n*100 if n else 0
    print(f'  {descs[p]:15s}: {h:2d}/{n:2d} ({rate:5.1f}%)')

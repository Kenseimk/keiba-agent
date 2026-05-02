import glob, csv, io, sys, re
from collections import defaultdict
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

EXCLUDE_CLASS = ['新馬', '未勝利', '1勝クラス', '500万下', '障害', 'ハードル', 'スティープル', 'JS']
JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

def should_exclude(name):
    return any(kw in name for kw in EXCLUDE_CLASS)

def parse_avg_pos(s):
    parts = [float(x) for x in str(s).split('-') if x.strip().isdigit()]
    return sum(parts)/len(parts) if parts else None

def parse_payout(s):
    result = {}
    if not s: return result
    for part in str(s).split('|'):
        if ':' not in part: continue
        key, pay = part.rsplit(':', 1)
        try: result[key.strip()] = int(pay.strip())
        except: pass
    return result

race_rows = defaultdict(list)
for fpath in sorted(glob.glob('data/raceresults_*.csv')):
    with open(fpath, 'rb') as f:
        raw = f.read()
    text = (raw[3:] if raw[:3] == b'\xef\xbb\xbf' else raw).decode('utf-8')
    for row in csv.DictReader(io.StringIO(text)):
        if row.get('場コード', '') not in JRA_VENUES: continue
        race_rows[row['race_id']].append(row)

sorted_races = sorted(race_rows.items())
jstats_df = pd.read_csv('data/jstats.csv', encoding='utf-8-sig', index_col='jockey')
jstats = {j: float(row['j_score']) for j, row in jstats_df.iterrows()}

def rank_to_s(r):
    t = {1: 10.0, 2: 8.0, 3: 6.0, 4: 5.0, 5: 4.0}
    return t.get(r, max(0.0, 3.0 - (r - 6) * 0.3))

def score_horse(name, jockey, course, history):
    decay = [0.40, 0.25, 0.17, 0.11, 0.07]
    if history:
        tw = sum(decay[:min(5, len(history))])
        ts = sum(decay[i] * rank_to_s(history[i]['rank']) for i in range(min(5, len(history))))
        recent = ts / tw
    else:
        recent = 5.0
    h0 = history[0] if history else {}
    ar, af = h0.get('agari_rank', -1), h0.get('agari_field', 1)
    agari = max(0.0, (af - ar + 1) / max(af, 1) * 10) if ar > 0 else 5.0
    jockey_s = jstats.get(jockey, 3.0)
    cr = [r for r in history if r.get('course') == course]
    cf = max(0.0, (10 - sum(r['rank'] for r in cr[:10]) / len(cr[:10])) / 9 * 10) if cr else 5.0
    pos = h0.get('avg_pos')
    if pos:
        rs = max(0, (10 - pos) / 9 * 10) if course == '芝' else (max(0, (8 - pos) / 7 * 10) if pos <= 8 else 2.0)
    else:
        rs = 5.0
    return (2.0 * recent + 1.0 * agari + 2.0 * jockey_s + 1.5 * cf + 1.0 * rs) / 7.5 * 10

horse_history = defaultdict(list)
BET_UNIT = 100
BET_COND = {'min_field': 14, 'min_gap': 2.0, 'odds_min': 3.0, 'odds_max': 10.0, 'min_dist': 1600}

def new_s():
    return {'races': 0, 'bet': 0, 'ret': 0, 'hit': 0}

stats = {
    'turf_all': new_s(), 'dirt_all': new_s(),
    'turf_cond': new_s(), 'dirt_cond': new_s(),
}

for race_id, rows in sorted_races:
    course = rows[0].get('コース', '')
    race_name = rows[0].get('race_name', '')

    valid_rows = []
    for r in rows:
        try:
            rank = int(r.get('着順', ''))
            if float(r.get('単勝オッズ', '0') or '0') > 0:
                valid_rows.append((rank, r))
        except:
            pass
    if len(valid_rows) < 5:
        continue

    agari_vals = sorted(
        [(r['馬名'], float(r.get('上がり3F', '')))
         for _, r in valid_rows if r.get('上がり3F', '').replace('.', '').isdigit()],
        key=lambda x: x[1]
    )
    agari_rank_map = {n: i + 1 for i, (n, _) in enumerate(agari_vals)}
    n_agari = len(agari_vals)

    is_eval = race_id[:4] >= '2023'

    if is_eval and course in ('芝', 'ダート') and not should_exclude(race_name):
        scored = []
        for rank, r in valid_rows:
            try:
                s = score_horse(r['馬名'], r.get('騎手', ''), course, horse_history[r['馬名']])
                scored.append((s, r['馬名'], r))
            except:
                pass
        scored.sort(reverse=True)
        if len(scored) < 3:
            continue

        pred_top3_umaban = [r.get('馬番', '') for _, name, r in scored[:3]]
        gap = scored[0][0] - scored[1][0]
        axis_odds = float(scored[0][2].get('単勝オッズ', '0') or '0')
        n_field = len(valid_rows)
        dist = int(rows[0].get('距離', 0) or 0)

        row1 = next((r for rank, r in valid_rows if rank == 1), None)
        if not row1:
            continue

        sanpuku = parse_payout(row1.get('三連複払戻', ''))
        if not all(u for u in pred_top3_umaban):
            continue
        key = ' - '.join(sorted(pred_top3_umaban, key=lambda x: int(x) if x.isdigit() else 99))

        sk_all = 'turf_all' if course == '芝' else 'dirt_all'
        stats[sk_all]['races'] += 1
        stats[sk_all]['bet'] += BET_UNIT
        if key in sanpuku:
            stats[sk_all]['ret'] += int(BET_UNIT * sanpuku[key] / 100)
            stats[sk_all]['hit'] += 1

        cond_ok = (
            n_field >= BET_COND['min_field'] and
            gap >= BET_COND['min_gap'] and
            BET_COND['odds_min'] <= axis_odds <= BET_COND['odds_max'] and
            dist >= BET_COND['min_dist']
        )
        if cond_ok:
            sk_cond = 'turf_cond' if course == '芝' else 'dirt_cond'
            stats[sk_cond]['races'] += 1
            stats[sk_cond]['bet'] += BET_UNIT
            if key in sanpuku:
                stats[sk_cond]['ret'] += int(BET_UNIT * sanpuku[key] / 100)
                stats[sk_cond]['hit'] += 1

    for rank, r in valid_rows:
        name = r['馬名']
        horse_history[name].insert(0, {
            'rank': rank, 'race_ym': race_id[:6],
            'agari_rank': agari_rank_map.get(name, -1),
            'agari_field': n_agari,
            'avg_pos': parse_avg_pos(r.get('通過順', '')),
            'course': r.get('コース', ''),
            'dist': int(r.get('距離', 0) or 0),
        })

print('=' * 70)
print('【三連複 回収率】芝+ダート 条件フィルタあり vs 全レース (2023〜2026)')
print('=' * 70)
print(f'{"":14} {"R数":>6} {"的中":>5} {"的中率":>7} {"投資":>9} {"払戻":>9} {"損益":>9} {"回収率":>7}')
print('-' * 70)

rows_data = [
    ('芝    全レース', 'turf_all'),
    ('芝    条件あり', 'turf_cond'),
    ('ダート 全レース', 'dirt_all'),
    ('ダート 条件あり', 'dirt_cond'),
]

combined_all = new_s()
combined_cond = new_s()

for label, sk in rows_data:
    s = stats[sk]
    roi = s['ret'] / s['bet'] * 100 if s['bet'] else 0
    pl = s['ret'] - s['bet']
    hr = s['hit'] / s['races'] * 100 if s['races'] else 0
    sign = '+' if pl >= 0 else '-'
    print(f'{label:14} {s["races"]:>6}R {s["hit"]:>5}回 {hr:>6.1f}% '
          f'¥{s["bet"]:>7,} ¥{s["ret"]:>7,} {sign}¥{abs(pl):>6,} {roi:>6.1f}%')
    if 'all' in sk:
        for k in combined_all: combined_all[k] += s[k]
    else:
        for k in combined_cond: combined_cond[k] += s[k]

print('-' * 70)
for label, s in [('合計 全レース', combined_all), ('合計 条件あり', combined_cond)]:
    roi = s['ret'] / s['bet'] * 100 if s['bet'] else 0
    pl = s['ret'] - s['bet']
    hr = s['hit'] / s['races'] * 100 if s['races'] else 0
    sign = '+' if pl >= 0 else '-'
    print(f'{label:14} {s["races"]:>6}R {s["hit"]:>5}回 {hr:>6.1f}% '
          f'¥{s["bet"]:>7,} ¥{s["ret"]:>7,} {sign}¥{abs(pl):>6,} {roi:>6.1f}%')

print('=' * 70)
print('条件: 頭数14頭以上 / ギャップ2.0pt以上 / 軸オッズ3〜10倍 / 距離1600m以上')

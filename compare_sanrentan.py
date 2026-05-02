"""三連単 vs 三連複 比較: 突出(max_p>=20%) + top4_cum>=60% レース"""
import sys, re, glob
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from itertools import permutations, combinations
from backtest_agent import load_all_races
from walkforward_winprob import build_db_upto, race_relative_features, predict_prob, load_model
from score_agent_core import load_models, compute_horse_factors

all_races = load_all_races('data')
jstats, dc_db = load_models('data')
w, b_bias, mean_v, std_v = load_model()
horse_db = build_db_upto(all_races, '202505')

frames = []
for f in sorted(glob.glob('data/raceresults_2026*.csv')):
    tmp = pd.read_csv(f, encoding='utf-8')
    frames.append(tmp)
df = pd.concat(frames, ignore_index=True)
df['race_id'] = df['race_id'].astype(str)
df = df.drop_duplicates(subset=['race_id', '馬名'])
target_ids = sorted(set(df[df['レース番号'].isin([8, 9, 10, 11])]['race_id']))


def parse_pair(raw):
    out = {}
    if not raw or str(raw) == 'nan': return out
    for part in str(raw).split('|'):
        m = re.match(r'(\d+)\s*[-\u2013]\s*(\d+):(\d+)', part.strip())
        if m: out[frozenset([m.group(1), m.group(2)])] = int(m.group(3))
    return out


def parse_san(raw):
    out = {}
    if not raw or str(raw) == 'nan': return out
    for part in str(raw).split('|'):
        m = re.match(r'(\d+)\s*[-\u2013]\s*(\d+)\s*[-\u2013]\s*(\d+):(\d+)', part.strip())
        if m: out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out


def parse_santan(raw):
    """三連単払戻: "11 → 3 → 13:126370" → {('11','3','13'):126370, ...}"""
    out = {}
    if not raw or str(raw) == 'nan': return out
    for part in str(raw).split('|'):
        # 区切り: → or - or –
        m = re.match(r'(\d+)\s*(?:→|[-\u2013])\s*(\d+)\s*(?:→|[-\u2013])\s*(\d+)\s*[:\uff1a]\s*(\d+)', part.strip())
        if m: out[(m.group(1), m.group(2), m.group(3))] = int(m.group(4))
    return out


# 構造定義
STRUCTURES = {
    'A_三連複top4BOX':   {'type': 'sanrenpuku', 'n': 4, 'pts': 4,   'cost': 400},
    'B_三連単top3全通':   {'type': 'sanrentan',  'n': 3, 'pts': 6,   'cost': 600},
    'C_三連単top4BOX':   {'type': 'sanrentan',  'n': 4, 'pts': 24,  'cost': 2400},
    'D_三連単1着固定流':  {'type': 'sanrentan_1fix', 'pts': 6, 'cost': 600},
    'E_三連単1-2着固定':  {'type': 'sanrentan_12fix', 'pts': 2, 'cost': 200},
    'F_三連単完全固定':   {'type': 'sanrentan_exact', 'pts': 1, 'cost': 100},
}

tallies = {k: {'invest': 0, 'ret': 0, 'hit': 0, 'total': 0, 'payouts': [], 'hit_details': []} for k in STRUCTURES}

n_races = 0
n_skip_max = 0
n_skip_cum = 0

for race_id in target_ids:
    race_df = df[df['race_id'] == race_id].drop_duplicates(subset=['馬名']).copy()
    if len(race_df) < 5: continue

    race_name = str(race_df['race_name'].iloc[0]) if not pd.isna(race_df['race_name'].iloc[0]) else ''
    course    = str(race_df['コース'].iloc[0]) if not pd.isna(race_df['コース'].iloc[0]) else 'ダート'
    track     = str(race_df['馬場状態'].iloc[0]) if not pd.isna(race_df['馬場状態'].iloc[0]) else ''
    dist      = int(race_df['距離'].iloc[0]) if not pd.isna(race_df['距離'].iloc[0]) else 1800
    venue     = race_id[4:6]
    ym_str    = race_id[:6]

    actual_ranks = {}
    for _, row in race_df.iterrows():
        try:    actual_ranks[str(row['馬名'])] = int(float(row['着順']))
        except: continue
    if not actual_ranks or min(actual_ranks.values()) != 1: continue

    pay_rows = race_df[race_df['着順'] == 1]
    if len(pay_rows) == 0: continue
    pr = pay_rows.iloc[0]
    san_pays    = parse_san(pr.get('三連複払戻', ''))
    santan_pays = parse_santan(pr.get('三連単払戻', ''))

    rank_map = {v: k for k, v in actual_ranks.items()}
    name2ban = {}
    for _, row in race_df.iterrows():
        try:    name2ban[str(row['馬名'])] = str(int(float(row['馬番'])))
        except: pass

    top3_bans_set = frozenset(filter(None, [name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]]))
    if len(top3_bans_set) != 3: continue
    # 順序付き正解
    ban1 = name2ban.get(rank_map.get(1, ''), '')
    ban2 = name2ban.get(rank_map.get(2, ''), '')
    ban3 = name2ban.get(rank_map.get(3, ''), '')
    if not (ban1 and ban2 and ban3): continue

    race_entries = []
    for _, row in race_df.iterrows():
        name   = str(row['馬名'])
        jockey = str(row['騎手']) if not pd.isna(row.get('騎手', '')) else ''
        try:    pop = int(float(row['人気']))
        except: pop = 99
        try:    odds = float(str(row['単勝オッズ']).replace(',', ''))
        except: odds = 0.0
        hist    = horse_db.get(name, [])
        factors = compute_horse_factors(
            name, jockey, course, dist, hist, jstats, dc_db,
            track_cond=track, venue_code=venue, race_ym=ym_str, pop=pop, odds=odds,
        )
        try:    umaban = str(int(float(row['馬番'])))
        except: umaban = ''
        race_entries.append({'name': name, 'factors': factors, 'umaban': umaban})

    if len(race_entries) < 5: continue
    normed = race_relative_features([e['factors'] for e in race_entries])
    horse_probs = []
    for entry, fv_rel in zip(race_entries, normed):
        fv_n = (fv_rel - mean_v) / (std_v + 1e-8)
        prob = float(predict_prob(fv_n.reshape(1, -1), w, b_bias)[0])
        horse_probs.append({'name': entry['name'], 'prob': prob, 'umaban': entry['umaban']})

    ranked   = sorted(horse_probs, key=lambda x: -x['prob'])
    max_p    = ranked[0]['prob'] * 100
    if max_p < 20.0:
        n_skip_max += 1
        continue

    probs    = [h['prob'] * 100 for h in ranked]
    top4_cum = sum(probs[:4])
    if top4_cum < 60.0:
        n_skip_cum += 1
        continue

    n_races += 1
    bans = [e['umaban'] for e in ranked[:4] if e['umaban']]
    if len(bans) < 3: continue
    u1, u2, u3 = ranked[0]['umaban'], ranked[1]['umaban'], ranked[2]['umaban']
    u4 = ranked[3]['umaban'] if len(ranked) >= 4 else ''

    for key, cfg in STRUCTURES.items():
        invest = cfg['cost']
        ret    = 0
        t      = cfg['type']

        if t == 'sanrenpuku':
            top_bans = bans[:cfg['n']]
            for combo in combinations(top_bans, 3):
                if frozenset(combo) == top3_bans_set:
                    ret = san_pays.get(top3_bans_set, 0)
                    break

        elif t == 'sanrentan':
            top_bans = bans[:cfg['n']]
            for perm in permutations(top_bans, 3):
                if perm == (ban1, ban2, ban3):
                    ret = santan_pays.get((ban1, ban2, ban3), 0)
                    break

        elif t == 'sanrentan_1fix':
            # 1着 = top1, 2-3着 = top2,3,4 の全順列
            others = [b for b in bans[:4] if b != u1][:3]
            for perm in permutations(others, 2):
                key_t = (u1, perm[0], perm[1])
                if key_t == (ban1, ban2, ban3):
                    ret = santan_pays.get((ban1, ban2, ban3), 0)
                    break

        elif t == 'sanrentan_12fix':
            # 1着=top1, 2着=top2, 3着=top3 or top4
            tickets = [(u1, u2, u3)]
            if u4: tickets.append((u1, u2, u4))
            for ticket in tickets:
                if ticket == (ban1, ban2, ban3):
                    ret = santan_pays.get((ban1, ban2, ban3), 0)
                    break

        elif t == 'sanrentan_exact':
            ticket = (u1, u2, u3)
            if ticket == (ban1, ban2, ban3):
                ret = santan_pays.get((ban1, ban2, ban3), 0)

        tallies[key]['invest'] += invest
        tallies[key]['ret']    += ret
        tallies[key]['total']  += 1
        if ret > 0:
            tallies[key]['hit'] += 1
            tallies[key]['payouts'].append(ret)
            if key == 'A_三連複top4BOX':
                tallies[key]['hit_details'].append({
                    'race_id': race_id, 'name': race_name,
                    'ret': ret, 'ranked': ranked[:6],
                    'actual_ranks': actual_ranks, 'name2ban': name2ban,
                    'top4_cum': top4_cum, 'gap': probs[0] - probs[1],
                })

print('=' * 78)
print('  三連単 vs 三連複 比較 (2026年ブラインド 突出R8-11 + top4_cum>=60%)')
print('=' * 78)
print(f'  対象レース: {n_races}件  (突出外スキップ:{n_skip_max}件 / top4_cum<60%スキップ:{n_skip_cum}件)')
print()
print(f'  {"構造":<22} {"点数":>4} {"投資/R":>6}  {"件数":>5} {"的中":>5} {"的中率":>7} {"総投資":>9} {"総払戻":>9} {"ROI":>9}')
print(f'  {"-"*80}')

for key, t in tallies.items():
    inv  = t['invest']
    ret  = t['ret']
    hit  = t['hit']
    n    = t['total']
    roi  = (ret - inv) / inv * 100 if inv > 0 else -100.0
    pts  = STRUCTURES[key]['pts']
    cost = STRUCTURES[key]['cost']
    mark = ' ←基準' if key.startswith('A') else ''
    payouts_str = '  ' + ', '.join(f'{p:,}円' for p in sorted(t['payouts'], reverse=True)) if t['payouts'] else ''
    print(f'  {key:<22} {pts:>4}点  {cost:>5}円  {n:>5}件 {hit:>5}件  {hit/n*100:>6.1f}%  {inv:>8,}円  {ret:>8,}円  {roi:>+8.1f}%{mark}')
    if t['payouts']:
        print(f'    └ 払戻内訳:{payouts_str}')

print()
print('  ※ 三連単払戻データが欠損しているレースは払戻0扱い')

# ── A的中レース詳細 ──
print()
print('=' * 78)
print('  A_三連複top4BOX  的中レース詳細')
print('=' * 78)
for d in sorted(tallies['A_三連複top4BOX']['hit_details'], key=lambda x: -x['ret']):
    print(f"\n  [{d['race_id']}] {d['name']}  払戻: {d['ret']:,}円  gap:{d['gap']:+.1f}%  cum:{d['top4_cum']:.1f}%")
    print(f"  {'順位':<4} {'馬名':<12} {'確率':>7} {'モデル順':>6} {'実際着順':>6}")
    print(f"  {'-'*42}")
    for i, h in enumerate(d['ranked'], 1):
        actual = d['actual_ranks'].get(h['name'], '-')
        mark = ' ◀1着' if actual == 1 else (' ◀2着' if actual == 2 else (' ◀3着' if actual == 3 else ''))
        print(f"  {i:<4} {h['name']:<12} {h['prob']*100:>6.1f}%  {i:>4}位  {str(actual):>4}着{mark}")

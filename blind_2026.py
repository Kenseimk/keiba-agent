"""2026年ブラインド検証 (p1-p2>=10%→馬連 / else→三連複top4)"""
import sys, re, glob
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from itertools import combinations
from backtest_agent import load_all_races
from walkforward_winprob import build_db_upto, race_relative_features, predict_prob, load_model
from score_agent_core import load_models, compute_horse_factors, _float, _int

all_races = load_all_races('data')
jstats, dc_db = load_models('data')
w, b, mean, std = load_model()
horse_db = build_db_upto(all_races, '202605')
print(f'horse_db: {len(horse_db)}頭')

# 2026年全CSVをmonth付きで読み込み
frames = []
for f in sorted(glob.glob('data/raceresults_2026*.csv')):
    mo = f.replace('\\','/').split('/')[-1][12:18]  # 202601 etc
    tmp = pd.read_csv(f, encoding='utf-8')
    tmp['month'] = mo
    frames.append(tmp)
df = pd.concat(frames, ignore_index=True)
df['race_id'] = df['race_id'].astype(str)
df = df.drop_duplicates(subset=['race_id','馬名'])
target_ids = sorted(set(df[df['レース番号'].isin([8,9,10,11])]['race_id']))
print(f'2026年 R8-11: {len(target_ids)}レース')

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

print()
print('=' * 74)
print('  2026年ブラインド検証 (p1-p2>=10%→馬連 / else→三連複top4)')
print('=' * 74)
print(f'  {"月":<6} {"race_id":<14} {"レース名":<18} {"max_p":>6} {"gap":>7} {"選択":<5} {"結果"} {"払戻":>8}')
print(f'  {"-"*72}')

results = []
n_skip  = 0
cur_month = None

for race_id in target_ids:
    race_df = df[df['race_id'] == race_id].drop_duplicates(subset=['馬名']).copy()
    if len(race_df) < 4: continue

    month     = race_df['month'].iloc[0]
    race_name = str(race_df['race_name'].iloc[0]) if not pd.isna(race_df['race_name'].iloc[0]) else ''
    course    = str(race_df['コース'].iloc[0]) if not pd.isna(race_df['コース'].iloc[0]) else 'ダート'
    track     = str(race_df['馬場状態'].iloc[0]) if not pd.isna(race_df['馬場状態'].iloc[0]) else ''
    dist      = int(race_df['距離'].iloc[0]) if not pd.isna(race_df['距離'].iloc[0]) else 1800
    venue     = race_id[4:6]
    ym_str    = race_id[:6]

    actual_ranks = {}
    for _, row in race_df.iterrows():
        try:    rank = int(float(row['着順']))
        except: continue
        actual_ranks[str(row['馬名'])] = rank
    if not actual_ranks or min(actual_ranks.values()) != 1: continue

    pay_rows = race_df[race_df['着順'] == 1]
    if len(pay_rows) == 0: continue
    pay_row = pay_rows.iloc[0]
    umaren_pays = parse_pair(pay_row.get('馬連払戻', ''))
    san_pays    = parse_san(pay_row.get('三連複払戻', ''))

    rank_map  = {v: k for k, v in actual_ranks.items()}
    name2ban  = {}
    for _, row in race_df.iterrows():
        try: name2ban[str(row['馬名'])] = str(int(float(row['馬番'])))
        except: pass
    top3_bans = frozenset(filter(None, [name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]]))
    if len(top3_bans) != 3: continue

    race_entries = []
    for _, row in race_df.iterrows():
        name   = str(row['馬名'])
        jockey = str(row['騎手']) if not pd.isna(row.get('騎手', '')) else ''
        try:    pop = int(float(row['人気']))
        except: pop = 99
        try:    odds = float(str(row['単勝オッズ']).replace(',', ''))
        except: odds = 0.0
        hist = horse_db.get(name, [])
        factors = compute_horse_factors(
            name, jockey, course, dist, hist, jstats, dc_db,
            track_cond=track, venue_code=venue, race_ym=ym_str,
            pop=pop, odds=odds,
        )
        try:    umaban = str(int(float(row['馬番'])))
        except: umaban = ''
        race_entries.append({'name': name, 'factors': factors, 'umaban': umaban})

    if len(race_entries) < 4: continue
    normed = race_relative_features([e['factors'] for e in race_entries])
    horse_probs = []
    for entry, fv_rel in zip(race_entries, normed):
        fv_n = (fv_rel - mean) / (std + 1e-8)
        prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
        horse_probs.append({'name': entry['name'], 'prob': prob, 'umaban': entry['umaban']})

    ranked = sorted(horse_probs, key=lambda x: -x['prob'])
    max_p  = ranked[0]['prob'] * 100
    if max_p < 20.0:
        n_skip += 1
        continue

    gap = ranked[0]['prob'] * 100 - ranked[1]['prob'] * 100

    if cur_month != month:
        cur_month = month
        print(f'  ── {month} {"─"*56}')

    if gap >= 10.0:
        bet_type = '馬連'
        key      = frozenset([ranked[0]['umaban'], ranked[1]['umaban']])
        invest   = 100
        ret      = umaren_pays.get(key, 0)
    else:
        bet_type = '三連複'
        top4     = [e['umaban'] for e in ranked[:4] if e['umaban']]
        combos   = list(combinations(top4, 3))
        invest   = len(combos) * 100
        ret      = 0
        for combo in combos:
            if frozenset(combo) == top3_bans:
                ret = san_pays.get(top3_bans, 0)
                break

    hit = '✅' if ret > 0 else '❌'
    results.append({
        'month': month, 'race_id': race_id, 'name': race_name,
        'max_p': max_p, 'gap': gap, 'bet': bet_type,
        'invest': invest, 'ret': ret, 'hit': hit,
    })
    print(f'  {month} {race_id:<14} {race_name[:16]:<18} {max_p:>5.1f}% {gap:>+6.1f}%  {bet_type:<5} {hit}  {ret:>7}円')

print(f'  {"-"*72}')
print(f'  突出外スキップ: {n_skip}レース')

if not results:
    print('  突出レースなし')
else:
    print()
    print(f'  {"月":<8} {"件":>4} {"的中":>4} {"的中率":>7} {"投資":>8} {"払戻":>9} {"ROI":>9}')
    print(f'  {"-"*58}')
    for mo in sorted(set(r['month'] for r in results)):
        sub   = [r for r in results if r['month'] == mo]
        inv   = sum(r['invest'] for r in sub)
        ret_v = sum(r['ret']    for r in sub)
        hits  = sum(1 for r in sub if r['hit'] == '✅')
        roi   = (ret_v - inv) / inv * 100 if inv > 0 else -100.0
        print(f'  {mo}  {len(sub):>4}件  {hits:>3}件  {hits/len(sub)*100:>6.1f}%  {inv:>7,}円  {ret_v:>8,}円  {roi:>+8.1f}%')

    total_inv = sum(r['invest'] for r in results)
    total_ret = sum(r['ret']    for r in results)
    n_hit     = sum(1 for r in results if r['hit'] == '✅')
    n         = len(results)
    roi       = (total_ret - total_inv) / total_inv * 100 if total_inv > 0 else -100.0
    print(f'  {"─"*58}')
    print(f'  {"合計":<8}  {n:>4}件  {n_hit:>3}件  {n_hit/n*100:>6.1f}%  {total_inv:>7,}円  {total_ret:>8,}円  {roi:>+8.1f}%')
    print()
    for bt in ['馬連', '三連複']:
        sub = [r for r in results if r['bet'] == bt]
        if not sub: continue
        i2 = sum(r['invest'] for r in sub)
        r2 = sum(r['ret']    for r in sub)
        h2 = sum(1 for r in sub if r['hit'] == '✅')
        print(f'  {bt}: {len(sub)}件  的中{h2}件 ({h2/len(sub)*100:.0f}%)  ROI {(r2-i2)/i2*100:+.1f}%')

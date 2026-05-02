"""三連複が外れる理由分析: top4に実際の1-3着が何頭含まれているか"""
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

frames = []
for f in sorted(glob.glob('data/raceresults_2026*.csv')):
    mo = f.replace('\\', '/').split('/')[-1][12:18]
    tmp = pd.read_csv(f, encoding='utf-8')
    tmp['month'] = mo
    frames.append(tmp)
df = pd.concat(frames, ignore_index=True)
df['race_id'] = df['race_id'].astype(str)
df = df.drop_duplicates(subset=['race_id', '馬名'])
target_ids = sorted(set(df[df['レース番号'].isin([8, 9, 10, 11])]['race_id']))


def parse_san(raw):
    out = {}
    if not raw or str(raw) == 'nan':
        return out
    for part in str(raw).split('|'):
        m = re.match(r'(\d+)\s*[-\u2013]\s*(\d+)\s*[-\u2013]\s*(\d+):(\d+)', part.strip())
        if m:
            out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out


print('=' * 72)
print('  三連複選択レース詳細 (gap<10% 突出)')
print('  モデルtop4に実際の1-3着が何頭含まれているか')
print('=' * 72)
print(f'  {"レース名":<20} {"max_p":>6} {"gap":>6}  カバー  結果  払戻      外れた馬')
print(f'  {"-"*70}')

san_records = []

for race_id in target_ids:
    race_df = df[df['race_id'] == race_id].drop_duplicates(subset=['馬名']).copy()
    if len(race_df) < 4:
        continue

    race_name = str(race_df['race_name'].iloc[0]) if not pd.isna(race_df['race_name'].iloc[0]) else ''
    course = str(race_df['コース'].iloc[0]) if not pd.isna(race_df['コース'].iloc[0]) else 'ダート'
    track  = str(race_df['馬場状態'].iloc[0]) if not pd.isna(race_df['馬場状態'].iloc[0]) else ''
    dist   = int(race_df['距離'].iloc[0]) if not pd.isna(race_df['距離'].iloc[0]) else 1800
    venue  = race_id[4:6]
    ym_str = race_id[:6]

    actual_ranks = {}
    for _, row in race_df.iterrows():
        try:
            actual_ranks[str(row['馬名'])] = int(float(row['着順']))
        except:
            continue
    if not actual_ranks or min(actual_ranks.values()) != 1:
        continue

    pay_rows = race_df[race_df['着順'] == 1]
    if len(pay_rows) == 0:
        continue
    san_pays = parse_san(pay_rows.iloc[0].get('三連複払戻', ''))

    rank_map  = {v: k for k, v in actual_ranks.items()}
    name2ban  = {}
    for _, row in race_df.iterrows():
        try:
            name2ban[str(row['馬名'])] = str(int(float(row['馬番'])))
        except:
            pass
    top3_bans  = frozenset(filter(None, [name2ban.get(rank_map.get(i, ''), '') for i in [1, 2, 3]]))
    top3_names = [rank_map.get(i, '') for i in [1, 2, 3]]
    if len(top3_bans) != 3:
        continue

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
            track_cond=track, venue_code=venue, race_ym=ym_str, pop=pop, odds=odds,
        )
        try:    umaban = str(int(float(row['馬番'])))
        except: umaban = ''
        race_entries.append({'name': name, 'factors': factors, 'umaban': umaban})

    if len(race_entries) < 4:
        continue
    normed = race_relative_features([e['factors'] for e in race_entries])
    horse_probs = []
    for entry, fv_rel in zip(race_entries, normed):
        fv_n = (fv_rel - mean) / (std + 1e-8)
        prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
        horse_probs.append({'name': entry['name'], 'prob': prob, 'umaban': entry['umaban']})

    ranked = sorted(horse_probs, key=lambda x: -x['prob'])
    max_p  = ranked[0]['prob'] * 100
    if max_p < 20.0:
        continue
    gap = ranked[0]['prob'] * 100 - ranked[1]['prob'] * 100
    if gap >= 10.0:
        continue  # 三連複選択のみ

    top4_names = [e['name'] for e in ranked[:4]]
    covered    = sum(1 for name in top3_names if name in top4_names)
    missed     = [name for name in top3_names if name not in top4_names]

    # 4位馬の実際の着順
    rank4_name   = ranked[3]['name'] if len(ranked) >= 4 else ''
    rank4_actual = actual_ranks.get(rank4_name, 99)
    # 外れた場合、何位の馬がtop4を食った？
    intruder_info = ''
    if covered < 3 and missed:
        missed_actual_rank = actual_ranks.get(missed[0], 99)
        # そのレースでの missed 馬のモデル順位
        missed_model_rank = next((i+1 for i, e in enumerate(ranked) if e['name'] == missed[0]), 99)
        intruder_info = f'{missed[0][:8]}(実{missed_actual_rank}着,モデル{missed_model_rank}位)'

    top4_bans = [e['umaban'] for e in ranked[:4] if e['umaban']]
    combos    = list(combinations(top4_bans, 3))
    invest    = len(combos) * 100
    ret       = 0
    for combo in combos:
        if frozenset(combo) == top3_bans:
            ret = san_pays.get(top3_bans, 0)
            break
    hit = '✅' if ret > 0 else '❌'

    san_records.append({
        'name': race_name, 'max_p': max_p, 'gap': gap,
        'covered': covered, 'invest': invest, 'ret': ret, 'hit': hit,
        'intruder': intruder_info, 'rank4_actual': rank4_actual,
    })
    print(f'  {race_name[:18]:<20} {max_p:>5.1f}% {gap:>+5.1f}%  {covered}/3頭  {hit}  {ret:>6}円  {intruder_info}')

# ─── 集計 ───
print(f'\n{"="*72}')
print(f'  カバー別集計')
print(f'{"="*72}')
print(f'  {"カバー":>6}  {"件数":>5}  {"割合":>6}  {"的中":>5}  {"的中率":>6}  {"平均払戻":>9}')
print(f'  {"-"*50}')
for c in [3, 2, 1, 0]:
    sub = [r for r in san_records if r['covered'] == c]
    if not sub:
        continue
    pct     = len(sub) / len(san_records) * 100
    hits    = sum(1 for r in sub if r['hit'] == '✅')
    hit_pay = [r['ret'] for r in sub if r['ret'] > 0]
    avg_pay = sum(hit_pay) / len(hit_pay) if hit_pay else 0
    label   = '完全カバー' if c == 3 else f'{c}/3頭カバー'
    print(f'  {label:<8}  {len(sub):>5}件  {pct:>5.1f}%  {hits:>5}件  {hits/len(sub)*100:>5.1f}%  {avg_pay:>8,.0f}円')

n  = len(san_records)
ti = sum(r['invest'] for r in san_records)
tr = sum(r['ret']    for r in san_records)
nh = sum(1 for r in san_records if r['hit'] == '✅')
print(f'\n  合計: {n}件  投資 {ti:,}円  払戻 {tr:,}円  ROI {(tr-ti)/ti*100:+.1f}%')

# ─── 根本原因 ───
c3 = sum(1 for r in san_records if r['covered'] == 3)
print(f'\n{"="*72}')
print(f'  根本原因分析')
print(f'{"="*72}')
print(f'  top4に1-3着全頭が入る率: {c3}/{n} = {c3/n*100:.1f}%')
print(f'  (3/3頭入っても払戻がゼロ = データ欠損が {c3 - nh}件)')
print()
print(f'  三連複top4が外れる主な理由:')
print(f'    A) モデルtop4に1-3着が揃わない ({n-c3}件 / {(n-c3)/n*100:.1f}%)')
print(f'       → gap<10%レースはモデルの確信度が低く, 3位以降の馬を読み外す')
print(f'    B) top4に揃っているが払戻0 = データ取得失敗 ({c3-nh}件)')
print()

# ─── gap別分析 ───
print(f'  gap帯別 カバー率:')
for lo, hi in [(0,3),(3,6),(6,10)]:
    sub = [r for r in san_records if lo <= r['gap'] < hi]
    if not sub: continue
    c3s = sum(1 for r in sub if r['covered'] == 3)
    hits = sum(1 for r in sub if r['hit'] == '✅')
    inv = sum(r['invest'] for r in sub)
    ret_v = sum(r['ret'] for r in sub)
    roi = (ret_v - inv) / inv * 100 if inv > 0 else -100
    print(f'    gap {lo:>1}-{hi:>2}%: {len(sub):>3}件  3/3カバー率 {c3s/len(sub)*100:>5.1f}%  ROI {roi:>+7.1f}%')

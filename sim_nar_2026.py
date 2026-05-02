"""地方競馬2026年 三連複top4BOX シミュレーション
地方馬の戦歴は地方CSVから、JRA馬はJRA horse_dbからそれぞれ参照
walk-forward: 月Mのレースには月M未満のNARデータのみ使用
"""
import sys, re, os, glob, csv
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from collections import defaultdict
from itertools import combinations
from backtest_agent import load_all_races
from walkforward_winprob import build_db_upto, race_relative_features, predict_prob, load_model, _int, _float
from walkforward_winprob import parse_avg_pos, parse_bw_change, parse_margin, parse_time
from score_agent_core import load_models, compute_horse_factors

# ── JRAモデル・データ読み込み ──
all_races = load_all_races('data')
jstats, dc_db = load_models('data')
w, b_bias, mean_v, std_v = load_model()
jra_horse_db = build_db_upto(all_races, '202505')
print(f'JRA horse_db: {len(jra_horse_db)}頭')

NAR_VENUE = {
    '30':'門別','31':'帯広','32':'盛岡','33':'水沢',
    '34':'浦和','35':'船橋','36':'大井','37':'川崎',
    '38':'金沢','39':'笠松','40':'名古屋','41':'園田',
    '42':'姫路','43':'高知','44':'佐賀',
}


# ── NAR戦歴DBビルダー (calendar_ym基準でwalk-forward) ──
def _make_nar_record(race_id, row, agari_rank_map, n_agari, n_field, cal_ym):
    rank = _int(str(row.get('着順', '')))
    return {
        'race_id':     race_id,
        'race_ym':     cal_ym,           # カレンダー月で管理
        'venue_code':  race_id[4:6],
        'rank':        rank,
        'field_size':  n_field,
        'agari_rank':  agari_rank_map.get(str(row.get('馬名', '')), -1),
        'agari_field': n_agari,
        'avg_pos':     parse_avg_pos(str(row.get('通過順', ''))),
        'bw_chg':      parse_bw_change(str(row.get('馬体重', ''))),
        'dist':        _int(str(row.get('距離', ''))),
        'track_cond':  str(row.get('馬場状態', '')).strip(),
        'margin':      parse_margin(str(row.get('着差', '')).strip(), rank) if rank else 0.0,
        'race_time':   parse_time(str(row.get('タイム', ''))),
    }


def build_nar_db_upto(nar_dir, cal_ym_excl):
    """NAR horse_db: cal_ym < cal_ym_excl のレースのみ使用"""
    db = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(nar_dir, 'raceresults_nar_*.csv'))):
        m = re.search(r'raceresults_nar_(\d{6})\.csv', os.path.basename(f))
        if not m: continue
        cal_ym = m.group(1)
        if cal_ym >= cal_ym_excl:
            continue   # walk-forward: 対象月以降は使わない
        df = pd.read_csv(f, encoding='utf-8').drop_duplicates(subset=['race_id', '馬名'])
        for race_id, grp in df.groupby('race_id'):
            race_id = str(race_id)
            agari_list = []
            for _, row in grp.iterrows():
                a = _float(str(row.get('上がり3F', '')))
                if a is not None:
                    agari_list.append((str(row['馬名']), a))
            agari_list.sort(key=lambda x: x[1])
            agari_rank_map = {name: i+1 for i, (name, _) in enumerate(agari_list)}
            n_field = len(grp)
            n_agari = len(agari_list)
            for _, row in grp.iterrows():
                if _int(str(row.get('着順', ''))) is None:
                    continue
                rec = _make_nar_record(race_id, row, agari_rank_map, n_agari, n_field, cal_ym)
                db[str(row['馬名'])].append(rec)
    return db


# ── NARデータ読み込み ──
def parse_san(raw):
    out = {}
    if not raw or str(raw) == 'nan': return out
    for part in str(raw).split('|'):
        m = re.match(r'(\d+)\s*[-\u2013]\s*(\d+)\s*[-\u2013]\s*(\d+)\s*[:\uff1a]\s*(\d+)', part.strip())
        if m: out[frozenset([m.group(1), m.group(2), m.group(3)])] = int(m.group(4))
    return out

frames = []
for f in sorted(glob.glob('data/raceresults_nar_2026*.csv')):
    mo = re.search(r'raceresults_nar_(\d{6})\.csv', f).group(1)
    tmp = pd.read_csv(f, encoding='utf-8')
    tmp['cal_month'] = mo
    frames.append(tmp)
df = pd.concat(frames, ignore_index=True)
df['race_id'] = df['race_id'].astype(str)
df = df.drop_duplicates(subset=['race_id', '馬名'])
target_ids = sorted(set(df[df['レース番号'].isin([8,9,10,11])]['race_id']))
print(f'地方競馬 R8-11: {len(target_ids)}レース')

# ── シミュレーション ──
results = []
n_skip_max = 0
n_skip_cum = 0
cur_month  = None
cur_nar_db_ym = None
nar_horse_db   = defaultdict(list)

print()
print('='*80)
print('  地方競馬2026年 三連複top4BOX (JRAモデル + 地方戦歴)')
print('  ルール: 突出(max_p>=20%) + top4_cum>=60% → 三連複top4BOX')
print('='*80)
print(f'  {"月":<6} {"race_id":<14} {"競馬場":<5} {"レース名":<16} {"gap":>6} {"cum":>7} {"結果"} {"払戻":>8}')
print(f'  {"-"*78}')

for race_id in target_ids:
    race_df = df[df['race_id'] == race_id].drop_duplicates(subset=['馬名']).copy()
    if len(race_df) < 5: continue

    cal_month = race_df['cal_month'].iloc[0]
    race_name = str(race_df['race_name'].iloc[0]) if not pd.isna(race_df['race_name'].iloc[0]) else ''
    course    = str(race_df['コース'].iloc[0]) if not pd.isna(race_df['コース'].iloc[0]) else 'ダート'
    track     = str(race_df['馬場状態'].iloc[0]) if not pd.isna(race_df['馬場状態'].iloc[0]) else ''
    try:    dist = int(float(race_df['距離'].iloc[0]))
    except: dist = 1800
    venue      = race_id[4:6]
    ym_str     = race_id[:6]
    venue_name = NAR_VENUE.get(venue, venue)

    # walk-forward: 月が変わったらNAR horse_dbを再構築
    if cal_month != cur_nar_db_ym:
        nar_horse_db  = build_nar_db_upto('data', cal_month)
        cur_nar_db_ym = cal_month
        print(f'  [NAR horse_db更新 → {cal_month}未満: {len(nar_horse_db)}頭]')

    actual_ranks = {}
    for _, row in race_df.iterrows():
        try:    actual_ranks[str(row['馬名'])] = int(float(row['着順']))
        except: continue
    if not actual_ranks or min(actual_ranks.values()) != 1: continue

    pay_rows = race_df[race_df['着順'] == 1]
    if len(pay_rows) == 0: continue
    san_pays = parse_san(pay_rows.iloc[0].get('三連複払戻', ''))

    rank_map = {v: k for k, v in actual_ranks.items()}
    name2ban = {}
    for _, row in race_df.iterrows():
        try:    name2ban[str(row['馬名'])] = str(int(float(row['馬番'])))
        except: pass
    top3_bans = frozenset(filter(None, [name2ban.get(rank_map.get(i,''),'') for i in [1,2,3]]))
    if len(top3_bans) != 3: continue

    race_entries = []
    for _, row in race_df.iterrows():
        name   = str(row['馬名'])
        jockey = str(row['騎手']) if not pd.isna(row.get('騎手','')) else ''
        try:    pop = int(float(row['人気']))
        except: pop = 99
        try:    odds = float(str(row['単勝オッズ']).replace(',',''))
        except: odds = 0.0
        # JRA戦歴 → NAR戦歴 の順で参照 (JRA馬ならJRA, 地方馬ならNAR)
        hist = jra_horse_db.get(name) or nar_horse_db.get(name, [])
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

    probs    = [h['prob']*100 for h in ranked]
    gap      = probs[0] - probs[1]
    top4_cum = sum(probs[:4])
    if top4_cum < 60.0:
        n_skip_cum += 1
        continue

    bans   = [e['umaban'] for e in ranked[:4] if e['umaban']]
    combos = list(combinations(bans, 3))
    invest = len(combos) * 100
    ret    = 0
    for combo in combos:
        if frozenset(combo) == top3_bans:
            ret = san_pays.get(top3_bans, 0)
            break

    if cur_month != cal_month:
        cur_month = cal_month
        print(f'  ── {cal_month} {"─"*58}')

    hit = '✅' if ret > 0 else '❌'
    results.append({
        'month': cal_month, 'race_id': race_id, 'venue': venue_name,
        'name': race_name, 'gap': gap, 'top4_cum': top4_cum,
        'invest': invest, 'ret': ret, 'hit': hit,
    })
    print(f'  {cal_month} {race_id:<14} {venue_name:<5} {race_name[:14]:<16} {gap:>+5.1f}% {top4_cum:>6.1f}%  {hit}  {ret:>7}円')

print(f'  {"-"*78}')
print(f'  突出外スキップ:{n_skip_max}件  top4_cum<60%スキップ:{n_skip_cum}件')

if not results:
    print('  ベット対象レースなし')
else:
    print()
    print(f'  {"月":<8} {"件":>4} {"的中":>4} {"的中率":>7} {"投資":>9} {"払戻":>9} {"ROI":>9}')
    print(f'  {"-"*60}')
    for mo in sorted(set(r['month'] for r in results)):
        sub   = [r for r in results if r['month'] == mo]
        inv   = sum(r['invest'] for r in sub)
        ret_v = sum(r['ret'] for r in sub)
        hits  = sum(1 for r in sub if r['hit'] == '✅')
        roi   = (ret_v - inv) / inv * 100 if inv > 0 else -100.0
        print(f'  {mo}   {len(sub):>4}件  {hits:>3}件  {hits/len(sub)*100:>6.1f}%  {inv:>8,}円  {ret_v:>8,}円  {roi:>+8.1f}%')

    ti = sum(r['invest'] for r in results)
    tr = sum(r['ret'] for r in results)
    nh = sum(1 for r in results if r['hit'] == '✅')
    n  = len(results)
    print(f'  {"-"*60}')
    print(f'  {"合計":<8}  {n:>4}件  {nh:>3}件  {nh/n*100:>6.1f}%  {ti:>8,}円  {tr:>8,}円  {(tr-ti)/ti*100:>+8.1f}%')

    print()
    print('  ── 競馬場別集計 ──')
    for v in sorted(set(r['venue'] for r in results)):
        sub   = [r for r in results if r['venue'] == v]
        inv   = sum(r['invest'] for r in sub)
        ret_v = sum(r['ret'] for r in sub)
        hits  = sum(1 for r in sub if r['hit'] == '✅')
        roi   = (ret_v - inv) / inv * 100 if inv > 0 else -100.0
        print(f'  {v:<5} {len(sub):>4}件  {hits:>3}件  {hits/len(sub)*100:>5.1f}%  ROI {roi:>+7.1f}%')

"""
backtest_combined.py
CSV（オッズ・人気）× JSON（上がり3F・実際の払戻）を組み合わせた
完全なバックテスト

実際の払戻オッズを使うので従来の推定値より正確。
"""
import subprocess, glob, json, re
from collections import defaultdict

JRA = {'札幌','函館','福島','新潟','東京','中山','中京','京都','阪神','小倉'}
VENUE_CODE = {'札幌':'01','函館':'02','福島':'03','新潟':'04','東京':'05',
              '中山':'06','中京':'07','京都':'08','阪神':'09','小倉':'10'}

# 穴馬モデル定数
ODDS_TOP3 = {'10-19':23.6,'20-49':13.4}
FAV_ADJ   = {'1x':-1.3,'2-3':0.0,'4-5':+1.1,'6+':+3.4}
FIELD_ADJ = {'few':+5.3,'mid':0.0,'many':-4.2}
UNDERVALUED_THRESHOLD = {4:6.0,5:8.0,6:10.0,7:12.0,8:15.0}
UNDERVALUED_BONUS = 8.0

def get_ob(o):  return '10-19' if 10<=o<20 else '20-49' if 20<=o<50 else None
def get_fb(fo): return '1x' if fo<2 else '2-3' if fo<4 else '4-5' if fo<6 else '6+'
def get_fsb(fs):return 'few' if fs<=8 else 'mid' if fs<=12 else 'many'
def top3_prob(odds, fav_odds, field_size, popularity):
    ob = get_ob(odds)
    if ob is None: return 0.0
    base = ODDS_TOP3[ob]
    adj  = FAV_ADJ[get_fb(fav_odds)] + FIELD_ADJ[get_fsb(field_size)]
    if odds < UNDERVALUED_THRESHOLD.get(popularity, 999): adj += UNDERVALUED_BONUS
    return max(0.0, min(100.0, base + adj))

def ana_bet(prob, capital):
    if prob >= 28:   return min(int(capital*0.03/100)*100, 15000)
    elif prob >= 25: return min(int(capital*0.02/100)*100, 10000)
    return 0
def ken_bet(capital): return min(int(capital*0.06/100)*100, 50000)

# ============================================================
# CSV読み込み
# ============================================================
def load_csv_data():
    csv_races = {}
    for fpath in sorted(glob.glob('/mnt/project/2024_*.csv')+glob.glob('/mnt/project/2025_*.csv')):
        if 'all' in fpath: continue
        result = subprocess.run(['iconv','-f','shift_jis','-t','utf-8',fpath], capture_output=True)
        lines = result.stdout.decode('utf-8',errors='replace').strip().split('\n')
        i = 0
        while i+1 < len(lines):
            l1=lines[i].rstrip('\r'); l2=lines[i+1].rstrip('\r')
            combined=l1.replace('"','')+l2.replace('"','')
            parts=[p.strip() for p in combined.split(',')]
            if len(parts)>=17 and ':' in parts[0]:
                try:
                    session=parts[16]
                    if not any(v in session for v in JRA): i+=2; continue
                    if '新馬' in parts[15]: i+=2; continue
                    date=parts[14]; race_num=parts[17] if len(parts)>17 else ''
                    key=f'{date}_{session}_{race_num}'
                    if key not in csv_races:
                        csv_races[key]={'horses':{},'date':date,'session':session,'race_num':race_num,'finish_count':0}
                    csv_races[key]['finish_count']+=1
                    fr=csv_races[key]['finish_count']
                    csv_races[key]['horses'][parts[1]]={
                        'finish_rank':fr,'odds':float(parts[7]),'popularity':int(float(parts[8]))
                    }
                except: pass
            i+=2
    print(f'CSV: {len(csv_races)}レース')
    return csv_races

def csv_key_to_race_id(key):
    p=key.split('_')
    if len(p)<3: return None
    date_str,session,race_num=p[0],p[1],p[2]
    ym=re.match(r'(\d{4})年',date_str)
    if not ym: return None
    year=ym.group(1)
    venue_code=next((c for v,c in VENUE_CODE.items() if v in session),None)
    if not venue_code: return None
    kai_m=re.search(r'^(\d+)回',session)
    kai=kai_m.group(1).zfill(2) if kai_m else '01'
    nichi_m=re.search(r'(\d+)日目',session)
    nichi=nichi_m.group(1).zfill(2) if nichi_m else '01'
    race_m=re.search(r'(\d+)',race_num)
    race_n=race_m.group(1).zfill(2) if race_m else '01'
    return f'{year}{venue_code}{kai}{nichi}{race_n}'

# ============================================================
# JSON読み込み（バックテスト環境なのでGitHubから取得済みと仮定し
# ここでは実際の払戻のみ使用）
# ============================================================
# JSON_DATA_DIR は引数で指定（デフォルトはローカルのdataフォルダ）
JSON_DATA_DIR = 'data'  # ← バックテスト実行時はkeiba-agentのdataフォルダ

def load_json_data(data_dir):
    json_data = {}
    for fpath in sorted(glob.glob(f'{data_dir}/netkeiba_*.json')):
        try:
            with open(fpath,encoding='utf-8') as f:
                d=json.load(f)
            json_data.update(d)
        except Exception as e:
            print(f'  {fpath}: {e}')
    print(f'JSON: {len(json_data)}レース')
    return json_data

# ============================================================
# バックテスト本体
# ============================================================
def run_backtest(csv_races, json_data, initial_capital=100000, monthly_supplement=20000):
    # race_idをキーにCSVとJSONを結合
    merged = {}
    for key, csv_info in csv_races.items():
        race_id = csv_key_to_race_id(key)
        if not race_id: continue
        json_info = json_data.get(race_id)
        if not json_info: continue
        merged[race_id] = {
            'csv': csv_info,
            'json': json_info,
            'ym': race_id[:4]+'-'+race_id[4:6],
        }

    print(f'結合: {len(merged)}レース（CSV∩JSON）')

    # 月別グループ
    by_month = defaultdict(list)
    for race_id, info in merged.items():
        by_month[info['ym']].append((race_id, info))

    capital = initial_capital
    stats_kensei = {'total':0,'hit':0,'invest':0,'ret':0}
    stats_ana    = {'total':0,'hit':0,'invest':0,'ret':0}
    # 組み合わせ馬券（実際の払戻使用）
    stats_wide   = {'total':0,'hit':0,'invest':0,'ret':0}
    stats_umaren = {'total':0,'hit':0,'invest':0,'ret':0}
    stats_umatan = {'total':0,'hit':0,'invest':0,'ret':0}
    stats_3puku  = {'total':0,'hit':0,'invest':0,'ret':0}
    stats_3tan   = {'total':0,'hit':0,'invest':0,'ret':0}

    monthly_rows = []

    for ym in sorted(by_month.keys()):
        capital += monthly_supplement
        races = by_month[ym]

        # --- 穴馬選定 ---
        ana_cands = []
        for race_id, info in races:
            csv_info = info['csv']
            horses_csv = csv_info['horses']
            field_size = len(horses_csv)
            fav_odds = min(h['odds'] for h in horses_csv.values()) if horses_csv else 2.0
            for name, h in horses_csv.items():
                if h['popularity'] < 4: continue
                if not (10 <= h['odds'] < 50): continue
                prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                if prob >= 25:
                    ana_cands.append((prob, h, name, race_id, info))

        ana_cands.sort(key=lambda x: -x[0])
        seen_races = set()
        ana_selected = []
        for prob, h, name, race_id, info in ana_cands:
            if race_id in seen_races: continue
            seen_races.add(race_id)
            bet = ana_bet(prob, capital)
            if bet >= 100:
                ana_selected.append((prob, h, name, race_id, info, bet))
            if len(ana_selected) >= 8: break

        # --- 堅実選定（1番人気 月4件） ---
        ken_cands = []
        for race_id, info in races:
            horses_csv = info['csv']['horses']
            fav = min(horses_csv.values(), key=lambda h: h['popularity']) if horses_csv else None
            if fav:
                ken_cands.append((fav['odds'], fav, race_id, info))
        ken_cands.sort(key=lambda x: x[0])
        ken_selected = ken_cands[:4]

        m_ken_invest = m_ken_ret = m_ken_hit = 0
        m_ana_invest = m_ana_ret = m_ana_hit = 0

        # --- 堅実単勝ベット（実際の払戻使用）---
        for fav_odds, fav, race_id, info in ken_selected:
            bet = ken_bet(capital)
            capital -= bet
            m_ken_invest += bet
            stats_kensei['total'] += 1
            stats_kensei['invest'] += bet

            payouts = info['json']['payouts']
            tansho = payouts.get('単勝', [])
            if tansho and fav['finish_rank'] == 1:
                payout = int(bet * tansho[0]['payout'] / 100)
                capital += payout
                m_ken_ret += payout
                m_ken_hit += 1
                stats_kensei['hit'] += 1
                stats_kensei['ret'] += payout

        # --- 穴馬複勝ベット（実際の払戻使用）---
        for prob, h, name, race_id, info, bet in ana_selected:
            capital -= bet
            m_ana_invest += bet
            stats_ana['total'] += 1
            stats_ana['invest'] += bet

            payouts = info['json']['payouts']
            fukusho = payouts.get('複勝', [])

            # 3着以内かどうか
            if h['finish_rank'] <= 3 and fukusho:
                # 着順に対応する複勝払戻を取得
                rank = h['finish_rank']
                if rank - 1 < len(fukusho):
                    payout_val = fukusho[rank-1]['payout']
                else:
                    payout_val = fukusho[-1]['payout']
                payout = int(bet * payout_val / 100)
                capital += payout
                m_ana_ret += payout
                m_ana_hit += 1
                stats_ana['hit'] += 1
                stats_ana['ret'] += payout

        # --- 組み合わせ馬券（実際の払戻データ全件集計）---
        for race_id, info in races:
            payouts = info['json']['payouts']
            # ワイド（1口100円換算で全払戻の平均を取る）
            for ptype, stat in [('ワイド',stats_wide),('馬連',stats_umaren),('馬単',stats_umatan)]:
                pt = payouts.get(ptype,[])
                if pt:
                    stat['total'] += len(pt)
                    stat['invest'] += len(pt)*100
                    stat['hit'] += len(pt)
                    stat['ret'] += sum(p['payout'] for p in pt)

            for ptype, stat in [('三連複',stats_3puku),('三連単',stats_3tan),('3連複',stats_3puku),('3連単',stats_3tan)]:
                pt = payouts.get(ptype,[])
                if pt:
                    stat['total'] += len(pt)
                    stat['invest'] += len(pt)*100
                    stat['hit'] += len(pt)
                    stat['ret'] += sum(p['payout'] for p in pt)

        monthly_rows.append({
            'ym':ym,
            'k_invest':m_ken_invest,'k_ret':m_ken_ret,'k_hit':m_ken_hit,
            'a_invest':m_ana_invest,'a_ret':m_ana_ret,'a_hit':m_ana_hit,
            'cap':int(capital)
        })

    return stats_kensei, stats_ana, stats_wide, stats_umaren, stats_umatan, stats_3puku, stats_3tan, monthly_rows

# ============================================================
# 出力
# ============================================================
def print_results(sk, sa, sw, su, sut, s3p, s3t, monthly_rows):
    total_races = sum(r['k_invest']//ken_bet(100000)+r['a_invest']//10000 for r in monthly_rows)

    print()
    print('='*75)
    print('=== 全馬券種バックテスト（実際の払戻データ使用）===')
    print('='*75)
    print(f'期間: {monthly_rows[0]["ym"]} 〜 {monthly_rows[-1]["ym"]}（{len(monthly_rows)}ヶ月）')
    print()

    # 堅実・穴馬
    for label, s in [('【堅実】単勝（1番人気・月4件・軍資金×6%）',sk),
                     ('【穴馬】複勝（確率25%以上・月8件・軍資金×2〜3%）',sa)]:
        hr  = s['hit']/s['total']*100 if s['total'] else 0
        roi = s['ret']/s['invest']*100 if s['invest'] else 0
        print(f'{label}')
        print(f'  参加:{s["total"]}件 的中:{s["hit"]}件({hr:.1f}%) 投資:¥{s["invest"]:,} 払戻:¥{s["ret"]:,} 回収率:{roi:.1f}%')
        print()

    total_invest = sk['invest']+sa['invest']
    total_ret    = sk['ret']+sa['ret']
    print(f'【堅実+穴馬 合計】')
    print(f'  投資:¥{total_invest:,} 払戻:¥{total_ret:,} 回収率:{total_ret/total_invest*100 if total_invest else 0:.1f}%')
    print()
    print('-'*75)
    print('【払戻データから見る全馬券種の平均回収率】（JRA全レース・実測値）')
    print()

    for label, s in [('ワイド', sw),('馬連', su),('馬単', sut),('三連複', s3p),('三連単', s3t)]:
        if s['total'] == 0: continue
        roi = s['ret']/s['invest']*100 if s['invest'] else 0
        avg = s['ret']//s['total'] if s['total'] else 0
        print(f'  {label:<8}: 件数{s["total"]:,} 平均払戻¥{avg:,}/100円 回収率{roi:.1f}%')

    print()
    print('【月別詳細（堅実+穴馬）】')
    print(f'月       | 堅実       | 穴馬          | 合計損益  | 軍資金')
    print('-'*70)
    for r in monthly_rows:
        k_roi = r['k_ret']/r['k_invest']*100 if r['k_invest'] else 0
        a_roi = r['a_ret']/r['a_invest']*100 if r['a_invest'] else 0
        t_profit = (r['k_ret']+r['a_ret'])-(r['k_invest']+r['a_invest'])
        s='+' if t_profit>=0 else ''
        print(f'{r["ym"]} | {r["k_hit"]}的中/{r["k_invest"]//1000}k→{r["k_ret"]//1000}k({k_roi:.0f}%) | '
              f'{r["a_hit"]}的中/{r["a_invest"]//1000}k→{r["a_ret"]//1000}k({a_roi:.0f}%) | '
              f'{s}¥{abs(t_profit)//1000}k | ¥{r["cap"]:,}')

if __name__ == '__main__':
    import sys
    data_dir = sys.argv[1] if len(sys.argv)>1 else 'data'
    print(f'[backtest] data_dir={data_dir}')
    csv_races = load_csv_data()
    json_data = load_json_data(data_dir)
    sk,sa,sw,su,sut,s3p,s3t,monthly = run_backtest(csv_races, json_data)
    print_results(sk,sa,sw,su,sut,s3p,s3t,monthly)

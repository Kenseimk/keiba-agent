#!/usr/bin/env python3
"""
score_v4.py  競馬予想スコアリング v4.0
=====================================
新機能:
  - 同コース・同距離実績スコア (×0.5)
  - 前走着順（複合条件フラグ）
  - テン3Fスコア（レースラップから計算）
  - 三連複 vs 三連単マルチ 自動判定
  - 買い方テンプレート自動選択（ケース1〜4）

使い方:
  python score_v4.py --race <race_id> [--budget 5000]

または関数として import して使用
"""

import json, re
import numpy as np
import pandas as pd

# ===== ロードデータ =====
def load_models(csv_dir='data'):
    df = pd.read_csv(f'{csv_dir}/df_v4.csv')
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
    df['win']  = (df['rank']==1).astype(int)
    js = df.groupby('jockey').agg(rides=('win','count'), wins=('win','sum')).query('rides>=50')
    js['j_score'] = (js['wins']/js['rides']/(js['wins']/js['rides']).max()*10).round(2)
    
    # 同コース・同距離実績DB
    dc = df.groupby(['horse','dist_course']).agg(
        dc_n=('win','count'), dc_wins=('win','sum')
    ).reset_index()
    dc['dc_wr'] = dc['dc_wins']/dc['dc_n']
    
    return js, dc

# ===== 各スコア関数 =====
def ev_score(o):
    o = float(o)
    return 7 if o<=4 else 8 if o<=7 else 10 if o<=15 else 8 if o<=30 else 9 if o<=99 else 1

def pop_score(p):
    p = float(p)
    return 10 if p==1 else 7 if p<=3 else 5 if p<=5 else 3 if p<=9 else 1

def bw_score(c):
    c = float(c) if c else 0
    return 9 if c>8 else 8 if 1<=c<=4 else 8 if c<=0 else 7 if c<=8 else 6

def pot_score(pb, mean=55.85, std=8.81):
    try: return max(0.0, min(10.0, (float(pb)-mean)/std*3+5))
    except: return 5.0

def prev_rank_flag(prev_rank, current_pop):
    """
    前走着順の複合条件フラグ
    「前走1〜2着 かつ 今走5人気以上（市場が過小評価）」→ 注目馬フラグ
    単純スコア加算より、フラグとして使う
    """
    try:
        pr = float(prev_rank); cp = float(current_pop)
        if pr <= 2 and cp >= 5:
            return True, f"前走{int(pr)}着→今走{int(cp)}人気（割安候補）"
        if pr <= 3 and cp >= 7:
            return True, f"前走{int(pr)}着→今走{int(cp)}人気（穴候補）"
    except: pass
    return False, ""

def agari_score(agari, race_mean, race_std):
    """上がり3Fスコア（レース内相対評価）"""
    try:
        a = float(agari)
        if race_std == 0: return 5.0
        return max(0.0, min(10.0, (race_mean - a) / race_std * 3 + 5))
    except: return 5.0

def position_score(avg_pos, course, dist, n=12):
    """脚質スコア（通過順から計算）"""
    try:
        p = float(avg_pos)
        base = max(0.0, min(10.0, (n - p + 1) / n * 10))
        # コース別補正
        if 'ダート' in str(course) or 'ダ' in str(course):
            w = 1.2
        elif float(dist) >= 2400:
            w = 0.6
        else:
            w = 0.8
        return base * w
    except: return 5.0 * 0.8

def dc_score(dc_n, dc_wr):
    """同コース・同距離実績スコア（v4.0追加）"""
    try:
        n = float(dc_n); wr = float(dc_wr)
        if n == 0: return 5.0
        score = wr * 10 + 2
        weight = min(n / 5.0, 1.0)
        return max(0.0, min(10.0, weight * score + (1-weight) * 5.0))
    except: return 5.0

def ten3f_score(horse_ten3f, race_ten3f_mean, race_ten3f_std):
    """
    テン3Fスコア（v4.0追加）
    テン3Fが速い馬 = 先行力あり = ダートで有利
    ※上がりと逆で「速い（小さい）ほど高スコア」
    """
    try:
        t = float(horse_ten3f)
        if race_ten3f_std == 0: return 5.0
        # テン3Fは小さいほど良い（速い）→ 上がりと逆
        return max(0.0, min(10.0, (race_ten3f_mean - t) / race_ten3f_std * 3 + 5))
    except: return 5.0

def parse_passage(p):
    """通過順文字列 → 平均位置取り"""
    if not p: return None
    parts = [float(x) for x in str(p).split('-') if x.strip().isdigit()]
    return np.mean(parts) if parts else None

def parse_bw_change(w):
    """馬体重文字列 → 増減値"""
    m = re.search(r'\(([+-]?\d+)\)', str(w))
    return int(m.group(1)) if m else 0

# ===== メインスコア計算 =====
def calc_total_score(horse_name, jockey, odds, pop, bw_chg,
                     js, dc_db,
                     past_best_dev=None,
                     agari=None, race_agari_mean=None, race_agari_std=None,
                     avg_pos=None, course='ダート', dist=1800,
                     ten3f=None, race_ten3f_mean=None, race_ten3f_std=None):
    
    o = float(odds); p = float(pop)
    j = float(js.loc[jockey,'j_score']) if jockey in js.index else 3.0
    
    # 基本スコア
    ev  = ev_score(o)
    ps  = pop_score(p)
    bw  = bw_score(bw_chg)
    pot = pot_score(past_best_dev)
    
    # 同コース実績（v4.0）
    dc_key = f"{int(dist)}_{course}"
    dc_row = dc_db[(dc_db['horse']==horse_name) & (dc_db['dist_course']==dc_key)]
    if len(dc_row) > 0:
        dcs = dc_score(dc_row['dc_n'].iloc[0], dc_row['dc_wr'].iloc[0])
    else:
        dcs = 5.0
    
    # 上がり3Fスコア（当日予想モード）
    ags = agari_score(agari, race_agari_mean, race_agari_std) if agari is not None else 5.0
    
    # 脚質スコア（当日予想モード）
    pos = position_score(avg_pos, course, dist) if avg_pos is not None else 5.0 * 0.8
    
    # テン3Fスコア（当日予想モード）
    t3f = ten3f_score(ten3f, race_ten3f_mean, race_ten3f_std) if ten3f is not None else None
    
    score = (j*1.0 + ev*1.0 + ps*2.0 + bw*0.5 + pot*0.5 + dcs*0.5
             + ags*1.0 + pos)
    
    if t3f is not None:
        score += t3f * 0.8  # テン3Fスコアを追加
    
    return {
        'score': round(score, 1),
        'j_score': round(j,1), 'ev_score': ev, 'pop_score': ps*2.0,
        'bw_score': bw, 'pot_score': round(pot,1), 'dc_score': round(dcs,1),
        'agari_score': round(ags,1), 'pos_score': round(pos,1),
        'ten3f_score': round(t3f,1) if t3f is not None else None,
    }

# ===== 条件判定 =====
def judge_condition(best_odds, n, gap, dist):
    b  = (3 <= float(best_odds) < 5)
    cC = b and (n <= 14)
    cA = b and (n <= 12) and (gap >= 3) and (float(dist) >= 1800)
    cAp= b and (n <= 12) and (3 <= gap < 5) and (float(dist) >= 1800)
    if cAp: return "★★条件A'（最優先・回収254%）"
    if cA:  return "★条件A（上乗せ・回収180%）"
    if cC:  return "○条件C（参加・回収113%）"
    return "—見送り"

# ===== 買い方テンプレート自動選択 =====
def recommend_bet(horses_sorted, condition, budget, tan3_odds=None, sanpuku_odds=None):
    """
    horses_sorted: スコア順に並んだ馬リスト
    condition: 条件A'/A/C
    budget: 予算（円）
    tan3_odds: 三連単オッズ（既知の場合）
    sanpuku_odds: 三連複オッズ（既知の場合）
    
    返値: 推奨馬券の辞書
    """
    h1 = horses_sorted[0]
    h2 = horses_sorted[1] if len(horses_sorted) > 1 else None
    h3 = horses_sorted[2] if len(horses_sorted) > 2 else None
    
    # 2・3位が人気外かどうか
    h2_pop = float(h2['pop']) if h2 else 99
    h3_pop = float(h3['pop']) if h3 else 99
    has_anaba = (h2_pop >= 4 or h3_pop >= 4)
    
    # 三連複 vs 三連単マルチ 判定
    multi_better = False
    if tan3_odds and sanpuku_odds:
        multi_better = float(tan3_odds) > float(sanpuku_odds) * 6
        multi_msg = f"三連単({tan3_odds}) > 三連複({sanpuku_odds})×6={float(sanpuku_odds)*6:.0f} → {'マルチ有利' if multi_better else '三連複有利'}"
    else:
        multi_msg = "三連単オッズ未判明 → 三連複推奨（デフォルト）"
    
    # ケース判定
    if '条件A' in condition and has_anaba:
        # ケース4：脚質スコアで人気外浮上
        case = 4
        case_label = "ケース4：人気外が複数浮上型"
        bets = [
            {'type':'単勝', 'horse':h1['name'], 'amount': int(budget*0.35)},
            {'type':'三連複', 'horses':[h1['name'],h2['name'],h3['name']], 'amount': int(budget*0.15)},
        ]
        if h2_pop >= 4:
            bets.append({'type':'複勝', 'horse':h2['name'], 'amount': int(budget*0.08),
                        'note':f"{h2['pop']}人気・上がり/脚質で浮上"})
    elif '条件A' in condition:
        # ケース1：本命強い
        case = 1
        case_label = "ケース1：本命強い・人気薄ヒモ"
        bets = [
            {'type':'単勝', 'horse':h1['name'], 'amount': int(budget*0.45)},
            {'type':'三連複', 'horses':[h1['name'],h2['name'],h3['name']], 'amount': int(budget*0.12)},
        ]
    else:
        # ケース3：3頭絞れた・順番不明
        case = 3
        case_label = "ケース3：3頭絞れた・順番不明"
        bets = [
            {'type':'単勝', 'horse':h1['name'], 'amount': int(budget*0.40)},
            {'type':'三連複', 'horses':[h1['name'],h2['name'],h3['name']], 'amount': int(budget*0.15)},
        ]
    
    # マルチ追加判定
    if multi_better and h2 and h3:
        bets.append({'type':'三連単マルチ', 'horses':[h1['name'],h2['name'],h3['name']],
                    'amount': int(budget*0.10), 'note':multi_msg})
    
    total = sum(b['amount'] for b in bets)
    
    return {
        'case': case,
        'case_label': case_label,
        'bets': bets,
        'total': total,
        'multi_judgment': multi_msg,
        'remainder': budget - total,
    }

# ===== レース結果から全馬スコアを計算してまとめる =====
def analyze_race(race_name, course, dist, horses_data, js, dc_db,
                 race_lap=None, budget=5000):
    """
    horses_data: list of dict
        必須: name, jockey, odds, pop, bw_chg
        任意: agari（上がり3F平均）, avg_pos（通過順平均）, ten3f（テン3F）
              past_best_dev（タイム偏差値）, prev_rank（前走着順）
    race_lap: レースのラップタイムリスト（200m刻み）
    """
    n = len(horses_data)
    
    # レース内の上がり平均・標準偏差
    agaris = [float(h['agari']) for h in horses_data if h.get('agari')]
    r_agari_mean = np.mean(agaris) if agaris else None
    r_agari_std  = np.std(agaris)  if agaris else None
    
    # テン3F（レースラップから計算）
    r_ten3f_mean = r_ten3f_std = None
    if race_lap and len(race_lap) >= 3:
        race_ten3f = sum(race_lap[:3])  # 最初の3ハロン
        # 個別テン3Fは通過順から推定（簡易版）
    
    # 各馬スコア計算
    results = []
    for h in horses_data:
        avg_pos = parse_passage(h.get('passage')) if h.get('passage') else None
        sc = calc_total_score(
            horse_name = h['name'],
            jockey     = h['jockey'],
            odds       = h['odds'],
            pop        = h['pop'],
            bw_chg     = h.get('bw_chg', 0),
            js         = js,
            dc_db      = dc_db,
            past_best_dev = h.get('past_best_dev'),
            agari         = h.get('agari'),
            race_agari_mean = r_agari_mean,
            race_agari_std  = r_agari_std,
            avg_pos    = avg_pos,
            course     = course,
            dist       = dist,
            ten3f      = h.get('ten3f'),
            race_ten3f_mean = r_ten3f_mean,
            race_ten3f_std  = r_ten3f_std,
        )
        
        # 前走着順フラグ
        flag, flag_msg = prev_rank_flag(h.get('prev_rank'), h['pop'])
        etype = ('先行' if avg_pos and avg_pos<=4 else
                 '好位' if avg_pos and avg_pos<=7 else
                 '差し' if avg_pos and avg_pos<=10 else
                 '追込' if avg_pos else '不明')
        
        results.append({
            'name':    h['name'],
            'jockey':  h['jockey'],
            'odds':    float(h['odds']),
            'pop':     int(h['pop']),
            'bw_chg':  h.get('bw_chg', 0),
            'agari':   h.get('agari'),
            'avg_pos': round(avg_pos, 1) if avg_pos else None,
            'etype':   etype,
            'flag':    flag_msg,
            **sc,
        })
    
    results.sort(key=lambda x: x['score'], reverse=True)
    
    best = results[0]
    gap  = best['score'] - results[1]['score'] if len(results) > 1 else 0
    cond = judge_condition(best['odds'], n, gap, dist)
    
    # 買い方推奨
    bet = recommend_bet(results, cond, budget)
    
    return {
        'race_name': race_name,
        'condition': cond,
        'gap': round(gap, 1),
        'horses': results,
        'best': best,
        'bet': bet,
    }

# ===== 結果表示 =====
def print_result(result):
    r = result
    print(f"\n{'='*68}")
    print(f"【{r['race_name']}】")
    print(f"  判定: {r['condition']}  スコア差: {r['gap']}pt")
    print(f"{'='*68}")
    
    headers = ['馬名','騎手','オッズ','人気','脚質','上がりPt','同コースPt','スコア','フラグ']
    print(f"\n{'馬名':16} {'騎手':8} {'オッズ':>6} {'人気':>4} {'脚質':>4} "
          f"{'上がり':>6} {'同コース':>8} {'スコア':>7}  フラグ")
    print("-"*80)
    for h in r['horses']:
        flag = h.get('flag','')
        print(f"{h['name']:16} {h['jockey']:8} {h['odds']:>6.1f} {h['pop']:>4} {h['etype']:>4} "
              f"{h['agari_score']:>6.1f} {h['dc_score']:>8.1f} {h['score']:>7.1f}  {flag}")
    
    b = r['best']
    print(f"\n★本命: {b['name']}（{b['jockey']}）{b['odds']}倍 {b['pop']}人気 スコア{b['score']}")
    
    bet = r['bet']
    print(f"\n【買い方推奨：{bet['case_label']}】")
    print(f"  {bet['multi_judgment']}")
    for bx in bet['bets']:
        if bx['type'] == '三連複':
            print(f"  {bx['type']} {'→'.join(bx['horses'])}  {bx['amount']:,}円")
        elif bx['type'] == '三連単マルチ':
            print(f"  {bx['type']} {'→'.join(bx['horses'])} 6点  {bx['amount']:,}円/点  ※{bx.get('note','')}")
        else:
            note = f"  ※{bx.get('note','')}" if bx.get('note') else ''
            print(f"  {bx['type']} {bx['horse']}  {bx['amount']:,}円{note}")
    print(f"  合計 {bet['total']:,}円 （残{bet['remainder']:,}円）")

if __name__ == '__main__':
    print("score_v4.py ロード完了")
    print("使い方: from score_v4 import analyze_race, load_models")

# ===== テン3F ペース補正（v4.0完成版） =====

# 距離別テン3F基準値（平均ペース）
TEN3F_BASELINE = {
    '芝1000': 34.0, '芝1200': 34.5, '芝1400': 35.5, '芝1600': 36.0,
    '芝1800': 37.0, '芝2000': 37.5, '芝2200': 38.0, '芝2400': 38.5,
    '芝2600': 39.0, '芝3000': 39.5, '芝3200': 40.0,
    'ダート1000': 36.0, 'ダート1150': 37.0, 'ダート1200': 37.0,
    'ダート1400': 37.5, 'ダート1600': 37.5, 'ダート1700': 37.5,
    'ダート1800': 38.0, 'ダート2000': 38.5, 'ダート2100': 39.0, 'ダート2400': 40.0,
}

def calc_pace_factor(race_ten3f, course, dist):
    """
    ペース係数を計算
    > 1.0: スローペース（先行有利・差し不利）
    < 1.0: ハイペース（差し有利・先行不利）
    = 1.0: 平均ペース
    
    例: 芝2600m基準39.0秒、実際37.9秒
        diff = 39.0 - 37.9 = -1.1秒（ハイペース）
        pace_factor = 1.0 + (-1.1) * 0.15 = 0.835
    """
    if race_ten3f is None:
        return 1.0  # 不明時は中立
    key = f"{'ダート' if 'ダ' in str(course) else '芝'}{int(dist)}"
    baseline = TEN3F_BASELINE.get(key)
    if baseline is None:
        # 最も近い距離の基準値を使用
        course_key = 'ダート' if 'ダ' in str(course) else '芝'
        dists = {int(k.replace(course_key,'')): v for k,v in TEN3F_BASELINE.items() if k.startswith(course_key)}
        if not dists: return 1.0
        closest = min(dists.keys(), key=lambda d: abs(d - int(dist)))
        baseline = dists[closest]
    
    diff = float(race_ten3f) - baseline  # 正=スロー(レースが遅い)、負=ハイ(レースが速い)
    pace_factor = 1.0 + diff * 0.15     # スローほど1より大きい（先行有利）
    return max(0.7, min(1.3, pace_factor))  # 0.7〜1.3の範囲に制限

def pace_adjusted_score(agari_sc, pos_sc, pace_factor, etype):
    """
    ペース補正済みの上がり・脚質スコアを返す
    
    ハイペース(pace_factor < 1.0):
        差し・追込の上がりスコアを増幅（2.0 - pace_factor）
        先行の脚質スコアを減衰（pace_factor）
    
    スローペース(pace_factor > 1.0):
        先行の脚質スコアを増幅（pace_factor）
        差し・追込の上がりスコアを減衰（2.0 - pace_factor）
    """
    is_frontrunner = etype in ('先行', '好位')
    
    if is_frontrunner:
        # 先行馬はペース因子で脚質を補正
        adj_pos   = pos_sc * pace_factor
        adj_agari = agari_sc * (2.0 - pace_factor)
    else:
        # 差し・追込馬はハイペースで上がりが増幅
        adj_agari = agari_sc * (2.0 - pace_factor)
        adj_pos   = pos_sc * pace_factor
    
    return adj_agari, adj_pos

def calc_total_score_v4(horse_name, jockey, odds, pop, bw_chg,
                        js, dc_db,
                        past_best_dev=None,
                        agari=None, race_agari_mean=None, race_agari_std=None,
                        avg_pos=None, course='ダート', dist=1800,
                        race_ten3f=None):
    """
    v4.0 完成版スコア計算
    テン3F（ペース補正）込みの最終版
    """
    o = float(odds); p = float(pop)
    j = float(js.loc[jockey,'j_score']) if jockey in js.index else 3.0
    
    ev  = ev_score(o)
    ps  = pop_score(p)
    bw  = bw_score(bw_chg)
    pot = pot_score(past_best_dev)
    
    # 同コース実績
    key = f"{int(dist)}_{'ダート' if 'ダ' in str(course) else '芝'}"
    dc_row = dc_db[(dc_db['horse']==horse_name) & (dc_db['dist_course']==key)]
    dcs = dc_score(dc_row['dc_n'].iloc[0], dc_row['dc_wr'].iloc[0]) if len(dc_row) > 0 else 5.0
    
    # 上がり3Fスコア
    ags_raw = agari_score(agari, race_agari_mean, race_agari_std) if agari is not None else 5.0
    
    # 脚質スコア（コース・距離補正）
    if 'ダ' in str(course):
        pos_w = 1.2
    elif int(dist) >= 2400:
        pos_w = 0.6
    else:
        pos_w = 0.8
    raw_pos = max(0.0, min(10.0, ((8 - (avg_pos or 5.0) + 1) / 8 * 10)))
    pos_raw = raw_pos * pos_w
    
    etype = ('先行' if (avg_pos or 99)<=4 else '好位' if (avg_pos or 99)<=7 else
             '差し' if (avg_pos or 99)<=10 else '追込')
    
    # ペース補正
    pace_factor = calc_pace_factor(race_ten3f, course, dist)
    ags_adj, pos_adj = pace_adjusted_score(ags_raw, pos_raw, pace_factor, etype)
    
    score = j*1.0 + ev*1.0 + ps*2.0 + bw*0.5 + pot*0.5 + dcs*0.5 + ags_adj*1.0 + pos_adj
    
    return {
        'score': round(score, 1),
        'breakdown': {
            '騎手': round(j,1), 'EV': ev, '人気': round(ps*2.0,1),
            '馬体重': round(bw*0.5,1), 'タイム偏差値': round(pot*0.5,1),
            '同コース': round(dcs*0.5,1),
            '上がり(補正前)': round(ags_raw,1), '上がり(補正後)': round(ags_adj,1),
            '脚質(補正前)': round(pos_raw,1), '脚質(補正後)': round(pos_adj,1),
            'ペース係数': round(pace_factor,3), '脚質タイプ': etype,
        }
    }

print("テン3Fペース補正スコア関数 追加完了")

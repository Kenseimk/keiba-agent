"""
backtest_csv.py - CSVデータだけで完結するバックテスト

data/raceresults_YYYYMM.csv から全馬券種の実際の払戻データを集計。

使い方:
    python backtest_csv.py
    python backtest_csv.py data
"""
import sys, glob, csv, io, re, math
from collections import defaultdict
from math import comb

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

# 穴馬モデル定数
ODDS_TOP3 = {'10-19': 23.6, '20-49': 13.4}
FAV_ADJ   = {'1x': -1.3, '2-3': 0.0, '4-5': +1.1, '6+': +3.4}
FIELD_ADJ = {'few': +5.3, 'mid': 0.0, 'many': -4.2}
UNDERVALUED_THRESHOLD = {4: 6.0, 5: 8.0, 6: 10.0, 7: 12.0, 8: 15.0}
UNDERVALUED_BONUS = 8.0

# 穴馬設定
ANA_ODDS_MAX      = 30    # オッズ上限
ANA_PROB_MIN      = 25.0  # 複勝確率最低ライン
ANA_COUNT_MAX     = 15    # 件数上限
ANA_FIELD_MIN     = 8     # 出走頭数下限（少頭数は荒れ性質が異なるため除外）

# スコアリング重み（⑤ コーナースコア追加で再配分）
W_MARKET     = 0.27
W_F3RANK     = 0.23
W_JOCKEY     = 0.18
W_PREV_RANK  = 0.13
W_WEIGHT_CHG = 0.09
W_CORNER     = 0.10  # ⑤ 前走コーナー位置（新規）

# ③ スコア閾値（0.93は厳しすぎたため0.90に戻す）
SCORE_THRESHOLD = 0.90

def get_ob(o):   return '10-19' if 10 <= o < 20 else '20-49' if 20 <= o < 50 else None
def get_fb(fo):  return '1x' if fo < 2 else '2-3' if fo < 4 else '4-5' if fo < 6 else '6+'
def get_fsb(fs): return 'few' if fs <= 8 else 'mid' if fs <= 12 else 'many'

def top3_prob(odds, fav_odds, field_size, popularity):
    ob = get_ob(odds)
    if ob is None: return 0.0
    base = ODDS_TOP3[ob]
    adj  = FAV_ADJ[get_fb(fav_odds)] + FIELD_ADJ[get_fsb(field_size)]
    if odds < UNDERVALUED_THRESHOLD.get(popularity, 999): adj += UNDERVALUED_BONUS
    return max(0.0, min(100.0, base + adj))

# Kelly基準ベット額（conservative型：小ベットで長期安定）
def ana_bet_kelly(prob, capital):
    """確率に応じてベット額を変動させるKelly風配分（conservative）"""
    if prob >= 35:   return min(int(capital * 0.03 / 100) * 100, 20000)
    elif prob >= 30: return min(int(capital * 0.02 / 100) * 100, 15000)
    else:            return min(int(capital * 0.015 / 100) * 100, 8000)

def ken_bet(capital):
    return min(int(capital * 0.06 / 100) * 100, 50000)

# FUKUSHO設定（隠れ末脚型複勝）
FUK_ODDS_MIN  = 14.0
FUK_ODDS_MAX  = 18.0
FUK_POP_MIN   = 6
FUK_POP_MAX   = 12
FUK_FIELD_MIN = 8

def fuk_bet(capital):
    return min(int(capital * 0.02 / 100) * 100, 15000)

# ============================================================
# スコアリング関数
# ============================================================
def parse_weight_change(s):
    m = re.match(r'\d+\(([+-]?\d+)\)', s.strip())
    return int(m.group(1)) if m else None

def score_market(odds, field_size):
    raw = math.log(max(field_size / odds, 0.01))
    return (min(max(raw, -2.0), 2.0) + 2.0) / 4.0

def score_jockey(jockey, jstats):
    return jstats.get(jockey, 3.0) / 10.0

def score_f3rank(name, prev_history):
    h = prev_history.get(name)
    if h is None: return 0.3
    rank, n = h['f3rank'], h['field_size']
    if rank == 1: return 1.0
    if rank == 2: return 0.75
    if rank == 3: return 0.50
    return max(0.10, (n - rank) / max(n - 4, 1) * 0.40 + 0.10)

def score_prev_rank(name, prev_history):
    h = prev_history.get(name)
    if h is None: return 0.3
    r = h['finish_rank']
    if r == 1: return 1.0
    if r <= 3: return 0.7
    if r <= 5: return 0.5
    if r <= 8: return 0.3
    return 0.1

def score_weight_chg(weight_str):
    chg = parse_weight_change(weight_str)
    if chg is None: return 0.5
    a = abs(chg)
    if a <= 2: return 1.0
    if a <= 6: return 0.7
    if a <= 10: return 0.4
    return 0.1

def score_corner(name, prev_history):
    """⑤ 前走最終コーナー位置スコア（0〜1）。先行ほど高い"""
    h = prev_history.get(name)
    if h is None: return 0.3
    lc = h.get('last_corner')
    if lc is None: return 0.3
    if lc <= 3: return 1.0
    if lc <= 6: return 0.55
    return 0.15

def compute_horse_score(h, jstats, prev_history, field_size):
    """6要素の複合スコアを計算"""
    return (W_MARKET     * score_market(h['odds'], field_size) +
            W_F3RANK     * score_f3rank(h['name'], prev_history) +
            W_JOCKEY     * score_jockey(h['jockey'], jstats) +
            W_PREV_RANK  * score_prev_rank(h['name'], prev_history) +
            W_WEIGHT_CHG * score_weight_chg(h['weight']) +
            W_CORNER     * score_corner(h['name'], prev_history))

def update_prev_history(race_horses, prev_history):
    """レース結果で前走履歴を更新（⑤ last_corner追加）"""
    valid = [(float(h['f3']), h['name']) for h in race_horses if h['f3'].strip()]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(race_horses)
    for h in race_horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            last_corner = int(nums[-1]) if nums else None
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': last_corner,
            }
        except:
            pass

# ============================================================
# データ読み込み
# ============================================================
def load_jstats(data_dir):
    result = {}
    fpath = f'{data_dir}/jstats.csv'
    try:
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                result[row['jockey']] = float(row['j_score'])
        print(f'騎手スコア: {len(result)}名読み込み')
    except FileNotFoundError:
        print(f'[WARN] {fpath} が見つかりません。騎手スコアは中立値を使用')
    return result

def load_csv_data(data_dir):
    races = {}
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    if not files:
        print(f'[ERROR] {data_dir}/raceresults_*.csv が見つかりません')
        sys.exit(1)

    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        file_ym = f'{m.group(1)}-{m.group(2)}' if m else None

        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                race_id = row['race_id']
                venue   = row['場コード']
                if venue not in JRA_VENUES:
                    continue

                ym = file_ym or (row['年'] + '-' + race_id[4:6])

                if race_id not in races:
                    races[race_id] = {
                        'horses': [],
                        'ym': ym,
                        'tansho_raw':     row.get('単勝払戻', ''),
                        'fukusho_raw':    row.get('複勝払戻', ''),
                        'umaren_raw':     row.get('馬連払戻', ''),
                        'umatan_raw':     row.get('馬単払戻', ''),
                        'wide_raw':       row.get('ワイド払戻', ''),
                        'sanrenpuku_raw': row.get('三連複払戻', ''),
                        'sanrentan_raw':  row.get('三連単払戻', ''),
                    }

                try:
                    finish_rank = int(row['着順'])
                    odds        = float(row['単勝オッズ'])
                    popularity  = int(row['人気'])
                    umaban      = row['馬番'].strip()
                except:
                    continue

                races[race_id]['horses'].append({
                    'name':        row['馬名'],
                    'umaban':      umaban,
                    'finish_rank': finish_rank,
                    'odds':        odds,
                    'popularity':  popularity,
                    'jockey':      row['騎手'],
                    'f3':          row.get('上がり3F', '').strip(),
                    'weight':      row.get('馬体重', '').strip(),
                    'corner':      row.get('通過順', '').strip(),
                })

    print(f'読み込み: {len(files)}ファイル / {len(races)}レース')
    return races

# ============================================================
# バックテスト本体
# ============================================================
def parse_payout(s):
    result = {}
    if not s or not s.strip():
        return result
    for part in s.split('|'):
        part = part.strip()
        if ':' not in part:
            continue
        key, pay = part.rsplit(':', 1)
        try:
            result[key.strip()] = int(pay.strip())
        except:
            pass
    return result

def run_backtest(races, jstats, initial_capital=70000, monthly_supplement=20000, profit_reinvest=0.70):
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = initial_capital
    prev_history = {}

    stats_kensei  = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
    stats_ana     = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
    stats_fukusho = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
    stats_wide   = {'total': 0, 'invest': 0, 'ret': 0}
    stats_umaren = {'total': 0, 'invest': 0, 'ret': 0}
    stats_umatan = {'total': 0, 'invest': 0, 'ret': 0}
    stats_3puku  = {'total': 0, 'invest': 0, 'ret': 0}
    stats_3tan   = {'total': 0, 'invest': 0, 'ret': 0}

    monthly_rows = []

    for ym in sorted(by_month.keys()):
        capital += monthly_supplement
        races_m = by_month[ym]

        # --- 穴馬選定 ---
        # ① 件数無制限  ② オッズ上限30倍  ④ 前走上がり3F上位のみ  ⑥ Kelly配分
        ana_cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if not horses: continue
            field_size = len(horses)
            if field_size < ANA_FIELD_MIN: continue  # 少頭数除外
            fav_odds   = min(h['odds'] for h in horses)
            for h in horses:
                if h['popularity'] < 4: continue
                if not (10 <= h['odds'] < ANA_ODDS_MAX): continue

                prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                if prob >= ANA_PROB_MIN:
                    ana_cands.append((prob, h, race_id, info))

        ana_cands.sort(key=lambda x: -x[0])
        seen_races = set()
        ana_selected = []
        for prob, h, race_id, info in ana_cands:
            if race_id in seen_races: continue
            seen_races.add(race_id)
            bet = ana_bet_kelly(prob, capital)  # ⑥ Kelly配分
            if bet >= 100:
                ana_selected.append((prob, h, race_id, info, bet))
            if len(ana_selected) >= ANA_COUNT_MAX: break  # ① 無制限

        # スコアリング無効化（穴馬専念）
        ken_selected = []

        # --- FUKUSHO選定（隠れ末脚型）---
        # 前走上がり3F 1位 & 前走7着以下 & 今走14〜18倍 & 6〜12番人気
        fuk_selected = []
        seen_fuk = set()
        for race_id, info in races_m:
            horses = info['horses']
            if len(horses) < FUK_FIELD_MIN: continue
            for h in horses:
                ph = prev_history.get(h['name'])
                if ph is None: continue
                if ph['f3rank'] != 1: continue
                if ph['finish_rank'] < 7: continue
                if not (FUK_ODDS_MIN <= h['odds'] <= FUK_ODDS_MAX): continue
                if not (FUK_POP_MIN <= h['popularity'] <= FUK_POP_MAX): continue
                if race_id in seen_fuk: continue
                seen_fuk.add(race_id)
                fuk_selected.append((h, race_id, info))

        # prev_history を月M分で更新（ベット選定の後）
        for race_id, info in races_m:
            update_prev_history(info['horses'], prev_history)

        m_ken_invest = m_ken_ret = m_ken_hit = 0
        m_ana_invest = m_ana_ret = m_ana_hit = 0
        m_fuk_invest = m_fuk_ret = m_fuk_hit = 0

        # --- スコアリング複勝ベット ---
        for _score, sel, race_id, info in ken_selected:
            bet = ken_bet(capital)
            capital -= bet
            m_ken_invest += bet
            stats_kensei['total']  += 1
            stats_kensei['invest'] += bet

            fukusho = parse_payout(info['fukusho_raw'])
            if sel['finish_rank'] <= 3 and sel['umaban'] in fukusho:
                payout = int(bet * fukusho[sel['umaban']] / 100)
                capital += payout
                m_ken_ret += payout
                m_ken_hit += 1
                stats_kensei['hit'] += 1
                stats_kensei['ret'] += payout

        # --- 穴馬複勝ベット ---
        for prob, h, race_id, info, bet in ana_selected:
            capital -= bet
            m_ana_invest += bet
            stats_ana['total']  += 1
            stats_ana['invest'] += bet

            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_ana_ret += payout
                m_ana_hit += 1
                stats_ana['hit'] += 1
                stats_ana['ret'] += payout

        # --- FUKUSHO複勝ベット ---
        for h, race_id, info in fuk_selected:
            bet = fuk_bet(capital)
            if bet < 100: continue
            capital -= bet
            m_fuk_invest += bet
            stats_fukusho['total']  += 1
            stats_fukusho['invest'] += bet

            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_fuk_ret += payout
                m_fuk_hit += 1
                stats_fukusho['hit'] += 1
                stats_fukusho['ret'] += payout

        # --- 全馬券種の払戻集計（全組み合わせ数を分母にした真のROI）---
        for race_id, info in races_m:
            n = len(info['horses'])
            if n < 2: continue
            n_wide   = comb(n, 2)
            n_umaren = comb(n, 2)
            n_umatan = n * (n - 1)
            n_3puku  = comb(n, 3) if n >= 3 else 0
            n_3tan   = n * (n-1) * (n-2) if n >= 3 else 0

            for raw_key, stat, n_combos in [
                ('wide_raw',       stats_wide,   n_wide),
                ('umaren_raw',     stats_umaren, n_umaren),
                ('umatan_raw',     stats_umatan, n_umatan),
                ('sanrenpuku_raw', stats_3puku,  n_3puku),
                ('sanrentan_raw',  stats_3tan,   n_3tan),
            ]:
                pt = parse_payout(info[raw_key])
                if pt and n_combos > 0:
                    stat['total']  += 1
                    stat['invest'] += n_combos * 100
                    stat['ret']    += sum(pt.values())

        # 月利益の70%を軍資金に追加投入
        m_profit = (m_ken_ret + m_ana_ret + m_fuk_ret) - (m_ken_invest + m_ana_invest + m_fuk_invest)
        reinvest = int(max(0, m_profit) * profit_reinvest / 100) * 100
        capital += reinvest

        monthly_rows.append({
            'ym':       ym,
            'k_invest': m_ken_invest, 'k_ret': m_ken_ret, 'k_hit': m_ken_hit,
            'a_invest': m_ana_invest, 'a_ret': m_ana_ret, 'a_hit': m_ana_hit,
            'f_invest': m_fuk_invest, 'f_ret': m_fuk_ret, 'f_hit': m_fuk_hit,
            'reinvest': reinvest,
            'cap':      int(capital),
        })

    return stats_kensei, stats_ana, stats_fukusho, stats_wide, stats_umaren, stats_umatan, stats_3puku, stats_3tan, monthly_rows

# ============================================================
# 出力
# ============================================================
def print_results(sk, sa, sf, sw, su, sut, s3p, s3t, monthly_rows):
    print()
    print('=' * 75)
    print('=== 全馬券種バックテスト（CSV実データ使用）===')
    print('=' * 75)
    print(f'期間: {monthly_rows[0]["ym"]} 〜 {monthly_rows[-1]["ym"]}（{len(monthly_rows)}ヶ月）')
    print()
    print(f'[設定] 穴馬オッズ上限:{ANA_ODDS_MAX}倍 頭数下限:{ANA_FIELD_MIN}頭 上限{ANA_COUNT_MAX}件 / Conservative Kelly / 穴馬専念')
    print()

    for label, s in [(f'【スコアリング】複勝（スコア{SCORE_THRESHOLD}以上・月最大4件・軍資金×6%）', sk),
                     ('【穴馬ANA】複勝（確率25%以上・オッズ10-30倍・件数無制限・Kelly配分）', sa),
                     ('【FUKUSHO】複勝（前走上がり1位&7着↓・オッズ14-18倍・6-12番人気・軍資金×2%）', sf)]:
        hr  = s['hit'] / s['total'] * 100 if s['total'] else 0
        roi = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        print(f'{label}')
        print(f'  参加:{s["total"]}件 的中:{s["hit"]}件({hr:.1f}%) 投資:¥{s["invest"]:,} 払戻:¥{s["ret"]:,} 回収率:{roi:.1f}%')
        print()

    total_invest = sk['invest'] + sa['invest'] + sf['invest']
    total_ret    = sk['ret'] + sa['ret'] + sf['ret']
    roi_total = total_ret / total_invest * 100 if total_invest else 0
    print(f'【ANA+FUKUSHO 合計】')
    print(f'  投資:¥{total_invest:,} 払戻:¥{total_ret:,} 損益:¥{total_ret-total_invest:+,} 回収率:{roi_total:.1f}%')
    print()
    print('-' * 75)
    print('【払戻データから見る全馬券種の平均回収率】（JRA全レース・実測値）')
    print()

    for label, s in [('ワイド', sw), ('馬連', su), ('馬単', sut), ('三連複', s3p), ('三連単', s3t)]:
        if s['total'] == 0: continue
        roi = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        avg_invest = s['invest'] // s['total']
        avg_ret    = s['ret']    // s['total']
        print(f'  {label:<8}: {s["total"]:,}レース 平均投資¥{avg_invest:,} 平均払戻¥{avg_ret:,} 回収率{roi:.1f}%')

    print()
    print('【月別詳細（ANA + FUKUSHO）】')
    print(f'月       | 穴馬ANA               | FUKUSHO          | 損益      | 再投入   | 軍資金')
    print('-' * 90)
    for r in monthly_rows:
        a_roi    = r['a_ret'] / r['a_invest'] * 100 if r['a_invest'] else 0
        f_roi    = r['f_ret'] / r['f_invest'] * 100 if r['f_invest'] else 0
        t_profit = (r['k_ret'] + r['a_ret'] + r['f_ret']) - (r['k_invest'] + r['a_invest'] + r['f_invest'])
        sign     = '+' if t_profit >= 0 else ''
        ri       = r.get('reinvest', 0)
        f_str    = f'{r["f_hit"]}的中/{r["f_invest"]//1000}k→{r["f_ret"]//1000}k({f_roi:.0f}%)' if r['f_invest'] else '---'
        print(f'{r["ym"]} | {r["a_hit"]}的中/{r["a_invest"]//1000}k→{r["a_ret"]//1000}k({a_roi:.0f}%) | '
              f'{f_str:<18} | {sign}¥{abs(t_profit)//1000}k | +¥{ri//1000}k | ¥{r["cap"]:,}')

if __name__ == '__main__':
    data_dir = sys.argv[1] if len(sys.argv) > 1 else 'data'
    print(f'[backtest_csv] data_dir={data_dir}')
    races  = load_csv_data(data_dir)
    jstats = load_jstats(data_dir)
    sk, sa, sf, sw, su, sut, s3p, s3t, monthly = run_backtest(races, jstats)
    print_results(sk, sa, sf, sw, su, sut, s3p, s3t, monthly)

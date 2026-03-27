"""
backtest_core.py - 穴馬・複勝スコアリング 共通バックテストエンジン
（multi_agent_optimizer.py から import して使用）
"""
import glob, csv, re, math
from collections import defaultdict
from math import comb

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

# ── 穴馬モデル定数 ──────────────────────────────────────
ODDS_TOP3 = {'10-19': 23.6, '20-49': 13.4}
FAV_ADJ   = {'1x': -1.3, '2-3': 0.0, '4-5': +1.1, '6+': +3.4}
FIELD_ADJ = {'few': +5.3, 'mid': 0.0, 'many': -4.2}
UNDERVALUED_THRESHOLD = {4: 6.0, 5: 8.0, 6: 10.0, 7: 12.0, 8: 15.0}
UNDERVALUED_BONUS = 8.0

def _get_ob(o):   return '10-19' if 10 <= o < 20 else '20-49' if 20 <= o < 50 else None
def _get_fb(fo):  return '1x' if fo < 2 else '2-3' if fo < 4 else '4-5' if fo < 6 else '6+'
def _get_fsb(fs): return 'few' if fs <= 8 else 'mid' if fs <= 12 else 'many'

def top3_prob(odds, fav_odds, field_size, popularity):
    ob = _get_ob(odds)
    if ob is None: return 0.0
    base = ODDS_TOP3[ob]
    adj  = FAV_ADJ[_get_fb(fav_odds)] + FIELD_ADJ[_get_fsb(field_size)]
    if odds < UNDERVALUED_THRESHOLD.get(popularity, 999): adj += UNDERVALUED_BONUS
    return max(0.0, min(100.0, base + adj))

# ── スコアリング関数 ─────────────────────────────────────
def _parse_weight_change(s):
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
    chg = _parse_weight_change(weight_str)
    if chg is None: return 0.5
    a = abs(chg)
    if a <= 2: return 1.0
    if a <= 6: return 0.7
    if a <= 10: return 0.4
    return 0.1

def score_corner(name, prev_history):
    h = prev_history.get(name)
    if h is None: return 0.3
    lc = h.get('last_corner')
    if lc is None: return 0.3
    if lc <= 3: return 1.0
    if lc <= 6: return 0.55
    return 0.15

def score_f3_advantage(name, prev_history):
    """前走上がり3Fの絶対時差スコア（相対値化）"""
    h = prev_history.get(name)
    if h is None or h.get('f3_time') is None or h.get('f3_best') is None: return 0.3
    gap = h['f3_time'] - h['f3_best']  # 0=最速, 正=遅れ
    if gap <= 0.0: return 1.00
    if gap <= 0.3: return 0.85
    if gap <= 0.6: return 0.65
    if gap <= 1.0: return 0.45
    if gap <= 1.5: return 0.25
    return 0.10

def compute_horse_score(h, jstats, prev_history, field_size, weights):
    W = weights
    # f3_adv が weights にあれば相対値スコアを使用（改善④）
    f3_score = (score_f3_advantage(h['name'], prev_history)
                if 'f3_adv' in W else
                score_f3rank(h['name'], prev_history))
    return (W['market']    * score_market(h['odds'], field_size) +
            W.get('f3rank', W.get('f3_adv', 0)) * f3_score +
            W['jockey']    * score_jockey(h['jockey'], jstats) +
            W['prev_rank'] * score_prev_rank(h['name'], prev_history) +
            W['weight_chg']* score_weight_chg(h['weight']) +
            W['corner']    * score_corner(h['name'], prev_history))

def parse_margin(s):
    """着差文字列を馬身数（float）に変換"""
    s = s.strip()
    if not s or s in ('同着',): return 0.0
    MAP = {'ハナ': 0.1, 'クビ': 0.2, 'アタマ': 0.3, '大': 10.0}
    if s in MAP: return MAP[s]
    s = s.replace('大', '10')
    if '.' in s and '/' in s:
        i, frac = s.split('.', 1)
        n, d = frac.split('/')
        return float(i) + float(n) / float(d)
    if '/' in s:
        n, d = s.split('/')
        return float(n) / float(d)
    try: return float(s)
    except: return 0.0

def update_prev_history(race_horses, prev_history, prev_history2=None):
    valid = [(float(h['f3']), h['name']) for h in race_horses if h['f3'].strip()]
    valid.sort()
    f3ranks  = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    f3_best  = valid[0][0] if valid else None
    f3_2nd   = valid[1][0] if len(valid) >= 2 else None  # 2位タイム
    n = len(race_horses)
    for h in race_horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            last_corner = int(nums[-1]) if nums else None
            f3t = float(h['f3']) if h['f3'].strip() else None
            # 2走前に退避
            if prev_history2 is not None and h['name'] in prev_history:
                prev_history2[h['name']] = prev_history[h['name']]
            # f3_advantage: この馬が最速だった場合の2位との差（秒）
            f3_adv = round(f3_2nd - f3_best, 2) if (f3t == f3_best and f3_2nd) else 0.0
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'f3_time':     f3t,
                'f3_best':     f3_best,
                'f3_advantage': f3_adv,  # 最速時の2位との差（秒）
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': last_corner,
                'margin':      parse_margin(h.get('margin', '')),
            }
        except:
            pass

# ── パース ────────────────────────────────────────────────
def parse_payout(s):
    result = {}
    if not s or not s.strip(): return result
    for part in s.split('|'):
        part = part.strip()
        if ':' not in part: continue
        key, pay = part.rsplit(':', 1)
        try: result[key.strip()] = int(pay.strip())
        except: pass
    return result

# ── データ読み込み ────────────────────────────────────────
def load_data(data_dir='data'):
    races  = {}
    jstats = {}

    # jstats
    try:
        with open(f'{data_dir}/jstats.csv', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                jstats[row['jockey']] = float(row['j_score'])
    except FileNotFoundError:
        pass

    # raceresults
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        file_ym = f'{m.group(1)}-{m.group(2)}' if m else None
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                race_id = row['race_id']
                if row['場コード'] not in JRA_VENUES: continue
                ym = file_ym or (row['年'] + '-' + race_id[4:6])
                if race_id not in races:
                    races[race_id] = {
                        'horses': [], 'ym': ym,
                        'fukusho_raw': row.get('複勝払戻', ''),
                    }
                try:
                    races[race_id]['horses'].append({
                        'name':        row['馬名'],
                        'umaban':      row['馬番'].strip(),
                        'finish_rank': int(row['着順']),
                        'odds':        float(row['単勝オッズ']),
                        'popularity':  int(row['人気']),
                        'jockey':      row['騎手'],
                        'f3':          row.get('上がり3F', '').strip(),
                        'weight':      row.get('馬体重', '').strip(),
                        'corner':      row.get('通過順', '').strip(),
                        'margin':      row.get('着差', '').strip(),
                    })
                except:
                    pass

    return races, jstats


def _metrics(invest, ret, count, hit, monthly_rois, final_capital):
    roi      = ret / invest * 100 if invest else 0
    hit_rate = hit / count * 100  if count  else 0
    red_months  = sum(1 for r in monthly_rois if r < 100)
    worst_month = min(monthly_rois) if monthly_rois else 0
    best_month  = max(monthly_rois) if monthly_rois else 0
    return {
        'roi': round(roi, 2),
        'invest': invest, 'ret': ret,
        'profit': ret - invest,
        'count': count, 'hit': hit,
        'hit_rate': round(hit_rate, 2),
        'red_months': red_months,
        'worst_month_roi': round(worst_month, 1),
        'best_month_roi': round(best_month, 1),
        'final_capital': final_capital,
    }


# ══════════════════════════════════════════════════════════
# 穴馬複勝バックテスト
#
# params キー:
#   odds_min, odds_max, prob_min, count_max, field_min,
#   pop_min, pop_max,
#   kelly_tiers: [(prob_thresh, pct_of_capital, max_bet), ...]  降順
# ══════════════════════════════════════════════════════════
def run_ana_backtest(races, params,
                     initial_capital=70000, monthly_supplement=20000,
                     profit_reinvest=0.70):
    p = params
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = initial_capital
    total_invest = total_ret = total_count = total_hit = 0
    monthly_rois = []

    for ym in sorted(by_month.keys()):
        capital += monthly_supplement
        races_m = by_month[ym]

        cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if len(horses) < p.get('field_min', 1): continue
            fav_odds = min(h['odds'] for h in horses)
            field_size = len(horses)
            for h in horses:
                if not (p['pop_min'] <= h['popularity'] <= p.get('pop_max', 99)): continue
                if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
                prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                if prob >= p['prob_min']:
                    cands.append((prob, h, race_id, info))

        cands.sort(key=lambda x: -x[0])
        seen = set()
        selected = []
        for prob, h, race_id, info in cands:
            if race_id in seen: continue
            seen.add(race_id)
            bet = 0
            for thresh, pct, cap_limit in p['kelly_tiers']:
                if prob >= thresh:
                    bet = min(int(capital * pct / 100) * 100, cap_limit)
                    break
            if bet >= 100:
                selected.append((prob, h, race_id, info, bet))
            if len(selected) >= p.get('count_max', 999): break

        m_inv = m_ret = m_hit = 0
        for prob, h, race_id, info, bet in selected:
            capital -= bet
            m_inv += bet
            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_ret += payout
                m_hit += 1

        # 利益の profit_reinvest 割を追加投入
        reinvest = int(max(0, m_ret - m_inv) * profit_reinvest / 100) * 100
        capital += reinvest

        total_invest += m_inv
        total_ret    += m_ret
        total_count  += len(selected)
        total_hit    += m_hit
        if m_inv > 0:
            monthly_rois.append(m_ret / m_inv * 100)

    return _metrics(total_invest, total_ret, total_count, total_hit, monthly_rois, capital)


# ══════════════════════════════════════════════════════════
# 隠れ末脚型複勝バックテスト（新戦略）
#
# シグナル:
#   前走で上がり3F上位（f3rank <= prev_f3rank_max）かつ
#   前走で大敗（finish_rank >= prev_finish_min）した馬を狙う
#   → 市場は「負けた」事実しか見ず、スピードを過小評価する
#
# params キー:
#   prev_f3rank_max      : 前走上がり3F順位の上限 (1=最速のみ, 2=2位まで, ...)
#   prev_finish_min      : 前走着順の下限 (6=6着以下のみ, ...)
#   prev_field_min       : 前走出走頭数の下限
#   prev_last_corner_min : 前走最終コーナー位置の下限 (0=無効, 6=6番手以降のみ)
#   prev_margin_max      : 前走着差の上限馬身数 (0=無効, 5=5馬身以内のみ)
#   require_2race_pattern: Trueなら2走前も同パターン必須
#   odds_min/max         : 対象オッズ帯 (EVプラス帯: 12-18倍)
#   pop_min/max          : 対象人気帯
#   field_min            : 当該レース出走頭数の下限
#   kelly_pct            : 軍資金に対するベット割合 (%)
#   kelly_max            : 1レースあたりの上限額
#   count_max            : 月あたりのベット上限件数
# ══════════════════════════════════════════════════════════
def run_fukusho_backtest(races, jstats, params,
                          initial_capital=70000, monthly_supplement=20000,
                          profit_reinvest=0.70):
    p = params
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = initial_capital
    prev_history  = {}
    prev_history2 = {}  # 2走前
    total_invest = total_ret = total_count = total_hit = 0
    monthly_rois = []

    for ym in sorted(by_month.keys()):
        capital += monthly_supplement
        races_m = by_month[ym]

        cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if len(horses) < p.get('field_min', 8): continue
            for h in horses:
                if not (p['pop_min'] <= h['popularity'] <= p.get('pop_max', 99)): continue
                if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
                ph = prev_history.get(h['name'])
                if ph is None: continue
                if ph['field_size'] < p.get('prev_field_min', 8): continue
                if ph['f3rank'] > p['prev_f3rank_max']: continue       # 前走上がり上位
                if ph['finish_rank'] < p['prev_finish_min']: continue  # 前走大敗
                # 改善①: 前走コーナー後方フィルタ
                lcm = p.get('prev_last_corner_min', 0)
                if lcm > 0:
                    lc = ph.get('last_corner')
                    if lc is None or lc < lcm: continue
                # 改善②: 前走着差フィルタ（大敗すぎを除外）
                mmx = p.get('prev_margin_max', 0)
                if mmx > 0:
                    mg = ph.get('margin', 0)
                    if mg > mmx: continue
                # 改善⑤: 前走F3圧倒的優位フィルタ（2位との差）
                f3adv_min = p.get('prev_f3_adv_min', 0.0)
                if f3adv_min > 0:
                    adv = ph.get('f3_advantage', 0.0)
                    if adv < f3adv_min: continue
                # 改善③: 2走前も同パターン必須
                if p.get('require_2race_pattern', False):
                    ph2 = prev_history2.get(h['name'])
                    if ph2 is None: continue
                    if ph2['field_size'] < p.get('prev_field_min', 8): continue
                    if ph2['f3rank'] > p['prev_f3rank_max']: continue
                    if ph2['finish_rank'] < p.get('prev2_finish_min', p['prev_finish_min']): continue
                # オッズ降順（高配当優先）
                cands.append((h['odds'], h, race_id, info))

        cands.sort(key=lambda x: -x[0])
        seen = set()
        selected = []
        for odds_val, h, race_id, info in cands:
            if race_id in seen: continue
            seen.add(race_id)
            bet = min(int(capital * p['kelly_pct'] / 100) * 100, p['kelly_max'])
            if bet >= 100:
                selected.append((h, race_id, info, bet))
            if len(selected) >= p.get('count_max', 999): break

        m_inv = m_ret = m_hit = 0
        for h, race_id, info, bet in selected:
            capital -= bet
            m_inv += bet
            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_ret += payout
                m_hit += 1

        # prev_history 更新（ベット後、2走前も保持）
        for race_id, info in races_m:
            update_prev_history(info['horses'], prev_history, prev_history2)

        reinvest = int(max(0, m_ret - m_inv) * profit_reinvest / 100) * 100
        capital += reinvest

        total_invest += m_inv
        total_ret    += m_ret
        total_count  += len(selected)
        total_hit    += m_hit
        if m_inv > 0:
            monthly_rois.append(m_ret / m_inv * 100)

    return _metrics(total_invest, total_ret, total_count, total_hit, monthly_rois, capital)

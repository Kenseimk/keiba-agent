"""
strategy.py - 戦略パラメータと判定ロジックの共通定義

他スクリプトからのインポート用:
  from strategy import ANA_PARAMS, FUKUSHO_PARAMS
  from strategy import ana_candidates, fukusho_candidates, build_prev_history
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtest_core import load_data, update_prev_history, top3_prob
from collections import defaultdict

# ══════════════════════════════════════════════════════════
# 戦略パラメータ（確定版・単一定義）
# ══════════════════════════════════════════════════════════
ANA_PARAMS = {
    'odds_min': 10, 'odds_max': 30, 'prob_min': 25.0,
    'field_min': 8, 'pop_min': 4, 'pop_max': 18,
    'kelly_tiers': [[35, 0.03, 20000], [30, 0.02, 15000], [0, 0.015, 8000]],
}

FUKUSHO_PARAMS = {
    'prev_f3rank_max': 1,   # 前走上がり3F: 1位
    'prev_finish_min': 7,   # 前走着順: 7着以下
    'prev_field_min':  8,   # 前走頭数: 8頭以上
    'odds_min': 14.0,       # オッズ: 14倍以上
    'odds_max': 18.0,       #         18倍未満
    'pop_min':  6,          # 人気: 6番人気以上
    'pop_max': 12,
    'field_min': 8,         # 出走頭数: 8頭以上
    'kelly_pct': 0.030,     # Kelly: 3.0%
    'kelly_max': 15000,     # 上限: ¥15,000
}


# ══════════════════════════════════════════════════════════
# 前走履歴の構築（過去データから）
# ══════════════════════════════════════════════════════════
def build_prev_history(data_dir):
    """過去データから前走履歴辞書を構築して返す"""
    races_hist, _ = load_data(data_dir)
    by_month = defaultdict(list)
    for rid, info in races_hist.items():
        by_month[info['ym']].append((rid, info))

    prev_history, prev_history2 = {}, {}
    for ym in sorted(by_month.keys()):
        for rid, info in by_month[ym]:
            update_prev_history(info['horses'], prev_history, prev_history2)
    return prev_history, len(races_hist), sorted(by_month.keys())[-1] if by_month else ''


# ══════════════════════════════════════════════════════════
# 穴馬複勝戦略: 候補馬リストを返す
# ══════════════════════════════════════════════════════════
def ana_candidates(races_input, capital):
    """
    races_input: {race_id: [{name, odds, popularity, ...}, ...]}
    返り値: [{race_id, name, odds, pop, prob, bet}, ...] (確率降順)
    """
    p = ANA_PARAMS
    results = []
    for race_id, horses in races_input.items():
        if len(horses) < p['field_min']:
            continue
        fav_odds   = min(h['odds'] for h in horses)
        field_size = len(horses)
        for h in horses:
            if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
            if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
            prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
            if prob >= p['prob_min']:
                bet = 0
                for thresh, pct, cap_limit in p['kelly_tiers']:
                    if prob >= thresh:
                        bet = min(int(capital * pct / 100) * 100, cap_limit)
                        break
                results.append({
                    'race_id': race_id,
                    'name':    h['name'],
                    'odds':    h['odds'],
                    'pop':     h['popularity'],
                    'prob':    prob,
                    'bet':     bet,
                })
    results.sort(key=lambda x: -x['prob'])
    return results


# ══════════════════════════════════════════════════════════
# 隠れ末脚型複勝戦略: 候補馬リストを返す
# ══════════════════════════════════════════════════════════
def fukusho_candidates(races_input, prev_history, capital):
    """
    races_input: {race_id: [{name, odds, popularity, ...}, ...]}
    prev_history: build_prev_history() の返り値（辞書）
    返り値: [{race_id, name, odds, pop, prev_f3rank, prev_finish, ...}, ...] (オッズ降順)
    """
    p = FUKUSHO_PARAMS
    results = []
    for race_id, horses in races_input.items():
        if len(horses) < p['field_min']:
            continue
        for h in horses:
            if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
            if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
            ph = prev_history.get(h['name'])
            if ph is None: continue
            if ph['field_size'] < p['prev_field_min']: continue
            if ph['f3rank'] > p['prev_f3rank_max']: continue
            if ph['finish_rank'] < p['prev_finish_min']: continue
            bet = min(int(capital * p['kelly_pct'] / 100) * 100, p['kelly_max'])
            results.append({
                'race_id':     race_id,
                'name':        h['name'],
                'odds':        h['odds'],
                'pop':         h['popularity'],
                'prev_f3rank': ph['f3rank'],
                'prev_finish': ph['finish_rank'],
                'prev_corner': ph.get('last_corner', '?'),
                'prev_field':  ph['field_size'],
                'f3_adv':      ph.get('f3_advantage', 0.0),
                'bet':         bet,
            })
    results.sort(key=lambda x: -x['odds'])
    return results


# ══════════════════════════════════════════════════════════
# 単一レース用判定（judge.py向け）
# ══════════════════════════════════════════════════════════
def judge_ana_single(horses, capital):
    """単一レースの穴馬複勝判定。最良候補を返す（なければNone）"""
    p = ANA_PARAMS
    if len(horses) < p['field_min']:
        return None
    fav_odds   = min(h['odds'] for h in horses)
    field_size = len(horses)
    best = None
    for h in horses:
        if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
        if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
        prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
        if prob >= p['prob_min']:
            if best is None or prob > best['prob']:
                bet = 0
                for thresh, pct, cap_limit in p['kelly_tiers']:
                    if prob >= thresh:
                        bet = min(int(capital * pct / 100) * 100, cap_limit)
                        break
                best = {**h, 'prob': prob, 'bet': bet,
                        'fav_odds': fav_odds, 'field_size': field_size}
    return best


def judge_fukusho_single(horses, prev_history, capital):
    """単一レースの隠れ末脚型複勝判定。最良候補を返す（なければNone）"""
    p = FUKUSHO_PARAMS
    if len(horses) < p['field_min']:
        return None
    best = None
    for h in horses:
        if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
        if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
        ph = prev_history.get(h['name'])
        if ph is None: continue
        if ph['field_size'] < p['prev_field_min']: continue
        if ph['f3rank'] > p['prev_f3rank_max']: continue
        if ph['finish_rank'] < p['prev_finish_min']: continue
        bet = min(int(capital * p['kelly_pct'] / 100) * 100, p['kelly_max'])
        if best is None or h['odds'] > best['odds']:
            best = {**h, 'bet': bet,
                    'prev_f3rank': ph['f3rank'],
                    'prev_finish': ph['finish_rank'],
                    'prev_corner': ph.get('last_corner', '?'),
                    'prev_field':  ph['field_size'],
                    'f3_adv':      ph.get('f3_advantage', 0.0)}
    return best

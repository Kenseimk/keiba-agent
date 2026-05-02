# -*- coding: utf-8 -*-
"""
optimize_NAGASHI.py  条件別重み最適化
========================================
コース(芝/ダート) × 距離帯(sprint/mile/middle/long) の8セグメントごとに
USCORE_WEIGHTS を最適化し、条件別重みを出力する。

最適化指標: NAGASHI的中率 (◎と○が両方3着以内に入る確率)
サーチ対象因子: 条件に関係する8因子の重みのみ変動、他は固定

使い方:
    python optimize_NAGASHI.py
    python optimize_NAGASHI.py --rebuild   # キャッシュ再構築
"""
import os, re, csv, glob, argparse, pickle, itertools, time
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
import numpy as np

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    analyze_race_uscore, assign_marks,
    build_trainer_stats, build_jockey_stats,
    should_exclude_uscore, USCORE_WEIGHTS, calc_horse_factors,
    _dist_zone,
)

CACHE_FILE = 'nagashi_factor_cache.pkl'
TRAIN_START, TRAIN_END = '202001', '202412'
TEST_START,  TEST_END  = '202501', '202603'

# ── 条件別に可変とする因子（他はベースライン固定）──────────
# コース系
COURSE_FACTORS = ['dirt_fit', 'track_cond', 'grass_type', 'running_style']
# 距離系
DIST_FACTORS   = ['dist_fit', 'gate', 'running_style', 'prev_position']
# 共通で微調整
COMMON_FACTORS = ['ability', 'acceleration', 'jockey', 'trainer']

TUNABLE = list(dict.fromkeys(COURSE_FACTORS + DIST_FACTORS + COMMON_FACTORS))

# グリッド: コース・距離に直接関係する4因子のみ、±50%の範囲で2段階
GRID = {
    'dirt_fit':      [0.5, 1.0, 2.0],   # ダート適性 (default 1.0)
    'grass_type':    [0.5, 1.0, 2.0],   # 野芝/洋芝適性 (default 1.0)
    'dist_fit':      [0.8, 1.5, 2.5],   # 距離帯適性 (default 1.5)
    'running_style': [0.8, 1.5, 2.5],   # 脚質 (default 1.5)
}

VENUE_MAP = {
    '01':'札幌','02':'函館','03':'福島','04':'新潟','05':'東京',
    '06':'中山','07':'中京','08':'京都','09':'阪神','10':'小倉',
}


def build_cache(races, train_start, train_end, test_start, test_end):
    """全レースの因子スコアをキャッシュ"""
    print('horse_db 構築中...', flush=True)
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=train_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)

    cache = {}  # race_id -> {meta, horses: [{name, rank, factors}]}

    all_yms = sorted(set(
        info['file_ym'] for info in races.values()
        if train_start <= info['file_ym'] <= test_end
    ))

    for ym in all_yms:
        month_races = {rid: info for rid, info in races.items() if info['file_ym'] == ym}
        print(f'  {ym}: {len(month_races)}R', flush=True)

        for race_id, info in sorted(month_races.items()):
            if should_exclude_uscore(info['race_name']): continue
            if all(h['odds'] == 0.0 for h in info['horse_list']): continue

            race_obj   = make_race_info(info)
            course_raw = info.get('course', '')
            course     = 'ダート' if 'ダ' in str(course_raw) else '芝'
            dist       = info.get('dist', 1800)
            dist_zone  = _dist_zone(dist)
            venue_code = info.get('venue_code', race_id[4:6])

            horse_factors = []
            for h in info['horse_list']:
                name    = h['name']
                history = horse_db.get(name, [])
                rank    = h['rank']
                pop     = h['pop']
                odds    = h['odds']
                jockey  = h['jockey']
                gate    = h['gate_num']
                uma     = h['umaban']
                bw      = h.get('bw')
                kg      = h.get('kg')

                factors = calc_horse_factors(
                    name, jockey, odds, pop, gate,
                    history, None, None,
                    course, dist, info.get('track_cond',''), venue_code, ym,
                    trainer_stats=trainer_stats,
                    jockey_stats=jockey_stats,
                    oikiri_db=None,
                    bw_now=bw,
                    kg_now=kg,
                )

                horse_factors.append({
                    'name':    name,
                    'rank':    rank,
                    'pop':     pop,
                    'odds':    odds,
                    'umaban':  uma,
                    'factors': factors['factors'],
                })

            cache[race_id] = {
                'race_id':   race_id,
                'file_ym':   ym,
                'course':    course,
                'dist':      dist,
                'dist_zone': dist_zone,
                'venue':     VENUE_MAP.get(venue_code, venue_code),
                'n_field':   len(info['horse_list']),
                'horses':    horse_factors,
            }

        # horse_db を当月分で更新
        _add_races_to_horse_db(horse_db, {rid: races[rid] for rid in month_races}, upto_ym=None)
        for n in horse_db:
            horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)

    return cache


def score_with_weights(horses, weights, temperature=1.5, market_alpha=0.4):
    """重みを使って各馬のnorm_scoreとwin_probを計算"""
    w_sum = sum(weights.values())
    scores = []
    for h in horses:
        raw = sum(weights.get(k, USCORE_WEIGHTS.get(k, 1.0)) * v
                  for k, v in h['factors'].items())
        scores.append(raw / w_sum)

    # softmax → model_prob
    temp = temperature
    scaled = [s / temp for s in scores]
    mx = max(scaled)
    exps = [np.exp(s - mx) for s in scaled]
    total = sum(exps)
    model_probs = [e / total for e in exps]

    # market_prob (オッズの逆数を正規化)
    inv_odds = [1.0 / h['odds'] if h['odds'] > 0 else 0.0 for h in horses]
    s_inv = sum(inv_odds)
    market_probs = [v / s_inv if s_inv > 0 else 1/len(horses) for v in inv_odds]

    win_probs = [
        market_alpha * mp + (1 - market_alpha) * mdp
        for mp, mdp in zip(market_probs, model_probs)
    ]
    return win_probs


def eval_weights(segment_races, weights):
    """指定セグメントのレースで◎と○が両方3着以内の確率を評価"""
    both_hit = total = 0
    for rc in segment_races:
        horses = rc['horses']
        if len(horses) < 4: continue
        win_probs = score_with_weights(horses, weights)
        # win_prob 降順でソート → ◎=1位, ○=2位
        order = sorted(range(len(horses)), key=lambda i: -win_probs[i])
        honmei_rank = horses[order[0]]['rank']
        retan_rank  = horses[order[1]]['rank']
        total += 1
        if honmei_rank <= 3 and retan_rank <= 3:
            both_hit += 1
    return both_hit / total if total > 0 else 0.0


def segment_key(rc):
    return (rc['course'], rc['dist_zone'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rebuild', action='store_true')
    args = parser.parse_args()

    print('=== NAGASHI 条件別重み最適化 ===\n')

    # キャッシュ
    print('全CSV読み込み中...', flush=True)
    races = load_all_csv_races('data')
    print(f'{len(races):,}レース読み込み完了')

    if args.rebuild or not os.path.exists(CACHE_FILE):
        cache = build_cache(races, TRAIN_START, TRAIN_END, TEST_START, TEST_END)
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        print(f'キャッシュ保存: {CACHE_FILE}')
    else:
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        print(f'キャッシュ読み込み: {len(cache):,}レース')

    # セグメント分割（学習期間）
    segments = defaultdict(list)
    for rc in cache.values():
        if TRAIN_START <= rc['file_ym'] <= TRAIN_END:
            segments[segment_key(rc)].append(rc)

    print('\n--- セグメント別サンプル数 ---')
    for seg, rcs in sorted(segments.items()):
        print(f'  {seg[0]} {seg[1]:10s}: {len(rcs):,}R')

    # ── グリッドサーチ ──────────────────────────────
    # 4因子を一括でグリッドサーチ
    SEARCH_GROUPS = [
        ['dirt_fit', 'grass_type', 'dist_fit', 'running_style'],
    ]

    best_weights_per_seg = {}

    for seg, seg_races in sorted(segments.items()):
        if len(seg_races) < 100:
            print(f'\n{seg}: サンプル不足({len(seg_races)}R) → デフォルト使用')
            best_weights_per_seg[seg] = dict(USCORE_WEIGHTS)
            continue

        print(f'\n{seg[0]} {seg[1]} ({len(seg_races)}R) 最適化中...', flush=True)
        current_weights = dict(USCORE_WEIGHTS)
        baseline = eval_weights(seg_races, current_weights)
        print(f'  ベースライン: {baseline:.4f}')

        # 逐次グリッドサーチ
        for group in SEARCH_GROUPS:
            best_local = eval_weights(seg_races, current_weights)
            best_combo = {k: current_weights.get(k, USCORE_WEIGHTS.get(k,1.0)) for k in group}

            grid_vals = [GRID.get(k, [current_weights.get(k,1.0)]) for k in group]
            combos = list(itertools.product(*grid_vals))

            for combo in combos:
                trial = dict(current_weights)
                for k, v in zip(group, combo):
                    trial[k] = v
                score = eval_weights(seg_races, trial)
                if score > best_local:
                    best_local = score
                    best_combo = {k: v for k, v in zip(group, combo)}

            for k, v in best_combo.items():
                current_weights[k] = v

        final = eval_weights(seg_races, current_weights)
        print(f'  最適化後:    {final:.4f}  (改善: {final-baseline:+.4f})')
        best_weights_per_seg[seg] = current_weights

    # ── テスト期間で検証 ─────────────────────────────
    print('\n\n=== テスト期間検証 (202501-202603) ===')
    test_segs = defaultdict(list)
    for rc in cache.values():
        if TEST_START <= rc['file_ym'] <= TEST_END:
            test_segs[segment_key(rc)].append(rc)

    print(f'\n{"セグメント":<18} {"R数":>5} {"デフォルト":>10} {"最適化後":>10} {"改善":>8}')
    print('-' * 58)

    total_default = total_opt = total_n = 0
    for seg in sorted(test_segs.keys()):
        rcs = test_segs[seg]
        if not rcs: continue
        weights_opt = best_weights_per_seg.get(seg, USCORE_WEIGHTS)
        score_def = eval_weights(rcs, USCORE_WEIGHTS)
        score_opt = eval_weights(rcs, weights_opt)
        n = len(rcs)
        print(f'{seg[0]} {seg[1]:<12} {n:>5}  {score_def*100:>8.1f}%  {score_opt*100:>8.1f}%  {(score_opt-score_def)*100:>+7.1f}%')
        total_default += score_def * n
        total_opt     += score_opt * n
        total_n       += n

    if total_n:
        print('-' * 58)
        print(f'{"合計":<18} {total_n:>5}  {total_default/total_n*100:>8.1f}%  {total_opt/total_n*100:>8.1f}%  {(total_opt-total_default)/total_n*100:>+7.1f}%')

    # ── 結果出力 ─────────────────────────────────────
    print('\n\n=== 条件別最適重み (コピー用) ===')
    print('CONDITION_WEIGHTS = {')
    for seg, w in sorted(best_weights_per_seg.items()):
        changed = {k: v for k, v in w.items() if abs(v - USCORE_WEIGHTS.get(k,1.0)) > 0.01}
        print(f'    {str(seg)}: {{  # 変更因子: {list(changed.keys())}')
        for k, v in w.items():
            marker = '  # <--' if k in changed else ''
            print(f'        {repr(k)}: {v},{marker}')
        print('    },')
    print('}')

    with open('nagashi_condition_weights.pkl', 'wb') as f:
        pickle.dump(best_weights_per_seg, f)
    print('\n→ nagashi_condition_weights.pkl に保存しました')


if __name__ == '__main__':
    main()

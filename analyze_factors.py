# -*- coding: utf-8 -*-
"""
analyze_factors.py  因子重要度分析 (Ablation Study)
=====================================================
各因子を1つずつ除外したときにmodel_probの予測精度がどう変わるか測定。
精度が下がる因子 = 有効、精度が上がる因子 = ノイズ/有害

指標: ◎1着率 / ◎3着以内率 (model_prob順で選んだ馬)
期間: 202501〜202603 / 8-11R / 頭数<=14

実行: python analyze_factors.py
"""
import os, sys
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

import numpy as np
from collections import defaultdict

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    build_trainer_stats, build_jockey_stats, should_exclude_uscore,
    USCORE_WEIGHTS, calc_horse_factors, _softmax_probs,
)

print('=== 因子重要度分析 ===\n')

# ── データ準備 ──────────────────────────────────
print('データ読み込み中...', flush=True)
races = load_all_csv_races('data')
horse_db = defaultdict(list)
_add_races_to_horse_db(horse_db, races, upto_ym='202501')
for n in horse_db:
    horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
trainer_stats = build_trainer_stats(horse_db)
jockey_stats  = build_jockey_stats(horse_db)
print(f'horse_db: {len(horse_db):,}頭\n')

# ── テスト対象レースの因子キャッシュ構築 ──────────
print('因子キャッシュ構築中...', flush=True)
from uscore import _dist_zone

VENUE_MAP = {
    '01':'札幌','02':'函館','03':'福島','04':'新潟','05':'東京',
    '06':'中山','07':'中京','08':'京都','09':'阪神','10':'小倉',
}

cached = []  # [{horses: [{name, rank, factors}]}]

test_rids = sorted(rid for rid, info in races.items()
                   if '202501' <= info['file_ym'] <= '202603')

for race_id in test_rids:
    info = races[race_id]
    if should_exclude_uscore(info['race_name']): continue
    if all(h['odds'] == 0.0 for h in info['horse_list']): continue
    rnum = int(race_id[-2:])
    if not (8 <= rnum <= 11): continue
    if info['n_field'] > 14: continue

    course_raw = info.get('course', '')
    course     = 'ダート' if 'ダ' in str(course_raw) else '芝'
    dist       = info.get('dist', 1800)
    ym         = info['file_ym']
    venue_code = info.get('venue_code', race_id[4:6])

    horse_rows = []
    for h in info['horse_list']:
        name = h['name']
        try:
            fdata = calc_horse_factors(
                name, h['jockey'], h['odds'], h['pop'], h['gate_num'],
                horse_db.get(name, []), None, None,
                course, dist, info.get('track_cond',''), venue_code, ym,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
                oikiri_db=None,
                bw_now=h.get('bw'), kg_now=h.get('kg'),
            )
        except:
            continue
        horse_rows.append({
            'name':    name,
            'rank':    h['rank'],
            'odds':    h['odds'],
            'factors': fdata['factors'],
        })

    if len(horse_rows) < 4:
        continue
    cached.append(horse_rows)

print(f'対象レース: {len(cached)}R\n')


# ── スコアリング関数 ──────────────────────────────
def score_races(cached, weights, temperature=1.5):
    """各レースでmodel_prob順に◎を選び、的中率を返す"""
    w_sum = sum(weights.values()) or 1.0
    top1_win = top1_p3 = top2_p3 = 0

    for horses in cached:
        raw_scores = []
        for h in horses:
            raw = sum(weights.get(k, 0.0) * v for k, v in h['factors'].items())
            raw_scores.append(raw / w_sum)

        probs = _softmax_probs(raw_scores, temperature)
        order = sorted(range(len(horses)), key=lambda i: -probs[i])

        r1 = horses[order[0]]['rank']
        r2 = horses[order[1]]['rank'] if len(order) > 1 else 99

        if r1 == 1: top1_win += 1
        if r1 <= 3: top1_p3 += 1
        if r2 <= 3: top2_p3 += 1

    n = len(cached)
    return top1_win/n*100, top1_p3/n*100, top2_p3/n*100


# ── ベースライン ──────────────────────────────────
base_win, base_p3, base_p3_2 = score_races(cached, USCORE_WEIGHTS)
print(f'ベースライン: ◎1着率={base_win:.1f}%  ◎3着内={base_p3:.1f}%  ○3着内={base_p3_2:.1f}%\n')

# ── 各因子をゼロにした場合の精度 ──────────────────
print(f'{"因子":<20} {"重み":>5}  {"◎1着率":>7} {"Δ1着":>7}  {"◎3着内":>7} {"Δ3着":>7}  {"評価":>6}')
print('-' * 72)

results = []
for factor in sorted(USCORE_WEIGHTS.keys()):
    w = dict(USCORE_WEIGHTS)
    w[factor] = 0.0
    win, p3, p3_2 = score_races(cached, w)
    d_win = win - base_win
    d_p3  = p3  - base_p3
    # 除外したら精度が下がる(d<0) = 有効な因子
    # 除外したら精度が上がる(d>0) = ノイズ/有害な因子
    if d_p3 < -0.5:
        verdict = '★有効'
    elif d_p3 > 0.5:
        verdict = '✗ノイズ'
    else:
        verdict = '－中立'
    results.append((factor, USCORE_WEIGHTS[factor], win, d_win, p3, d_p3, verdict))

# ◎3着内への影響でソート（有効な因子を上に）
results.sort(key=lambda x: x[5])

for factor, weight, win, d_win, p3, d_p3, verdict in results:
    print(f'{factor:<20} {weight:>5.1f}  {win:>6.1f}% {d_win:>+6.1f}%  {p3:>6.1f}% {d_p3:>+6.1f}%  {verdict}')

print()
print('※ 除外時に精度が下がる(Δ<0) = 有効な因子')
print('※ 除外時に精度が上がる(Δ>0) = ノイズ・除去候補')

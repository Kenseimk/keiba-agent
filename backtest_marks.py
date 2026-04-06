# -*- coding: utf-8 -*-
"""
backtest_marks.py  印（◎○▲☆△）別・馬券パターン別 的中率バックテスト

PDFの馬券構築パターン:
  A: 単勝       ◎
  B: ワイド     ◎→○▲
  C: 三連単マルチ  ◎○→▲△☆  /  ◎▲→△☆
  D: 三連単マルチ  ◎○→▲△☆  +  ◎▲→△☆  +  ◎→△☆

印の割り当て (U_score 降順):
  ◎=1位  ○=2位  ▲=3位  ☆=4位  △=5〜8位
"""

import os, sys, re, glob, csv
from collections import defaultdict

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from uscore_backtest import (
    load_all_csv_races, make_race_info,
    _add_races_to_horse_db, _build_race_records,
)
from uscore import analyze_race_uscore, should_exclude_uscore, _float, _int

DATA_DIR     = 'data'
TEST_START   = '202501'
TEST_END     = '202603'
N_DELTA      = 4   # △の頭数（5位〜8位）


# ══════════════════════════════════════════════════════
# 印割り当て
# ══════════════════════════════════════════════════════

def assign_marks(results: list) -> dict:
    """
    U_score 降順の results から馬名→印 の辞書を返す。
    ◎=1位, ○=2位, ▲=3位, ☆=4位, △=5〜(4+N_DELTA)位
    """
    marks = {}
    for i, r in enumerate(results):
        name = r['name']
        if i == 0:
            marks[name] = '◎'
        elif i == 1:
            marks[name] = '○'
        elif i == 2:
            marks[name] = '▲'
        elif i == 3:
            marks[name] = '☆'
        elif i < 4 + N_DELTA:
            marks[name] = '△'
        else:
            marks[name] = ''
    return marks


# ══════════════════════════════════════════════════════
# 的中判定
# ══════════════════════════════════════════════════════

def _names_with_mark(marks: dict, *mark_list) -> set:
    return {n for n, m in marks.items() if m in mark_list}


def check_hit(marks: dict, actual_1: str, actual_2: str, actual_3: str) -> dict:
    """
    各パターンの的中判定。
    actual_1/2/3: 実際の1着/2着/3着馬名
    戻り値: {pattern: bool}
    """
    honmei  = _names_with_mark(marks, '◎')
    rentan  = _names_with_mark(marks, '◎', '○')
    sante   = _names_with_mark(marks, '▲', '☆', '△')
    hosi    = _names_with_mark(marks, '☆', '△')
    sankaku = _names_with_mark(marks, '△')
    hosi_only = _names_with_mark(marks, '☆')

    top3 = {actual_1, actual_2, actual_3}

    # パターンA: 単勝 ◎
    hit_a = actual_1 in honmei

    # パターンB: ワイド ◎→○▲  (◎と○▲のどちらかが2着以内に同時に含まれる)
    ob_pair = _names_with_mark(marks, '○', '▲')
    top2    = {actual_1, actual_2}
    hit_b   = bool(honmei & top2) and bool(ob_pair & top2)

    # パターンC-1: 三連単マルチ ◎○→▲△☆
    # ◎と○が1-2着を独占し、3着が▲△☆のいずれか
    c1_axis = rentan   # {◎, ○}
    hit_c1  = ({actual_1, actual_2} == c1_axis and actual_3 in sante)

    # パターンC-2: 三連単マルチ ◎▲→△☆
    c2_axis = _names_with_mark(marks, '◎', '▲')
    hit_c2  = ({actual_1, actual_2} == c2_axis and actual_3 in hosi)

    hit_c   = hit_c1 or hit_c2

    # パターンD: C + ◎→△☆ (◎が1着, △☆が2-3着)
    # ◎→△☆: ◎が1着、2着と3着がともに△か☆
    hit_d3  = (actual_1 in honmei
               and actual_2 in hosi
               and actual_3 in hosi
               and actual_2 != actual_3)
    hit_d   = hit_c or hit_d3

    return {
        'A':  hit_a,
        'B':  hit_b,
        'C1': hit_c1,
        'C2': hit_c2,
        'C':  hit_c,
        'D':  hit_d,
    }


# ══════════════════════════════════════════════════════
# バックテスト本体
# ══════════════════════════════════════════════════════

def run_marks_backtest():
    print('=== 印・馬券パターン別 的中率バックテスト ===')
    print(f'テスト期間: {TEST_START} 〜 {TEST_END}')
    print(f'△頭数: {N_DELTA}  (◎○▲☆△1〜△{N_DELTA})\n')

    races = load_all_csv_races(DATA_DIR)
    print(f'全レース: {len(races):,}')

    test_months = sorted(set(
        info['file_ym'] for info in races.values()
        if info['file_ym'] and TEST_START <= info['file_ym'] <= TEST_END
    ))

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=TEST_START)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'初期 horse_db: {len(horse_db):,} 頭\n')

    # 月別集計
    counters = defaultdict(lambda: {'races': 0, 'hits': defaultdict(int)})

    all_details = []   # verbose 用

    for ym in test_months:
        month_races = {rid: info for rid, info in races.items() if info['file_ym'] == ym}

        for race_id in sorted(month_races.keys()):
            info = month_races[race_id]
            if should_exclude_uscore(info['race_name']):
                continue
            if all(h['odds'] == 0.0 for h in info['horse_list']):
                continue

            race_info_obj = make_race_info(info)
            try:
                results = analyze_race_uscore(race_info_obj, horse_db, None, None)
            except Exception:
                continue
            if not results:
                continue

            # 実際の着順
            rank_map = {h['name']: h['rank'] for h in info['horse_list']}
            placed = sorted(
                [(name, rank) for name, rank in rank_map.items() if rank in (1, 2, 3)],
                key=lambda x: x[1]
            )
            if len(placed) < 3:
                continue
            act1, act2, act3 = placed[0][0], placed[1][0], placed[2][0]

            # 印は win_prob 降順で割り当て（U_score ではなく勝率ベース）
            results_by_winprob = sorted(results, key=lambda x: x['win_prob'], reverse=True)
            marks = assign_marks(results_by_winprob)
            hits  = check_hit(marks, act1, act2, act3)

            counters[ym]['races'] += 1
            for pat, h in hits.items():
                if h:
                    counters[ym]['hits'][pat] += 1

            all_details.append({
                'race_id': race_id,
                'race_name': info['race_name'],
                'ym': ym,
                'marks': marks,
                'actual': (act1, act2, act3),
                'hits': hits,
                'results': results,
            })

        # この月のデータを horse_db に追加
        for race_id, info in month_races.items():
            for name, rec in _build_race_records(race_id, info).items():
                horse_db.setdefault(name, []).insert(0, rec)

    # ══════════════════════════════════════════════════
    # 集計
    # ══════════════════════════════════════════════════
    patterns = ['A', 'B', 'C1', 'C2', 'C', 'D']
    total_races = sum(v['races'] for v in counters.values())

    print('=' * 75)
    print(f"{'月':>8}  {'R数':>5}  {'A単勝':>7}  {'Bワイド':>7}  {'CC1':>7}  {'CC2':>7}  {'C合計':>7}  {'D合計':>7}")
    print('-' * 75)

    for ym in test_months:
        c = counters[ym]
        n = c['races']
        if n == 0:
            continue
        row = f"  {ym}  {n:5d}"
        for pat in patterns:
            h = c['hits'][pat]
            row += f"  {h:3d}/{n:3d}"
        print(row)

    print('-' * 75)

    # 合計行
    total_hits = defaultdict(int)
    for c in counters.values():
        for pat, h in c['hits'].items():
            total_hits[pat] += h

    row = f"  {'合計':>8}  {total_races:5d}"
    for pat in patterns:
        h = total_hits[pat]
        row += f"  {h:3d}/{total_races:3d}"
    print(row)
    print('=' * 75)

    # 的中率サマリ
    print()
    print('── 的中率サマリ ──')
    descs = {
        'A':  '単勝 ◎',
        'B':  'ワイド ◎→○▲',
        'C1': '三連単マルチ ◎○→▲△☆',
        'C2': '三連単マルチ ◎▲→△☆',
        'C':  'パターンC (C1+C2)',
        'D':  'パターンD (C+◎→△☆)',
    }
    for pat in patterns:
        h = total_hits[pat]
        rate = h / total_races * 100 if total_races > 0 else 0
        print(f"  {descs[pat]:25s}: {h:4d}/{total_races:4d} = {rate:5.1f}%")

    # ── 印の精度チェック ──────────────────────────────
    print()
    print('── 印別「3着以内率」──')
    mark_in3 = defaultdict(lambda: {'total': 0, 'in3': 0})
    for d in all_details:
        top3 = set(d['actual'])
        for name, mark in d['marks'].items():
            if mark:
                mark_in3[mark]['total'] += 1
                if name in top3:
                    mark_in3[mark]['in3'] += 1

    for mark in ['◎', '○', '▲', '☆', '△']:
        t = mark_in3[mark]['total']
        i = mark_in3[mark]['in3']
        r = i / t * 100 if t > 0 else 0
        print(f"  {mark}: {i:4d}/{t:4d} = {r:5.1f}%  (期待値: {'◎'==mark and '33%' or '○'==mark and '33%' or '▲'==mark and '33%' or '20%'})")


if __name__ == '__main__':
    run_marks_backtest()

# -*- coding: utf-8 -*-
"""
analyze_axis_prob.py — ◎が3着以内・1着に入る確率の傾向分析

実行:
  python analyze_axis_prob.py              # チューニング期間のみ
  python analyze_axis_prob.py --blind      # チューニング→ブラインド検証
  python analyze_axis_prob.py --threshold 0.42  # フィルタ閾値指定
"""
import os, math, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'
from collections import defaultdict

from backtest_GEKIUMA_TENKAI import (
    _run_core, load_sanrenpuku,
    build_jockey_style_db, build_course_pace_db, build_cond_pace_db,
    TUNE_START, TUNE_END, BLIND_START, BLIND_END,
)
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db

# ── ベストパラメータ (前回チューニング結果) ──
BEST_PARAMS = dict(
    min_records=3, recency_decay=0.85,
    speed_adv_w=0.45, pace_fit_w=0.25,
    min_pop_rank=1, n_select=6,
    bet_type='sanrenpuku',
)

FEATURES = [
    'axis_score',        # ◎の絶対スコア
    'gap_1_2',           # ◎-○スコア差 (1着優位)
    'gap_1_3',           # ◎-▲スコア差 (3着内余裕)
    'score_share',       # ◎スコアシェア (フィールド支配率)
    'field_std',         # 全馬スコアstd (低=拮抗)
    'pace_score',        # 展開スコア (高=ハイ)
    'axis_pace_fit',     # ◎の展開適性
    'same_style_n',      # 同脚質競合数
    'n_nige_adj',        # 逃げ馬数 (補正済み)
    'pace_style_match',  # 展開×脚質一致 (0/1)
    'competition_score', # 競合ペナルティ値
    'n_field',           # 出走頭数
]

FEATURE_LABELS = {
    'axis_score':        '◎スコア',
    'gap_1_2':           '◎-○差 (1着余裕)',
    'gap_1_3':           '◎-▲差 (3着内余裕)',
    'score_share':       'スコアシェア',
    'field_std':         'フィールドstd (低=拮抗)',
    'pace_score':        '展開スコア (高=ハイ)',
    'axis_pace_fit':     '◎展開適性',
    'same_style_n':      '同脚質競合数',
    'n_nige_adj':        '逃げ馬数',
    'pace_style_match':  '展開×脚質一致',
    'competition_score': '競合ペナルティ',
    'n_field':           '出走頭数',
}


# ══════════════════════════════════════════════════════
# 分位ビン分析
# ══════════════════════════════════════════════════════

def bin_analysis(records, feature, n_bins=7, label=None):
    vals = sorted(set(r[feature] for r in records))
    if len(vals) <= n_bins:
        # 離散値はそのまま集計
        buckets = {v: [r for r in records if r[feature] == v] for v in vals}
        ranges  = [(v, v, bk) for v, bk in sorted(buckets.items())]
    else:
        # 分位数でビン分割
        idxs = [int(i * (len(vals)-1) / n_bins) for i in range(n_bins+1)]
        thresholds = sorted(set(vals[i] for i in idxs))
        ranges = []
        for i in range(len(thresholds)-1):
            lo, hi = thresholds[i], thresholds[i+1]
            if i < len(thresholds)-2:
                bk = [r for r in records if lo <= r[feature] < hi]
            else:
                bk = [r for r in records if lo <= r[feature]]
            ranges.append((lo, hi, bk))

    lbl = label or FEATURE_LABELS.get(feature, feature)
    print(f'\n--- {lbl} ---')
    print(f'  {"範囲":>18}  {"n":>5}  {"3着内率":>8}  {"1着率":>7}  bar')
    print('  ' + '-' * 62)
    for lo, hi, bk in ranges:
        if not bk:
            continue
        n  = len(bk)
        p3 = sum(r['hit_place'] for r in bk) / n * 100
        p1 = sum(r['hit_win']   for r in bk) / n * 100
        bar = '#' * int(p3 / 2)
        if lo == hi:
            rng = f'{lo:>18.3g}'
        else:
            rng = f'{lo:>8.3f}~{hi:<8.3f}'
        print(f'  {rng}  {n:>5}  {p3:>7.1f}%  {p1:>6.1f}%  {bar}')


def cross_pace_style(records):
    """展開ラベル × ◎脚質 の交差テーブル"""
    print(f'\n--- 展開×◎脚質 3着内率 ---')
    pace_order  = ["スロー", "ミドルスロー", "ミドル", "ミドルハイ", "ハイ", "超ハイ"]
    style_order = ["逃げ", "先行", "差し", "追い込み"]
    # header
    header = f'  {"展開":>10}' + ''.join(f'  {s:>8}' for s in style_order) + f'  {"合計":>8}'
    print(header)
    print('  ' + '-' * (len(header) - 2))
    for pace in pace_order:
        row = f'  {pace:>10}'
        pr  = [r for r in records if r['pace_label'] == pace]
        if not pr:
            continue
        for style in style_order:
            bk = [r for r in pr if r['axis_style'] == style]
            if len(bk) < 5:
                row += f'  {"  -":>8}'
            else:
                p3 = sum(r['hit_place'] for r in bk) / len(bk) * 100
                row += f'  {p3:>7.1f}%'
        tot = len(pr)
        p3t = sum(r['hit_place'] for r in pr) / tot * 100
        row += f'  {p3t:>7.1f}%'
        print(row)


# ══════════════════════════════════════════════════════
# ロジスティック回帰 (pure Python)
# ══════════════════════════════════════════════════════

def _sig(x):
    x = max(-30.0, min(30.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def logistic_fit(records, target, lr=0.05, epochs=3000, l2=0.01):
    """特徴量 → target(0/1) のロジスティック回帰。正規化込み。"""
    X = [[r[f] for f in FEATURES] for r in records]
    y = [r[target] for r in records]
    n, d = len(X), len(X[0])

    means = [sum(X[i][j] for i in range(n)) / n for j in range(d)]
    stds  = [math.sqrt(sum((X[i][j]-means[j])**2 for i in range(n))/n) or 1.0
             for j in range(d)]
    Xn = [[(X[i][j]-means[j])/stds[j] for j in range(d)] for i in range(n)]

    w = [0.0]*d; b = 0.0
    for _ in range(epochs):
        dw = [0.0]*d; db = 0.0
        for i in range(n):
            z   = sum(w[j]*Xn[i][j] for j in range(d)) + b
            err = _sig(z) - y[i]
            for j in range(d):
                dw[j] += err * Xn[i][j]
            db += err
        for j in range(d):
            w[j] -= lr * (dw[j]/n + l2*w[j])
        b -= lr * db/n

    return w, b, means, stds


def predict_prob(record, w, b, means, stds):
    x = [record[f] for f in FEATURES]
    z = sum(w[j]*(x[j]-means[j])/stds[j] for j in range(len(w))) + b
    return _sig(z)


def calibration_table(records, w, b, means, stds, target, step=0.05, label=''):
    probs = [predict_prob(r, w, b, means, stds) for r in records]
    thresholds = [round(i*step, 3) for i in range(int(0.15/step), int(0.85/step)+1)]
    print(f'\n--- キャリブレーション ({label}) ---')
    print(f'  {"推定P":>10}  {"実績3着内":>9}  {"n":>5}  bar')
    print('  ' + '-' * 48)
    for lo in thresholds:
        hi = round(lo + step, 3)
        idxs = [i for i, p in enumerate(probs) if lo <= p < hi]
        if len(idxs) < 10:
            continue
        actual = sum(records[i][target] for i in idxs) / len(idxs) * 100
        bar    = '#' * int(actual / 2)
        print(f'  {lo:.2f}~{hi:.2f}      {actual:>8.1f}%  {len(idxs):>5}  {bar}')
    return probs


def filter_analysis(records, probs, thresholds):
    """確率閾値フィルタリング後の的中率・件数"""
    print(f'\n--- 確率閾値フィルタリング効果 ---')
    print(f'  {"閾値":>8}  {"n":>6}  {"3着内率":>8}  {"1着率":>7}  {"絞込率":>7}')
    print('  ' + '-' * 52)
    N = len(records)
    for thr in thresholds:
        idxs = [i for i, p in enumerate(probs) if p >= thr]
        if not idxs:
            continue
        n  = len(idxs)
        p3 = sum(records[i]['hit_place'] for i in idxs) / n * 100
        p1 = sum(records[i]['hit_win']   for i in idxs) / n * 100
        sel_rate = n / N * 100
        print(f'  P>={thr:.2f}  {n:>6}  {p3:>7.1f}%  {p1:>6.1f}%  {sel_rate:>6.1f}%')


# ══════════════════════════════════════════════════════
# データ収集ユーティリティ
# ══════════════════════════════════════════════════════

def collect(races, start, end, walkforward):
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)

    jdb   = build_jockey_style_db(horse_db)
    cpdb  = build_course_pace_db(races, start)
    cdb   = build_cond_pace_db(races, start)
    pdb   = load_sanrenpuku('data', start, end)

    _, _, feats = _run_core(
        races, horse_db, pdb, start, end,
        **BEST_PARAMS,
        jockey_style_db=jdb, course_pace_db=cpdb, cond_pace_db=cdb,
        collect_features=True, walkforward=walkforward,
    )
    return feats


# ══════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════

def predict_csv(feat_records, w_p, b_p, mu_p, sd_p,
                w_w, b_w, mu_w, sd_w, out_path):
    """全レースの確率をCSVに書き出す"""
    import csv
    rows = []
    for r in feat_records:
        p_place = predict_prob(r, w_p, b_p, mu_p, sd_p)
        p_win   = predict_prob(r, w_w, b_w, mu_w, sd_w)
        rows.append({
            'race_id':      r['race_id'],
            'axis_name':    r['axis_name'],
            'axis_pop':     r.get('axis_pop', ''),
            'axis_style':   r.get('axis_style', ''),
            'pace_label':   r.get('pace_label', ''),
            'n_field':      r['n_field'],
            'axis_score':   f"{r['axis_score']:.4f}",
            'gap_1_3':      f"{r['gap_1_3']:.4f}",
            'axis_pace_fit':f"{r['axis_pace_fit']:.4f}",
            'pace_style_match': r['pace_style_match'],
            'p_win':        f"{p_win:.4f}",
            'p_place':      f"{p_place:.4f}",
            'actual_rank':  r.get('actual_rank', ''),
            'hit_win':      r['hit_win'],
            'hit_place':    r['hit_place'],
        })
    # 3着内確率の高い順にソート
    rows.sort(key=lambda x: float(x['p_place']), reverse=True)

    fieldnames = ['race_id','axis_name','axis_pop','axis_style','pace_label',
                  'n_field','axis_score','gap_1_3','axis_pace_fit','pace_style_match',
                  'p_win','p_place','actual_rank','hit_win','hit_place']
    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f'CSV出力: {out_path}  ({len(rows):,}レース)')

    # コンソールにも上位30件を表示
    print(f'\n{"="*90}')
    print(f'◎ 確率ランキング (p_place上位30件)')
    print(f'{"="*90}')
    print(f'  {"race_id":>16}  {"◎":>14}  {"人気":>4}  {"脚質":>6}  {"展開":>8}  '
          f'{"頭":>3}  {"P(1着)":>7}  {"P(3着内)":>9}  {"実着順":>6}  {"結果"}')
    print('  ' + '-' * 88)
    for i, r in enumerate(rows[:30]):
        result = '1着' if r['hit_win'] else ('3着内' if r['hit_place'] else '着外')
        mark   = '◎' if r['hit_win'] else ('○' if r['hit_place'] else '✗')
        print(f'  {r["race_id"]:>16}  {r["axis_name"]:>14}  {str(r["axis_pop"])+"番人":>6}  '
              f'{r["axis_style"]:>6}  {r["pace_label"]:>8}  {r["n_field"]:>3}頭  '
              f'{float(r["p_win"])*100:>6.1f}%  {float(r["p_place"])*100:>8.1f}%  '
              f'{str(r["actual_rank"])+"着":>6}  {mark}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--blind',     action='store_true', help='ブラインド期間でも検証')
    parser.add_argument('--predict',   action='store_true', help='全レース確率をCSV出力')
    parser.add_argument('--start',     default=None, help='予測対象開始年月 (例: 202501)')
    parser.add_argument('--end',       default=None, help='予測対象終了年月 (例: 202603)')
    parser.add_argument('--out',       default='axis_prob.csv', help='CSV出力先')
    parser.add_argument('--threshold', type=float, default=None,
                        help='フィルタ閾値 (デフォルト: 0.30〜0.55を自動スキャン)')
    args = parser.parse_args()

    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R')

    # ── チューニング期間で特徴量収集 ──
    print(f'\n【チューニング {TUNE_START}〜{TUNE_END}】特徴量収集中...')
    feat_tune = collect(races, TUNE_START, TUNE_END, walkforward=False)
    n_tune = len(feat_tune)
    p3_base = sum(r['hit_place'] for r in feat_tune) / n_tune * 100
    p1_base = sum(r['hit_win']   for r in feat_tune) / n_tune * 100
    print(f'収集: {n_tune:,}R  全体3着内率: {p3_base:.1f}%  1着率: {p1_base:.1f}%')

    # ── 各特徴量の単変量分析 ──
    print(f'\n{"="*68}')
    print('各特徴量別 的中率 (チューニング期間)')
    print(f'{"="*68}')
    for feat in FEATURES:
        bin_analysis(feat_tune, feat)

    # ── 展開×脚質クロス分析 ──
    print(f'\n{"="*68}')
    print('展開×脚質 クロス分析')
    print(f'{"="*68}')
    cross_pace_style(feat_tune)

    # ── ロジスティック回帰 ──
    print(f'\n{"="*68}')
    print('ロジスティック回帰 学習中...')
    print(f'{"="*68}')
    print('  3着内確率モデル ...')
    w_p, b_p, mu_p, sd_p = logistic_fit(feat_tune, 'hit_place')
    print('  1着確率モデル ...')
    w_w, b_w, mu_w, sd_w = logistic_fit(feat_tune, 'hit_win')

    # 係数 (標準化済み → 重要度として解釈可)
    print(f'\n--- 特徴量重要度 (標準化済み係数) ---')
    print(f'  {"特徴量":>20}  {"3着内係数":>10}  {"1着係数":>10}')
    print('  ' + '-' * 46)
    pairs = sorted(zip(FEATURES, w_p, w_w), key=lambda x: abs(x[1]), reverse=True)
    for fname, wp, ww in pairs:
        lbl = FEATURE_LABELS.get(fname, fname)
        print(f'  {lbl:>20}  {wp:>+10.4f}  {ww:>+10.4f}')

    # チューニング期間キャリブレーション
    probs_tune = calibration_table(feat_tune, w_p, b_p, mu_p, sd_p,
                                   'hit_place', label='チューニング')
    thr_list = [0.30, 0.35, 0.38, 0.40, 0.42, 0.45, 0.48, 0.50]
    if args.threshold:
        thr_list = [args.threshold]
    filter_analysis(feat_tune, probs_tune, thr_list)

    # ── 全レース確率CSV出力 ──
    if args.predict:
        pred_start = args.start or BLIND_START
        pred_end   = args.end   or BLIND_END
        print(f'\n{"="*68}')
        print(f'【予測出力】{pred_start}〜{pred_end}  全レース確率を計算中...')
        print(f'{"="*68}')
        feat_pred = collect(races, pred_start, pred_end, walkforward=True)
        predict_csv(feat_pred, w_p, b_p, mu_p, sd_p,
                              w_w, b_w, mu_w, sd_w, args.out)
        return

    # ── ブラインド検証 ──
    if args.blind:
        print(f'\n{"="*68}')
        print(f'【ブラインド {BLIND_START}〜{BLIND_END}】検証')
        print(f'{"="*68}')
        print('特徴量収集中...')
        feat_blind = collect(races, BLIND_START, BLIND_END, walkforward=True)
        n_blind = len(feat_blind)
        p3b = sum(r['hit_place'] for r in feat_blind) / n_blind * 100
        p1b = sum(r['hit_win']   for r in feat_blind) / n_blind * 100
        print(f'収集: {n_blind:,}R  全体3着内率: {p3b:.1f}%  1着率: {p1b:.1f}%')

        probs_blind = calibration_table(feat_blind, w_p, b_p, mu_p, sd_p,
                                        'hit_place', label='ブラインド')
        filter_analysis(feat_blind, probs_blind, thr_list)

        # ブラインド期間の展開×脚質
        print(f'\n展開×脚質 クロス (ブラインド)')
        cross_pace_style(feat_blind)


if __name__ == '__main__':
    main()

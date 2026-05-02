"""
walkforward_winprob.py  勝率モデル (ロジスティック回帰) ウォークフォワード検証
=================================================================================
各馬の因子スコア（近走着順・上がり・騎手・コース適性 etc.）を特徴量として
ロジスティック回帰で P(1着) を予測する。確率は馬ごと独立（合計100%にはならない）。

フェーズ:
  Phase 0: train_start 以前データで初期 horse_db 構築
  Phase 1: train 期間 (2022-2023) で (因子ベクトル, 勝敗) を収集 → モデル学習
  Phase 2: test 期間 (2024-) でウォークフォワード評価
  モデルは data/winprob_model.json に保存

使い方:
  python walkforward_winprob.py                        # デフォルト
  python walkforward_winprob.py --test-end 202412
  python walkforward_winprob.py --load-only            # 学習済みモデルで評価のみ
"""

import os, json, math, argparse
from collections import defaultdict

import numpy as np

from score_agent_core import (
    load_models, compute_horse_factors, LOGISTIC_FACTORS,
    _float, _int, parse_avg_pos, parse_bw_change, parse_margin,
    parse_time, odds_fallback_score,
)
from backtest_agent import load_all_races, parse_tansho


MODEL_PATH = 'data/winprob_model.json'


# ─── horse_db ──────────────────────────────────────────

def _make_record(race_id: str, race_info: dict, h: dict) -> dict:
    rank       = _int(h.get('着順'))
    margin_raw = h.get('着差', '').strip()
    return {
        'race_id':     race_id,
        'race_ym':     race_id[:6],
        'venue_code':  race_id[4:6],
        'rank':        rank,
        'field_size':  race_info['n_field'],
        'agari_rank':  race_info['agari_rank_map'].get(h['馬名'], -1),
        'agari_field': race_info['n_agari'],
        'avg_pos':     parse_avg_pos(h.get('通過順', '')),
        'bw_chg':      parse_bw_change(h.get('馬体重', '')),
        'dist':        _int(h.get('距離')),
        'track_cond':  h.get('馬場状態', '').strip(),
        'margin':      parse_margin(margin_raw, rank) if rank else 0.0,
        'race_time':   parse_time(h.get('タイム', '')),
    }


def build_db_upto(all_races: dict, ym_excl: str) -> defaultdict:
    db = defaultdict(list)
    for race_id in sorted(all_races):
        if race_id[:6] >= ym_excl:
            continue
        for h in all_races[race_id]['horses']:
            if _int(h.get('着順')) is None:
                continue
            db[h['馬名']].append(_make_record(race_id, all_races[race_id], h))
    return db


def add_month_to_db(db: defaultdict, all_races: dict, ym: str) -> None:
    for race_id in sorted(all_races):
        if race_id[:6] != ym:
            continue
        for h in all_races[race_id]['horses']:
            if _int(h.get('着順')) is None:
                continue
            db[h['馬名']].append(_make_record(race_id, all_races[race_id], h))


# ─── ロジスティック回帰 ────────────────────────────────

def sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -30, 30)))


def fit_logistic(X: np.ndarray, y: np.ndarray,
                 lr: float = 0.01, n_iter: int = 3000,
                 l2: float = 0.01) -> tuple:
    """
    バイナリロジスティック回帰を勾配降下で学習。
    X: (N, F)  y: (N,) binary  →  weights (F,), bias (1,)
    l2: L2正則化係数（過学習防止）
    """
    N, F   = X.shape
    w      = np.zeros(F)
    b      = 0.0

    for i in range(n_iter):
        logits = X @ w + b
        preds  = sigmoid(logits)
        err    = preds - y
        grad_w = X.T @ err / N + l2 * w
        grad_b = err.mean()
        w -= lr * grad_w
        b -= lr * grad_b

        if (i + 1) % 500 == 0:
            loss = -np.mean(y * np.log(preds + 1e-9) +
                            (1 - y) * np.log(1 - preds + 1e-9))
            print(f'    iter {i+1:>5}  loss={loss:.4f}')

    return w, b


def predict_prob(X: np.ndarray, w: np.ndarray, b: float) -> np.ndarray:
    return sigmoid(X @ w + b)


def save_model(w: np.ndarray, b: float, mean: np.ndarray, std: np.ndarray) -> None:
    model = {
        'weights':  w.tolist(),
        'bias':     float(b),
        'mean':     mean.tolist(),
        'std':      std.tolist(),
        'factors':  LOGISTIC_FACTORS,
    }
    os.makedirs('data', exist_ok=True)
    with open(MODEL_PATH, 'w', encoding='utf-8') as f:
        json.dump(model, f, indent=2, ensure_ascii=False)
    print(f'  モデル保存: {MODEL_PATH}')


def load_model() -> tuple:
    with open(MODEL_PATH, encoding='utf-8') as f:
        m = json.load(f)
    return (np.array(m['weights']), float(m['bias']),
            np.array(m['mean']),    np.array(m['std']))


# ─── データ収集 ────────────────────────────────────────

def _rnum(rid: str) -> int:
    try:   return int(rid[10:12])
    except: return -1


def race_relative_features(raw_vectors: list) -> np.ndarray:
    """
    レース内偏差値化: 各因子を (x - race_mean) / race_std に変換。
    None / NaN はレース平均で補完してから正規化する（course_time 等の欠損対応）。
    raw_vectors: [[f1,f2,...], ...] (1レース分の全馬, None含む可)
    """
    X = np.array(raw_vectors, dtype=object)
    # None → NaN に変換して float 配列に
    X = np.where(X is None, np.nan, X).astype(float)

    # 各特徴量ごとにレース内の有効値で mean/std を計算
    with np.errstate(all='ignore'):
        col_mean = np.nanmean(X, axis=0)
        col_std  = np.nanstd(X, axis=0)

    # 全馬が NaN の列（全員データなし）は mean=0, std=1 とする → 正規化後も 0 になる
    col_mean = np.where(np.isnan(col_mean), 0.0, col_mean)
    col_std  = np.where(np.isnan(col_std)  | (col_std == 0), 1.0, col_std)

    # NaN をレース平均で補完してから正規化
    nan_mask = np.isnan(X)
    X[nan_mask] = np.take(col_mean, np.where(nan_mask)[1])

    return (X - col_mean) / col_std


def collect_samples(
    all_races:    dict,
    horse_db:     defaultdict,
    yms:          list,
    target_rnums: set,
    jstats,
    dc_db,
) -> tuple:
    """指定期間の全レースから (レース内偏差値化済み因子ベクトル, labels, extras) を収集"""
    Xs, ys, extras = [], [], []

    for ym in yms:
        month_races = sorted(
            [rid for rid in all_races if rid[:6] == ym and _rnum(rid) in target_rnums]
        )
        for race_id in month_races:
            info   = all_races[race_id]
            dist   = info.get('dist') or 1800
            course = info.get('course') or 'ダート'
            track  = info.get('track_cond') or ''
            venue  = race_id[4:6]
            ym_str = race_id[:6]

            actual_ranks = {h['馬名']: _int(h.get('着順'))
                            for h in info['horses']
                            if _int(h.get('着順')) is not None}
            if not actual_ranks or min(actual_ranks.values()) != 1:
                continue

            top3 = {n for n, r in actual_ranks.items() if r <= 3}

            # 1レース分の全馬を先に収集してからレース内正規化
            race_rows = []
            for h in info['horses']:
                name   = h['馬名']
                rank   = _int(h.get('着順'))
                if rank is None:
                    continue
                jockey = h.get('騎手', '')
                pop    = _int(h.get('人気'), 99)
                odds   = _float(h.get('単勝オッズ'), 0.0)
                hist   = horse_db.get(name, [])
                factors = compute_horse_factors(
                    name, jockey, course, dist, hist, jstats, dc_db,
                    track_cond=track, venue_code=venue, race_ym=ym_str,
                    pop=pop, odds=odds,
                )
                race_rows.append({
                    'factors': factors,
                    'is_win':  rank == 1,
                    'is_top3': name in top3,
                })

            if len(race_rows) < 3:
                continue

            raw_vecs = [r['factors'] for r in race_rows]
            normed   = race_relative_features(raw_vecs)

            for fv, row in zip(normed, race_rows):
                Xs.append(fv)
                ys.append(1 if row['is_win'] else 0)
                extras.append({'is_win': row['is_win'], 'is_top3': row['is_top3']})

        add_month_to_db(horse_db, all_races, ym)

    return np.array(Xs, dtype=float), np.array(ys, dtype=float), extras


# ─── キャリブレーション表示 ────────────────────────────

BUCKETS = [
    ( 0,  5,  ' 0- 5%'),
    ( 5, 10,  ' 5-10%'),
    (10, 15,  '10-15%'),
    (15, 20,  '15-20%'),
    (20, 30,  '20-30%'),
    (30, 100, '30%+  '),
]

def bucket_label(prob_pct: float) -> str:
    for lo, hi, label in BUCKETS:
        if lo <= prob_pct < hi:
            return label
    return '30%+  '


def print_calib_table(records: list) -> None:
    calib = {label: [] for _, _, label in BUCKETS}
    for r in records:
        calib[bucket_label(r['prob'] * 100)].append(r)

    print(f'  {"バケット":>8}  {"N頭":>6}  {"予測勝率":>8}  {"実勝率":>7}  {"3着以内率":>9}  バー(3着以内)')
    print(f'  {"-"*68}')
    for lo, hi, label in BUCKETS:
        recs = calib[label]
        if not recs:
            continue
        pred_avg  = sum(r['prob'] for r in recs) / len(recs) * 100
        win_rate  = sum(r['is_win']  for r in recs) / len(recs) * 100
        top3_rate = sum(r['is_top3'] for r in recs) / len(recs) * 100
        diff      = win_rate - pred_avg
        sign      = '+' if diff >= 0 else ''
        bar       = '█' * int(top3_rate / 3)
        print(f'  {label}  {len(recs):>6}頭  {pred_avg:>7.1f}%  {win_rate:>6.1f}%  '
              f'{top3_rate:>8.1f}%  {bar}')


# ─── テストウォーク ────────────────────────────────────

def walk_test(
    all_races:    dict,
    horse_db:     defaultdict,
    yms:          list,
    target_rnums: set,
    jstats,
    dc_db,
    w:            np.ndarray,
    b:            float,
    mean:         np.ndarray,
    std:          np.ndarray,
) -> tuple:
    records, monthly = [], []

    for ym in yms:
        month_races = sorted(
            [rid for rid in all_races if rid[:6] == ym and _rnum(rid) in target_rnums]
        )
        m = dict(ym=ym, total=0, hit=0, invest=0, ret=0)

        for race_id in month_races:
            info   = all_races[race_id]
            dist   = info.get('dist') or 1800
            course = info.get('course') or 'ダート'
            track  = info.get('track_cond') or ''
            venue  = race_id[4:6]
            ym_str = race_id[:6]

            actual_ranks = {h['馬名']: _int(h.get('着順'))
                            for h in info['horses']
                            if _int(h.get('着順')) is not None}
            if not actual_ranks or min(actual_ranks.values()) != 1:
                continue
            winner = min(actual_ranks, key=actual_ranks.get)
            top3   = {n for n, r in actual_ranks.items() if r <= 3}

            # 1レース分を先に収集 → レース内偏差値化 → 予測
            race_entries = []
            for h in info['horses']:
                name   = h['馬名']
                if _int(h.get('着順')) is None:
                    continue
                jockey = h.get('騎手', '')
                pop    = _int(h.get('人気'), 99)
                odds   = _float(h.get('単勝オッズ'), 0.0)
                hist   = horse_db.get(name, [])
                factors = compute_horse_factors(
                    name, jockey, course, dist, hist, jstats, dc_db,
                    track_cond=track, venue_code=venue, race_ym=ym_str,
                    pop=pop, odds=odds,
                )
                race_entries.append({
                    'name':    name,
                    'factors': factors,
                    'is_win':  (name == winner),
                    'is_top3': (name in top3),
                    'umaban':  h.get('馬番', '').strip(),
                })

            if len(race_entries) < 3:
                continue

            # レース内偏差値化
            raw_vecs = [e['factors'] for e in race_entries]
            normed   = race_relative_features(raw_vecs)

            # グローバル標準化 → 予測
            horse_probs = []
            for entry, fv_rel in zip(race_entries, normed):
                fv_n = (fv_rel - mean) / (std + 1e-8)
                prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
                horse_probs.append({
                    'name':    entry['name'],
                    'prob':    prob,
                    'is_win':  entry['is_win'],
                    'is_top3': entry['is_top3'],
                    'umaban':  entry['umaban'],
                })

            if not horse_probs:
                continue

            for hp in horse_probs:
                records.append(dict(race_id=race_id, ym=ym,
                                    prob=hp['prob'], is_win=hp['is_win'],
                                    is_top3=hp['is_top3'], name=hp['name'],
                                    umaban=hp['umaban']))

            tansho = parse_tansho(info.get('tansho_raw', ''))
            best   = max(horse_probs, key=lambda x: x['prob'])
            payout = tansho.get(best['umaban'], 0) if best['is_win'] else 0

            m['total']  += 1
            m['hit']    += best['is_win']
            m['invest'] += 100
            m['ret']    += payout

        add_month_to_db(horse_db, all_races, ym)

        if m['total'] > 0:
            monthly.append(m)

    return records, monthly


# ─── メイン ───────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',        default='data')
    parser.add_argument('--train-start', default='202201', dest='train_start')
    parser.add_argument('--train-end',   default='202312', dest='train_end')
    parser.add_argument('--test-start',  default='202401', dest='test_start')
    parser.add_argument('--test-end',    default='',       dest='test_end')
    parser.add_argument('--rnum',        nargs='+', type=int, default=[8, 9, 10, 11])
    parser.add_argument('--lr',          type=float, default=0.01)
    parser.add_argument('--l2',          type=float, default=0.01)
    parser.add_argument('--load-only',   action='store_true', dest='load_only',
                        help='学習をスキップして保存済みモデルを使う')
    args = parser.parse_args()

    target_rnums = set(args.rnum)

    print('=' * 70)
    print('  勝率モデル (ロジスティック回帰) ウォークフォワード')
    print('=' * 70)
    print(f'  訓練期間 : {args.train_start}〜{args.train_end}')
    print(f'  テスト期間: {args.test_start}〜{args.test_end or "最新"}')
    print(f'  対象R    : {sorted(target_rnums)}')
    print(f'  特徴量   : {LOGISTIC_FACTORS}')
    print()

    # ── データ読み込み ──
    print('[0] データ・モデル読み込み...')
    all_races    = load_all_races(args.data)
    jstats, dc_db = load_models(args.data)
    print(f'  レース: {len(all_races)}件')

    all_yms    = sorted({r[:6] for r in all_races})
    train_yms  = [ym for ym in all_yms if args.train_start <= ym <= args.train_end]
    test_yms   = [ym for ym in all_yms
                  if ym >= args.test_start and (not args.test_end or ym <= args.test_end)]

    if args.load_only:
        print(f'  保存済みモデル読み込み: {MODEL_PATH}')
        w, b, mean, std = load_model()
        print(f'  weights={w.round(3)}  bias={b:.4f}')
    else:
        # ── Phase 0: 初期 horse_db ──
        print(f'\n[1] Phase0 - 初期 horse_db (〜{args.train_start})...')
        db_train = build_db_upto(all_races, args.train_start)
        print(f'  {len(db_train)} 頭')

        # ── Phase 1: 訓練データ収集 ──
        print(f'\n[2] Phase1 - 訓練サンプル収集 ({args.train_start}〜{args.train_end})...')
        X_train, y_train, train_extras = collect_samples(
            all_races, db_train, train_yms, target_rnums, jstats, dc_db
        )
        n_pos = int(y_train.sum())
        print(f'  サンプル: {len(y_train)}件  勝者: {n_pos}件  非勝者: {len(y_train)-n_pos}件')

        # 標準化
        mean = X_train.mean(axis=0)
        std  = X_train.std(axis=0)
        X_n  = (X_train - mean) / (std + 1e-8)

        # ── Phase 2: モデル学習 ──
        print(f'\n[3] Phase2 - ロジスティック回帰学習 (lr={args.lr} l2={args.l2})...')
        w, b = fit_logistic(X_n, y_train, lr=args.lr, n_iter=3000, l2=args.l2)

        # 重み表示
        print(f'\n  学習済み重み:')
        for fname, fw in sorted(zip(LOGISTIC_FACTORS, w), key=lambda x: -abs(x[1])):
            bar  = '█' * int(abs(fw) * 20)
            sign = '+' if fw >= 0 else '-'
            print(f'    {fname:18} {sign}{abs(fw):.4f}  {bar}')
        print(f'    bias: {b:.4f}')

        save_model(w, b, mean, std)

        # 訓練期間キャリブレーション確認
        probs_train = predict_prob(X_n, w, b)
        train_recs  = [{'prob': float(p), 'is_win': ex['is_win'], 'is_top3': ex['is_top3']}
                       for p, ex in zip(probs_train, train_extras)]
        print(f'\n  訓練期間キャリブレーション:')
        print_calib_table(train_recs)

    # ── Phase 3: テスト ウォークフォワード ──
    print(f'\n[4] Phase3 - テスト ウォークフォワード ({args.test_start}〜)...\n')
    db_test = build_db_upto(all_races, args.test_start)
    print(f'  テスト初期 horse_db: {len(db_test)} 頭\n')

    test_records, monthly = walk_test(
        all_races, db_test, test_yms, target_rnums, jstats, dc_db, w, b, mean, std
    )

    # 月別表示
    print(f'  {"月":>8}  {"R数":>5}  {"的中":>12}  {"ROI":>9}')
    print(f'  {"-"*42}')
    for m in monthly:
        n = m['total']
        print(f'  {m["ym"]:>8}  {n:>5}  '
              f'{m["hit"]:>4}/{n:<4}({m["hit"]/n*100:>5.1f}%)  '
              f'{m["ret"]/m["invest"]*100:>8.1f}%')

    # テスト期間キャリブレーション
    print(f'\n  テスト期間キャリブレーション:')
    print_calib_table(test_records)

    # 全体サマリー
    if monthly:
        tot_n   = sum(m['total']  for m in monthly)
        tot_hit = sum(m['hit']    for m in monthly)
        tot_inv = sum(m['invest'] for m in monthly)
        tot_ret = sum(m['ret']    for m in monthly)
        print(f'\n{"=" * 70}')
        print(f'  全体  {tot_n}レース  '
              f'的中率 {tot_hit/tot_n*100:.1f}%  '
              f'単勝ROI {tot_ret/tot_inv*100:.1f}%')
        print('=' * 70)


if __name__ == '__main__':
    main()

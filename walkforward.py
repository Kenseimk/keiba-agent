"""
walkforward.py  ウォークフォワード検証
======================================================
訓練期間でグリッドサーチによる最適重みを探索し、
テスト期間でその重みを使って予測精度・回収率を検証する。

データ漏洩を防ぐため、テスト期間の各レース予測には
「そのレース直前までのデータ」しか使わない。

使い方:
  python walkforward.py                          # デフォルト(12ヶ月訓練/3ヶ月テスト)
  python walkforward.py --train 12 --test 3      # 明示的に指定
  python walkforward.py --expanding              # 拡張ウィンドウ (訓練期間が累積)
  python walkforward.py --quick                  # 粗いグリッドで高速
  python walkforward.py --baseline               # 固定重みとの比較のみ

フォールド例 (--train 12 --test 3):
  Fold1: 訓練 202401-202412  テスト 202501-202503
  Fold2: 訓練 202404-202503  テスト 202504-202506
  Fold3: 訓練 202407-202506  テスト 202507-202509
  ...
"""

import os, sys, re, glob, csv, argparse, itertools, time
from collections import defaultdict
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


from score_agent_core import (
    load_models, odds_fallback_score,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    WEIGHT_PRESETS, parse_avg_pos, parse_bw_change, _float, _int,
)
from backtest_agent import (
    load_all_races, build_horse_db_upto,
    parse_tansho, parse_sanrenpuku,
)

# ─── グリッド定義 ──────────────────────────────────────
GRID_FINE = {
    'recent_rank':   [2.0, 2.5, 3.0, 3.5],
    'agari_rank':    [1.0, 1.5, 2.0, 2.5],
    'jockey':        [1.0, 1.5, 2.0, 2.5],
    'course_fit':    [0.5, 1.0, 1.5],
    'running_style': [0.5, 1.0, 1.5],
    'weight_change': [0.0, 0.3, 0.5],
}
GRID_QUICK = {
    'recent_rank':   [2.0, 3.0],
    'agari_rank':    [1.0, 2.0],
    'jockey':        [1.0, 2.0],
    'course_fit':    [0.5, 1.0],
    'running_style': [0.5, 1.0],
    'weight_change': [0.0, 0.5],
}
FACTOR_KEYS = ['recent_rank', 'agari_rank', 'jockey', 'course_fit', 'running_style', 'weight_change']


# ─── ヘルパー ──────────────────────────────────────────
def ym_add(ym: str, months: int) -> str:
    """'202401' + 3 → '202404'"""
    d = datetime.strptime(ym + '01', '%Y%m%d')
    d = d + relativedelta(months=months)
    return d.strftime('%Y%m')

def ym_range(start: str, n_months: int) -> list:
    """start から n_months ヶ月分の YYYYMM リスト"""
    return [ym_add(start, i) for i in range(n_months)]

def ym_le(a: str, b: str) -> bool:
    return a <= b


# ─── 因子スコア事前計算 ────────────────────────────────
def precompute_factor_scores(target_ids, all_races, jstats, dc_db) -> dict:
    """
    各レースの全馬の6因子スコアを事前計算 (build_horse_db_upto 込み)
    戻り値: {race_id: {'winner': str, 'actual_ranks': {name: rank},
                       'horses': [{'name': str, 'factors': [...6...]}],
                       'name_to_umaban': {name: umaban_str},
                       'tansho_raw': str, 'sanrenpuku_raw': str}}
    """
    course = 'ダート'
    dist   = 1800

    cache = {}
    for i, race_id in enumerate(target_ids):
        if i % 50 == 0:
            print(f'    {i}/{len(target_ids)} ({i/len(target_ids)*100:.0f}%)', end='\r', flush=True)

        info       = all_races[race_id]
        horses_raw = info['horses']

        actual_ranks = {}
        for h in horses_raw:
            r = _int(h.get('着順'))
            if r is not None:
                actual_ranks[h['馬名']] = r
        if not actual_ranks or len(actual_ranks) < 3:
            continue

        horse_db = build_horse_db_upto(all_races, race_id)

        horse_factors = []
        for h in horses_raw:
            name   = h['馬名']
            jockey = h.get('騎手', '')
            pop    = _int(h.get('人気'), 99)
            odds   = _float(h.get('単勝オッズ'), 0.0)
            rank   = _int(h.get('着順'))
            if rank is None:
                continue

            history = horse_db.get(name, [])
            if not history:
                fb   = odds_fallback_score(pop, odds)
                f_rr = fb
                f_ar = fb * 0.9
                f_rs = 5.0
                f_wc = 5.0
            else:
                f_rr = score_recent_rank(history)
                f_ar = score_agari_rank(history)
                f_rs = score_running_style(history, course, dist)
                f_wc = score_weight_change(history)
            f_js = score_jockey(jockey, jstats)
            f_cf = score_course_fit(name, course, dist, dc_db)

            horse_factors.append({
                'name':    name,
                'umaban':  h.get('馬番', '').strip(),
                'factors': [f_rr, f_ar, f_js, f_cf, f_rs, f_wc],
            })

        if len(horse_factors) < 3:
            continue

        winner = min(actual_ranks, key=actual_ranks.get)
        cache[race_id] = {
            'winner':         winner,
            'actual_ranks':   actual_ranks,
            'horses':         horse_factors,
            'tansho_raw':     info.get('tansho_raw', ''),
            'sanrenpuku_raw': info.get('sanrenpuku_raw', ''),
        }

    print()
    return cache


# ─── 重みを使った評価 ──────────────────────────────────
def evaluate(cache: dict, weights_vec: list) -> dict:
    """
    キャッシュ済み因子スコアに weights_vec を適用して
    的中率・回収率を集計して返す。
    """
    total = hit1 = hit3 = sanpuku = niren = 0
    tan_inv = tan_ret = san_inv = san_ret = 0

    for info in cache.values():
        winner       = info['winner']
        actual_ranks = info['actual_ranks']
        horses       = info['horses']

        scored = sorted(
            [(h['name'], h['umaban'], sum(f * w for f, w in zip(h['factors'], weights_vec)))
             for h in horses],
            key=lambda x: x[2], reverse=True
        )
        if len(scored) < 3:
            continue

        pred1, ub1, _ = scored[0]
        pred2, ub2, _ = scored[1]
        pred3, ub3, _ = scored[2]

        rp1 = actual_ranks.get(pred1, 99)
        rp2 = actual_ranks.get(pred2, 99)
        rp3 = actual_ranks.get(pred3, 99)

        is_hit1    = (pred1 == winner)
        is_hit3    = (winner in (pred1, pred2, pred3))
        is_sanpuku = (rp1 <= 3 and rp2 <= 3 and rp3 <= 3)
        is_niren   = (rp1 <= 2 and rp2 <= 2)

        total  += 1
        hit1   += is_hit1
        hit3   += is_hit3
        sanpuku += is_sanpuku
        niren  += is_niren

        # 単勝
        tan_inv += 100
        if is_hit1:
            tansho = parse_tansho(info['tansho_raw'])
            tan_ret += tansho.get(ub1, 0)

        # 三連複
        san_inv += 100
        if is_sanpuku:
            sp = parse_sanrenpuku(info['sanrenpuku_raw'])
            i1 = _int(ub1, 0); i2 = _int(ub2, 0); i3 = _int(ub3, 0)
            if i1 and i2 and i3:
                san_ret += sp.get(tuple(sorted([i1, i2, i3])), 0)

    if total == 0:
        return {'total': 0, 'hit1': 0, 'hit3': 0, 'sanpuku': 0,
                'tan_roi': 0, 'san_roi': 0}
    return {
        'total':   total,
        'hit1':    hit1 / total * 100,
        'hit3':    hit3 / total * 100,
        'sanpuku': sanpuku / total * 100,
        'niren':   niren / total * 100,
        'tan_roi': tan_ret / tan_inv * 100 if tan_inv else 0,
        'san_roi': san_ret / san_inv * 100 if san_inv else 0,
    }


def grid_search(cache: dict, grid: dict) -> tuple:
    """キャッシュに対してグリッドサーチを実行し (三連複的中率, best_weights_vec) を返す"""
    combos = list(itertools.product(*[grid[k] for k in FACTOR_KEYS]))
    best_score = -1
    best_vec   = [WEIGHT_PRESETS['sanpuku'][k] for k in FACTOR_KEYS]

    for vals in combos:
        total = sanpuku = 0
        for info in cache.values():
            if len(info['horses']) < 3:
                continue
            scored = sorted(
                [(h['name'], sum(f * w for f, w in zip(h['factors'], vals)))
                 for h in info['horses']],
                key=lambda x: x[1], reverse=True
            )
            ar = info['actual_ranks']
            rp = [ar.get(scored[i][0], 99) for i in range(3)]
            total   += 1
            sanpuku += (rp[0] <= 3 and rp[1] <= 3 and rp[2] <= 3)

        if total > 0 and sanpuku / total > best_score:
            best_score = sanpuku / total
            best_vec   = list(vals)

    return best_score, best_vec


# ─── フォールド定義 ────────────────────────────────────
def make_folds(all_yms: list, train_months: int, test_months: int,
               step_months: int, expanding: bool) -> list:
    """
    all_yms: 利用可能な全 YYYYMM (ソート済み)
    戻り値: [(train_yms, test_yms), ...]
    """
    if len(all_yms) < train_months + test_months:
        return []

    folds  = []
    cursor = 0

    while True:
        if expanding:
            train_start_idx = 0
            train_end_idx   = cursor + train_months - 1
        else:
            train_start_idx = cursor
            train_end_idx   = cursor + train_months - 1

        test_start_idx = train_end_idx + 1
        test_end_idx   = test_start_idx + test_months - 1

        if test_end_idx >= len(all_yms):
            break

        train_yms = all_yms[train_start_idx : train_end_idx + 1]
        test_yms  = all_yms[test_start_idx  : test_end_idx  + 1]
        folds.append((train_yms, test_yms))
        cursor += step_months

    return folds


# ─── メイン ───────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='ウォークフォワード検証')
    parser.add_argument('--data',      default='data')
    parser.add_argument('--train',     type=int, default=12, help='訓練期間(月) デフォルト12')
    parser.add_argument('--test',      type=int, default=3,  help='テスト期間(月) デフォルト3')
    parser.add_argument('--step',      type=int, default=0,
                        help='ロール幅(月) デフォルト=テスト期間と同じ')
    parser.add_argument('--expanding', action='store_true', help='拡張ウィンドウ')
    parser.add_argument('--quick',     action='store_true', help='粗いグリッドで高速')
    parser.add_argument('--baseline',  action='store_true',
                        help='固定重み(sanpuku/win)との比較のみ（最適化スキップ）')
    args = parser.parse_args()

    step_months = args.step if args.step > 0 else args.test
    grid        = GRID_QUICK if args.quick else GRID_FINE

    print('=' * 76)
    print('  スコアエージェント ウォークフォワード検証')
    print('=' * 76)
    print(f'  訓練: {args.train}ヶ月  テスト: {args.test}ヶ月  ステップ: {step_months}ヶ月')
    print(f'  ウィンドウ: {"拡張" if args.expanding else "ローリング"}  '
          f'グリッド: {"クイック" if args.quick else "精細"}  '
          f'最適化: {"スキップ" if args.baseline else "実行"}')
    print()

    # ── データ読み込み ──
    print('[1/3] データ・モデル読み込み...')
    jstats, dc_db = load_models(args.data)
    all_races     = load_all_races(args.data)
    print(f'  合計 {len(all_races)} レース')

    # R8-R11 に絞る
    def _rnum(r):
        try:   return int(r[10:12])
        except: return -1

    target_all = sorted(
        [r for r in all_races if 8 <= _rnum(r) <= 11]
    )
    all_yms = sorted({all_races[r].get('ym', r[:6]) for r in target_all})
    print(f'  対象期間: {all_yms[0]} 〜 {all_yms[-1]}  ({len(all_yms)}ヶ月)')

    # ym → race_id リスト
    ym_to_ids = defaultdict(list)
    for r in target_all:
        ym_to_ids[all_races[r].get('ym', r[:6])].append(r)

    # ── フォールド定義 ──
    folds = make_folds(all_yms, args.train, args.test, step_months, args.expanding)
    print(f'  フォールド数: {len(folds)}')

    # ── 固定重みベクトル ──
    vec_sanpuku = [WEIGHT_PRESETS['sanpuku'][k] for k in FACTOR_KEYS]
    vec_win     = [WEIGHT_PRESETS['win'][k]     for k in FACTOR_KEYS]

    # ── フォールド処理 ──
    print('\n[2/3] フォールド処理...\n')

    fold_results = []

    for fold_i, (train_yms, test_yms) in enumerate(folds):
        print(f'  ── Fold {fold_i+1}/{len(folds)} ──')
        print(f'    訓練: {train_yms[0]}〜{train_yms[-1]}  テスト: {test_yms[0]}〜{test_yms[-1]}')

        train_ids = [r for ym in train_yms for r in ym_to_ids[ym]]
        test_ids  = [r for ym in test_yms  for r in ym_to_ids[ym]]
        print(f'    訓練レース: {len(train_ids)}  テストレース: {len(test_ids)}')

        # テストデータ事前計算 (常に必要)
        print(f'    テスト因子スコア計算中...')
        t0 = time.time()
        test_cache = precompute_factor_scores(test_ids, all_races, jstats, dc_db)
        print(f'    → {len(test_cache)}レース  {time.time()-t0:.0f}秒')

        if not test_cache:
            print('    テストデータなし。スキップ')
            continue

        # 固定重みでのテスト評価
        res_sanpuku = evaluate(test_cache, vec_sanpuku)
        res_win     = evaluate(test_cache, vec_win)

        # 最適化
        best_vec = vec_sanpuku  # デフォルト
        if not args.baseline:
            print(f'    訓練因子スコア計算中...')
            t0 = time.time()
            train_cache = precompute_factor_scores(train_ids, all_races, jstats, dc_db)
            print(f'    → {len(train_cache)}レース  {time.time()-t0:.0f}秒')

            print(f'    グリッドサーチ中... ({len(list(itertools.product(*[grid[k] for k in FACTOR_KEYS])))}通り)')
            t0 = time.time()
            best_train_score, best_vec = grid_search(train_cache, grid)
            print(f'    → 訓練三連複率: {best_train_score*100:.1f}%  {time.time()-t0:.0f}秒')
            best_weights = dict(zip(FACTOR_KEYS, best_vec))
            print(f'    最適重み: {best_weights}')
        else:
            best_train_score = None
            best_weights     = dict(zip(FACTOR_KEYS, vec_sanpuku))

        res_opt = evaluate(test_cache, best_vec)

        fold_results.append({
            'fold':        fold_i + 1,
            'train_yms':   f'{train_yms[0]}〜{train_yms[-1]}',
            'test_yms':    f'{test_yms[0]}〜{test_yms[-1]}',
            'train_n':     len(train_ids),
            'test_n':      len(test_cache),
            'best_train':  best_train_score,
            'best_weights': best_weights,
            'opt':         res_opt,
            'sanpuku':     res_sanpuku,
            'win':         res_win,
        })

        print(f'    [テスト結果]')
        print(f'      {"モデル":12}  {"1着%":>7}  {"三連複%":>7}  {"単勝ROI":>8}  {"三連複ROI":>10}')
        for label, res in [('最適化', res_opt), ('固定sanpuku', res_sanpuku), ('固定win', res_win)]:
            print(f'      {label:12}  {res["hit1"]:>6.1f}%  {res["sanpuku"]:>6.1f}%  '
                  f'{res["tan_roi"]:>7.1f}%  {res["san_roi"]:>9.1f}%')
        print()

    # ── 集計 ──
    print('[3/3] 総合サマリー')
    print('=' * 76)
    if not fold_results:
        print('フォールドなし。終了')
        return

    def _avg(key, sub):
        vals = [r[sub][key] for r in fold_results if r[sub]['total'] > 0]
        return sum(vals) / len(vals) if vals else 0

    print(f'\n  フォールド別サマリー:')
    print(f'  {"Fold":>5}  {"テスト期間":>16}  {"件数":>6}  '
          f'{"1着%":>7}  {"三連複%":>7}  {"単勝ROI":>8}  {"三連複ROI":>10}  {"モデル"}')
    print(f'  {"-"*74}')

    for r in fold_results:
        for label, key in [('最適化', 'opt'), ('固定SP', 'sanpuku')]:
            res = r[key]
            if res['total'] == 0:
                continue
            print(f'  {r["fold"]:>5}  {r["test_yms"]:>16}  {res["total"]:>6}  '
                  f'{res["hit1"]:>6.1f}%  {res["sanpuku"]:>6.1f}%  '
                  f'{res["tan_roi"]:>7.1f}%  {res["san_roi"]:>9.1f}%  {label}')

    print(f'\n  全フォールド平均:')
    print(f'  {"モデル":12}  {"1着%":>7}  {"三連複%":>7}  {"単勝ROI":>8}  {"三連複ROI":>10}')
    print(f'  {"-"*52}')

    for label, key in [('最適化', 'opt'), ('固定sanpuku', 'sanpuku'), ('固定win', 'win')]:
        vals = [r[key] for r in fold_results if r[key]['total'] > 0]
        if not vals:
            continue
        avg_hit1    = sum(v['hit1']    for v in vals) / len(vals)
        avg_sanpuku = sum(v['sanpuku'] for v in vals) / len(vals)
        avg_tan_roi = sum(v['tan_roi'] for v in vals) / len(vals)
        avg_san_roi = sum(v['san_roi'] for v in vals) / len(vals)
        print(f'  {label:12}  {avg_hit1:>6.1f}%  {avg_sanpuku:>6.1f}%  '
              f'{avg_tan_roi:>7.1f}%  {avg_san_roi:>9.1f}%')

    # 最適化 vs 固定 の改善
    opt_res  = [r['opt']     for r in fold_results if r['opt']['total']     > 0]
    fix_res  = [r['sanpuku'] for r in fold_results if r['sanpuku']['total'] > 0]
    if opt_res and fix_res and not args.baseline:
        diff_roi = (sum(v['san_roi'] for v in opt_res) / len(opt_res) -
                    sum(v['san_roi'] for v in fix_res) / len(fix_res))
        sign = '+' if diff_roi >= 0 else ''
        print(f'\n  最適化 vs 固定sanpuku 三連複ROI差: {sign}{diff_roi:.1f}%pt')
        if diff_roi > 0:
            print('  -> ウォークフォワード最適化が有効')
        else:
            print('  -> 過学習の可能性: 訓練期間の最適重みがテスト期間に汎化していない')

    print('\n' + '=' * 76)


if __name__ == '__main__':
    main()

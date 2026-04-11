# -*- coding: utf-8 -*-
"""
backtest_NAGASHI.py  NAGASHI モデル バックテスト (正式版 v2)
=============================================================
戦略名: NAGASHI
概要  : ◎○二軸 × ▲☆△△✕ながし 三連単C1

フィルター条件 (ブラインド検証済み):
  - 8R〜11R
  - 出走頭数 <= 14頭
  - ◎ win_prob >= 15%
  - ◎ odds <= 4.0倍
  - ◎ + ○ win_prob合計 >= 35%

買い目:
  - 三連単C1: ◎○二軸 × ▲☆△△✕ながし (win_prob 3〜7位 計5頭)
  - 計10通り × 100円 = 1,000円/R

検証方法:
  - チューニング期間: 202301〜202412 (2年間) でグリッドサーチ
  - ブラインド期間  : 202501〜202603 (未使用データ) で検証
  - horse_db はウォークフォワード (月次更新)

ブラインド検証結果:
  447R  的中98R(21.9%)  投資443,800円  回収465,430円  +21,630円  ROI 104.9%
  黒字月: 7ヶ月 / 赤字月: 8ヶ月  月平均収支: +1,442円

実行:
  python backtest_NAGASHI.py
  python backtest_NAGASHI.py --start 202501 --end 202603
  python backtest_NAGASHI.py --verbose
  python backtest_NAGASHI.py --blind   # チューニング→ブラインド検証モード
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info, _build_race_records
from uscore import (
    analyze_race_uscore, build_trainer_stats, build_jockey_stats,
    should_exclude_uscore,
)

# ── 固定パラメータ ────────────────────────────────
RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
N_AITE          = 5      # ながし相手頭数 (win_prob 3〜7位) ← ブラインド検証済み
MARKET_ALPHA    = 0.4
BET             = 100

# ── デフォルト (ブラインド検証済み最良パラメータ) ──
DEFAULT_HONMEI_WP_MIN   = 15.0   # チューニング: 20.0 → 15.0
DEFAULT_HONMEI_ODDS_MAX = 4.0
DEFAULT_WP_SUM_MIN      = 35.0   # チューニング: 45.0 → 35.0

# ── チューニング / ブラインド期間 ─────────────────
TUNE_START  = '202301'   # 2年分 (2023-2024) でチューニング
TUNE_END    = '202412'
BLIND_START = '202501'   # 2025年以降がブラインド
BLIND_END   = '202603'

# ── グリッドサーチ候補 ────────────────────────────
GRID = {
    'honmei_wp_min':   [15.0, 20.0, 25.0, 30.0],
    'honmei_odds_max': [3.5, 4.0, 4.5, 5.0],
    'wp_sum_min':      [35.0, 40.0, 45.0, 50.0, 55.0],
    'n_aite':          [3, 4, 5],
}
MIN_BETS = 30   # チューニング期間が2年に増えたので基準も引き上げ

def _tune_score(roi: float, n_bets: int) -> float:
    """
    過学習防止スコア: ROI × √(n_bets / MIN_BETS)
    - ベット数が多いほど高評価 (√でペナルティを緩和)
    - ROI 200% × 15R ≈ ROI 141% × 30R (同スコア)
    """
    import math
    return roi * math.sqrt(n_bets / MIN_BETS)


def load_sanrentan(data_dir, start_ym, end_ym):
    db = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if ym < start_ym or ym > end_ym:
            continue
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if rid not in db:
                    db[rid] = {}
                san = row.get('三連単払戻', '').strip()
                if san and san != '-':
                    mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', san)
                    if mm:
                        key = (mm.group(1), mm.group(2), mm.group(3))
                        db[rid][key] = int(mm.group(4))
    return db


def _run_core(races, horse_db, trainer_stats, jockey_stats, sanrentan_db,
              test_start, test_end,
              honmei_wp_min, honmei_odds_max, wp_sum_min, n_aite=N_AITE,
              walkforward=True, verbose=False):
    """
    コア評価ループ。monthly dict と total dict を返す。

    walkforward=True (推奨):
      各月のテスト後にその月の実績を horse_db に追加。
      trainer_stats / jockey_stats も月次再構築。
      → 評価時点で「未来のデータを使わない」真のブラインド評価。

    walkforward=False (後方互換):
      horse_db を更新しない（旧来動作）。
    """
    test_months = sorted(set(
        info['file_ym'] for info in races.values()
        if test_start <= info['file_ym'] <= test_end
    ))

    monthly = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0})

    for ym in test_months:
        month_races = {rid: info for rid, info in races.items()
                       if info['file_ym'] == ym}

        # 月ごとに stats を再構築 (walkforward 時)
        t_stats = build_trainer_stats(horse_db) if walkforward else trainer_stats
        j_stats = build_jockey_stats(horse_db)  if walkforward else jockey_stats

        for race_id in sorted(month_races.keys()):
            info = month_races[race_id]
            if should_exclude_uscore(info['race_name']):
                continue
            if all(h['odds'] == 0.0 for h in info['horse_list']):
                continue

            rnum = int(race_id[-2:])
            if not (RNUM_MIN <= rnum <= RNUM_MAX):
                continue
            if info['n_field'] > MAX_FIELD:
                continue

            race_obj = make_race_info(info)
            try:
                sc = analyze_race_uscore(
                    race_obj, horse_db, None, None,
                    trainer_stats=t_stats,
                    jockey_stats=j_stats,
                    market_alpha=MARKET_ALPHA,
                )
            except:
                continue
            if not sc:
                continue

            wp_map    = {h['name']: h['win_prob'] for h in sc}
            odds_map  = {h['name']: h['odds']     for h in info['horse_list']}
            uma_map   = {h['name']: h['umaban']   for h in info['horse_list']}
            sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)

            hn = sorted_wp[0]['name']
            rn = sorted_wp[1]['name'] if len(sorted_wp) > 1 else None
            if not rn:
                continue

            if wp_map.get(hn, 0) < honmei_wp_min:
                continue
            if odds_map.get(hn, 99) > honmei_odds_max:
                continue
            if wp_map.get(hn, 0) + wp_map.get(rn, 0) < wp_sum_min:
                continue

            aite = [h['name'] for h in sorted_wp[2:2+n_aite]]
            u_h  = uma_map.get(hn, '')
            u_r  = uma_map.get(rn, '')
            u_a  = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
            if not u_h or not u_r or not u_a:
                continue

            tix = set()
            for perm in permutations([u_h, u_r]):
                for a in u_a:
                    tix.add((*perm, a))

            san  = sanrentan_db.get(race_id, {})
            cost = len(tix) * BET
            ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
            hit  = int(ret > 0)

            monthly[ym]['cost']  += cost
            monthly[ym]['ret']   += ret
            monthly[ym]['races'] += 1
            monthly[ym]['hits']  += hit

            if verbose and hit:
                pay = max(san.get(t, 0) for t in tix)
                print(f'  ✓ {race_id} {info["race_name"]}  '
                      f'◎{hn}/wp{wp_map[hn]:.0f}%  ○{rn}/wp{wp_map[rn]:.0f}%  '
                      f'→ {ret:,}円 (配当{pay:,}円)')

        # ── この月の実績を horse_db に追加 (walkforward) ──
        if walkforward:
            for race_id, info in month_races.items():
                for name, rec in _build_race_records(race_id, info).items():
                    horse_db.setdefault(name, []).insert(0, rec)

    total = {'races': 0, 'hits': 0, 'cost': 0, 'ret': 0}
    for s in monthly.values():
        for k in total:
            total[k] += s[k]
    return monthly, total


def _print_monthly(monthly, total):
    print(f'{"月":>8}  {"R数":>4}  {"的中":>4}  {"的中率":>6}  '
          f'{"投資":>8}  {"回収":>8}  {"収支":>9}  {"ROI":>7}')
    print('-' * 72)
    for ym in sorted(monthly):
        s = monthly[ym]
        n = s['races']; h = s['hits']; c = s['cost']; r = s['ret']
        roi = r / c * 100 if c else 0
        mark = '✓' if roi >= 100 else '✗'
        print(f'{ym:>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  '
              f'{c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}% {mark}')
    print('-' * 72)
    n = total['races']; h = total['hits']; c = total['cost']; r = total['ret']
    roi = r / c * 100 if c else 0
    print(f'{"合計":>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  '
          f'{c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}%')
    print()
    black = sum(1 for s in monthly.values() if s['ret'] > s['cost'])
    red   = len(monthly) - black
    print(f'黒字月: {black}ヶ月  赤字月: {red}ヶ月')
    if monthly:
        avg_cost = total['cost'] // len(monthly)
        avg_pl   = (total['ret'] - total['cost']) // len(monthly)
        print(f'月平均投資: {avg_cost:,}円  月平均収支: {avg_pl:+,}円')


def run(test_start='202501', test_end='202603',
        honmei_wp_min=DEFAULT_HONMEI_WP_MIN,
        honmei_odds_max=DEFAULT_HONMEI_ODDS_MAX,
        wp_sum_min=DEFAULT_WP_SUM_MIN,
        verbose=False):
    print(f'=== NAGASHI バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'条件: {RNUM_MIN}-{RNUM_MAX}R / 頭数≤{MAX_FIELD} / '
          f'◎wp≥{honmei_wp_min}% / ◎odds≤{honmei_odds_max} / ◎+○wp≥{wp_sum_min}%')
    print(f'買い目: 三連単C1 ◎○二軸×▲☆△({N_AITE}頭) = 8通/R\n')

    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R')

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=test_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'horse_db: {len(horse_db):,}頭\n')

    sanrentan_db = load_sanrentan('data', test_start, test_end)
    monthly, total = _run_core(
        races, horse_db, trainer_stats, jockey_stats, sanrentan_db,
        test_start, test_end,
        honmei_wp_min, honmei_odds_max, wp_sum_min,
        walkforward=True, verbose=verbose,
    )
    _print_monthly(monthly, total)


def tune_and_blind():
    """
    チューニング期間でグリッドサーチ → ブラインド期間で検証。
    パラメータはチューニング期間のデータのみで決定し、
    ブラインド期間は一切参照しない。
    """
    import itertools

    print('=' * 70)
    print(f'NAGASHI ブラインド検証')
    print(f'  チューニング期間: {TUNE_START} 〜 {TUNE_END}')
    print(f'  ブラインド期間  : {BLIND_START} 〜 {BLIND_END}')
    print('=' * 70)

    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R')

    # horse_db はチューニング開始前のデータで構築
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=TUNE_START)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'horse_db: {len(horse_db):,}頭\n')

    san_tune  = load_sanrentan('data', TUNE_START, TUNE_END)
    san_blind = load_sanrentan('data', BLIND_START, BLIND_END)

    # ── STEP 1: グリッドサーチ (チューニング期間のみ) ──────────────
    print('【STEP 1】チューニング期間グリッドサーチ ...')
    keys   = list(GRID.keys())
    combos = list(itertools.product(*[GRID[k] for k in keys]))
    print(f'  候補数: {len(combos)}通り  (最低ベット数 ≥ {MIN_BETS}R)\n')

    best_score  = -999
    best_roi    = -999
    best_params = None
    results = []

    for combo in combos:
        params = dict(zip(keys, combo))
        # グリッドサーチは相対比較なので horse_db 固定 (walkforward=False)
        _, total = _run_core(
            races, horse_db, trainer_stats, jockey_stats, san_tune,
            TUNE_START, TUNE_END,
            params['honmei_wp_min'], params['honmei_odds_max'], params['wp_sum_min'],
            n_aite=params['n_aite'],
            walkforward=False,
        )
        n = total['races']; c = total['cost']; r = total['ret']
        roi   = r / c * 100 if c else 0
        score = _tune_score(roi, n) if n >= MIN_BETS else -999
        if n >= MIN_BETS:
            results.append((score, roi, n, params))
            if score > best_score:
                best_score  = score
                best_roi    = roi
                best_params = params

    results.sort(key=lambda x: x[0], reverse=True)
    print(f'  {"スコア":>8}  {"ROI":>7}  {"R数":>4}  wp_min  odds_max  wp_sum')
    print(f'  ' + '-' * 60)
    for score, roi, n, p in results[:10]:
        print(f'  {score:>7.1f}  {roi:>6.1f}%  {n:>4}  '
              f'{p["honmei_wp_min"]:>5.0f}%  '
              f'{p["honmei_odds_max"]:>7.1f}  '
              f'{p["wp_sum_min"]:>5.0f}%')

    if not best_params:
        print('\n⚠ チューニング期間に有効なベットがありませんでした。')
        return

    print(f'\n  → 最良パラメータ: wp_min={best_params["honmei_wp_min"]}%  '
          f'odds_max={best_params["honmei_odds_max"]}  '
          f'wp_sum={best_params["wp_sum_min"]}%  '
          f'(チューニング ROI {best_roi:.1f}%  スコア {best_score:.1f})')

    # ── STEP 2: ブラインド検証 ──────────────────────────────────────
    print(f'\n{"=" * 70}')
    print(f'【STEP 2】ブラインド検証 ({BLIND_START} 〜 {BLIND_END})')
    print(f'  適用パラメータ: wp_min={best_params["honmei_wp_min"]}%  '
          f'odds_max={best_params["honmei_odds_max"]}  '
          f'wp_sum={best_params["wp_sum_min"]}%')
    print(f'{"=" * 70}\n')

    # ブラインド用 horse_db: BLIND_START 時点まで（チューニング期間含む）
    print(f'  ブラインド用 horse_db 構築 (〜{BLIND_START} 前) ...', end=' ', flush=True)
    horse_db_blind = defaultdict(list)
    _add_races_to_horse_db(horse_db_blind, races, upto_ym=BLIND_START)
    for n in horse_db_blind:
        horse_db_blind[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'{len(horse_db_blind):,}頭\n')

    monthly, total = _run_core(
        races, horse_db_blind, None, None, san_blind,
        BLIND_START, BLIND_END,
        best_params['honmei_wp_min'],
        best_params['honmei_odds_max'],
        best_params['wp_sum_min'],
        n_aite=best_params['n_aite'],
        walkforward=True,
    )
    _print_monthly(monthly, total)

    # ── 参考: デフォルトパラメータでのブラインド結果 ───────────────
    print(f'\n{"─" * 70}')
    print(f'【参考】デフォルトパラメータ (wp_min={DEFAULT_HONMEI_WP_MIN}%  '
          f'odds_max={DEFAULT_HONMEI_ODDS_MAX}  '
          f'wp_sum={DEFAULT_WP_SUM_MIN}%) でのブラインド結果 (walkforward):')
    horse_db_blind2 = defaultdict(list)
    _add_races_to_horse_db(horse_db_blind2, races, upto_ym=BLIND_START)
    for n in horse_db_blind2:
        horse_db_blind2[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    _, total_def = _run_core(
        races, horse_db_blind2, None, None, san_blind,
        BLIND_START, BLIND_END,
        DEFAULT_HONMEI_WP_MIN, DEFAULT_HONMEI_ODDS_MAX, DEFAULT_WP_SUM_MIN,
        walkforward=True,
    )
    n = total_def['races']; c = total_def['cost']; r = total_def['ret']
    roi_def = r / c * 100 if c else 0
    print(f'  {n}R  投資{c:,}円  回収{r:,}円  ROI {roi_def:.1f}%')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',   default='202501')
    parser.add_argument('--end',     default='202603')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--blind',   action='store_true',
                        help='チューニング→ブラインド検証モード')
    args = parser.parse_args()

    if args.blind:
        tune_and_blind()
    else:
        run(args.start, args.end, verbose=args.verbose)

"""
optimize_tansho.py  単勝フィルター最適化

validate_tansho.py のスコアリングモデルをベースに、
オッズ帯・人気帯・スコアギャップ・頭数などのフィルターを
グリッドサーチして最良の単勝戦略を探す。

Phase1: 全レースのスコアをキャッシュ（1回だけ）
Phase2: フィルター条件を変えてROI集計（高速）

使い方:
    python optimize_tansho.py              # 2015-2022最適化 / 2023-2024検証
    python optimize_tansho.py --all        # 全期間で最適化
    python optimize_tansho.py --top 30     # 上位30件表示
"""

import sys, glob, csv, io, re, math, argparse, pickle, time, itertools
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

CACHE_FILE = 'optimize_tansho_cache.pkl'

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

# ─── スコアリング（backtest_csv.py と同一） ───────────
W_MARKET = 0.27; W_F3RANK = 0.23; W_JOCKEY = 0.18
W_PREV_RANK = 0.13; W_WEIGHT_CHG = 0.09; W_CORNER = 0.10

def parse_weight_change(s):
    m = re.match(r'\d+\(([+-]?\d+)\)', s.strip())
    return int(m.group(1)) if m else None

def score_market(odds, field_size):
    raw = math.log(max(field_size / odds, 0.01))
    return (min(max(raw, -2.0), 2.0) + 2.0) / 4.0

def score_jockey(jockey, jstats):
    return jstats.get(jockey, 3.0) / 10.0

def score_f3rank(name, ph):
    h = ph.get(name)
    if h is None: return 0.3
    rank, n = h['f3rank'], h['field_size']
    if rank == 1: return 1.0
    if rank == 2: return 0.75
    if rank == 3: return 0.50
    return max(0.10, (n - rank) / max(n - 4, 1) * 0.40 + 0.10)

def score_prev_rank(name, ph):
    h = ph.get(name)
    if h is None: return 0.3
    r = h['finish_rank']
    if r == 1: return 1.0
    if r <= 3: return 0.7
    if r <= 5: return 0.5
    if r <= 8: return 0.3
    return 0.1

def score_weight_chg(s):
    chg = parse_weight_change(s)
    if chg is None: return 0.5
    a = abs(chg)
    if a <= 2: return 1.0
    if a <= 6: return 0.7
    if a <= 10: return 0.4
    return 0.1

def score_corner(name, ph):
    h = ph.get(name)
    if h is None: return 0.3
    lc = h.get('last_corner')
    if lc is None: return 0.3
    if lc <= 3: return 1.0
    if lc <= 6: return 0.55
    return 0.15

def compute_score(h, jstats, ph, field_size):
    return (W_MARKET     * score_market(h['odds'], field_size) +
            W_F3RANK     * score_f3rank(h['name'], ph) +
            W_JOCKEY     * score_jockey(h['jockey'], jstats) +
            W_PREV_RANK  * score_prev_rank(h['name'], ph) +
            W_WEIGHT_CHG * score_weight_chg(h['weight']) +
            W_CORNER     * score_corner(h['name'], ph))

def update_ph(horses, ph):
    valid = [(float(h['f3']), h['name']) for h in horses if h['f3'].strip()]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(horses)
    for h in horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            lc = int(nums[-1]) if nums else None
            ph[h['name']] = {
                'f3rank': f3ranks.get(h['name'], n),
                'field_size': n,
                'finish_rank': h['finish_rank'],
                'last_corner': lc,
            }
        except: pass

def parse_payout(s):
    result = {}
    if not s: return result
    for part in s.split('|'):
        if ':' not in part: continue
        key, pay = part.rsplit(':', 1)
        try: result[key.strip()] = int(pay.strip())
        except: pass
    return result

# ─── データ読み込み ────────────────────────────────────
def load_data(data_dir, year_start, year_end):
    races = {}
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        if not m: continue
        year = int(m.group(1))
        if not (year_start <= year <= year_end): continue
        ym = f'{m.group(1)}-{m.group(2)}'
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if row['場コード'] not in JRA_VENUES: continue
                if rid not in races:
                    races[rid] = {'horses': [], 'ym': ym,
                                  'tansho_raw': row.get('単勝払戻', '')}
                try:
                    races[rid]['horses'].append({
                        'name':        row['馬名'],
                        'umaban':      row['馬番'].strip(),
                        'finish_rank': int(row['着順']),
                        'odds':        float(row['単勝オッズ']),
                        'popularity':  int(row['人気']),
                        'jockey':      row['騎手'],
                        'f3':          row.get('上がり3F', '').strip(),
                        'weight':      row.get('馬体重', '').strip(),
                        'corner':      row.get('通過順', '').strip(),
                    })
                except: pass
    return races

def load_jstats(data_dir):
    result = {}
    try:
        with open(f'{data_dir}/jstats.csv', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                result[row['jockey']] = float(row['j_score'])
    except FileNotFoundError: pass
    return result

# ─── Phase1: スコアキャッシュ構築 ─────────────────────
def build_cache(races, jstats):
    """
    各レースについて全馬スコアを計算してキャッシュ。
    戻り値: list of dict
      - ym, field_size
      - scored: [(score, odds, popularity, umaban, finish_rank), ...]  スコア降順
      - tansho_raw
    """
    by_month = defaultdict(list)
    for rid, info in races.items():
        by_month[info['ym']].append((rid, info))

    ph = {}
    cache = []
    total = len(races)
    done = 0

    for ym in sorted(by_month.keys()):
        for rid, info in sorted(by_month[ym]):
            horses = info['horses']
            if len(horses) < 4:
                update_ph(horses, ph)
                done += 1
                continue

            field_size = len(horses)
            scored = []
            for h in horses:
                sc = compute_score(h, jstats, ph, field_size)
                scored.append((sc, h['odds'], h['popularity'],
                               h['umaban'], h['finish_rank']))
            scored.sort(key=lambda x: -x[0])

            gap = scored[0][0] - scored[1][0] if len(scored) >= 2 else 0.0

            cache.append({
                'ym':          ym,
                'field_size':  field_size,
                'scored':      scored,   # [(score, odds, pop, umaban, rank), ...]
                'gap':         gap,      # 1位-2位のスコア差
                'tansho':      parse_payout(info['tansho_raw']),
            })
            update_ph(horses, ph)
            done += 1
            if done % 2000 == 0:
                print(f'  {done}/{total} ({done/total*100:.0f}%)', end='\r', flush=True)

    print(f'  キャッシュ構築完了: {len(cache)}レース')
    return cache

# ─── Phase2: フィルター評価（高速） ───────────────────
def evaluate(cache, odds_min, odds_max, pop_min, pop_max,
             min_gap, min_field, year_start, year_end):
    total = hit = invest = ret = 0

    for entry in cache:
        ym = entry['ym']
        year = int(ym[:4])
        if not (year_start <= year <= year_end): continue
        if entry['field_size'] < min_field: continue
        if entry['gap'] < min_gap: continue

        # フィルター条件を満たす馬の中で最高スコアを選択
        candidates = [
            (sc, odds, pop, umaban, rank)
            for sc, odds, pop, umaban, rank in entry['scored']
            if odds_min <= odds <= odds_max and pop_min <= pop <= pop_max
        ]
        if not candidates: continue

        best = candidates[0]  # スコア降順なので先頭が最高スコア
        sc, odds, pop, umaban, rank = best

        tansho = entry['tansho']
        won = (rank == 1)
        payout = tansho.get(umaban, 0) if won else 0

        total  += 1
        invest += 100
        ret    += payout
        if won: hit += 1

    if total == 0: return None
    roi = ret / invest * 100
    win_rate = hit / total * 100
    return {'total': total, 'hit': hit, 'roi': roi, 'win_rate': win_rate,
            'invest': invest, 'ret': ret}

# ─── グリッドサーチ ───────────────────────────────────
def grid_search(cache, year_start, year_end, min_bets=80):
    odds_min_list  = [1.0, 3.0, 5.0, 8.0, 10.0, 12.0, 15.0, 20.0]
    odds_max_list  = [10.0, 15.0, 20.0, 30.0, 50.0, 999.0]
    pop_min_list   = [1, 2, 3, 4, 5, 6]
    pop_max_list   = [3, 5, 8, 10, 14]
    min_gap_list   = [0.0, 0.02, 0.05, 0.08, 0.10]
    min_field_list = [6, 8, 10, 12]

    combos = list(itertools.product(
        odds_min_list, odds_max_list,
        pop_min_list, pop_max_list,
        min_gap_list, min_field_list
    ))
    # odds_min < odds_max / pop_min <= pop_max の組み合わせだけ残す
    combos = [(omin, omax, pmin, pmax, gap, field)
              for omin, omax, pmin, pmax, gap, field in combos
              if omin < omax and pmin <= pmax]

    print(f'  探索条件数: {len(combos):,}')
    t0 = time.time()
    results = []

    for i, (omin, omax, pmin, pmax, gap, field) in enumerate(combos):
        if i % 5000 == 0:
            print(f'  {i:,}/{len(combos):,} ({i/len(combos)*100:.0f}%) '
                  f'{time.time()-t0:.0f}秒', end='\r', flush=True)
        r = evaluate(cache, omin, omax, pmin, pmax, gap, field, year_start, year_end)
        if r is None or r['total'] < min_bets: continue
        results.append({**r, 'odds_min': omin, 'odds_max': omax,
                        'pop_min': pmin, 'pop_max': pmax,
                        'min_gap': gap, 'min_field': field})

    print(f'\n  完了: {len(results):,}件 / {time.time()-t0:.1f}秒')
    results.sort(key=lambda x: -x['roi'])
    return results

# ─── 出力 ─────────────────────────────────────────────
def print_results(results, top_n, label):
    print()
    print('=' * 80)
    print(f'  {label}  上位{top_n}件')
    print('=' * 80)
    print(f'{"ROI":>7} {"的中率":>7} {"件数":>6} {"損益":>9} '
          f'{"オッズ帯":>14} {"人気帯":>9} {"gap":>5} {"頭数":>4}')
    print('-' * 80)
    for r in results[:top_n]:
        odds_str = f'{r["odds_min"]:.0f}〜{r["odds_max"]:.0f}'
        pop_str  = f'{r["pop_min"]}-{r["pop_max"]}人気'
        print(f'{r["roi"]:>6.1f}%  {r["win_rate"]:>5.1f}%  {r["total"]:>6,}'
              f'  {r["ret"]-r["invest"]:>+9,}  {odds_str:>14}  {pop_str:>9}'
              f'  {r["min_gap"]:>4.2f}  {r["min_field"]:>4}')
    print()

# ─── メイン ───────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--all',   action='store_true', help='全期間で最適化')
    ap.add_argument('--top',   type=int, default=20)
    ap.add_argument('--min-bets', type=int, default=80)
    ap.add_argument('--rebuild', action='store_true', help='キャッシュ再構築')
    ap.add_argument('--data',  default='data')
    args = ap.parse_args()

    if args.all:
        opt_start, opt_end = 2015, 2024
        val_start, val_end = None, None
    else:
        opt_start, opt_end = 2015, 2022
        val_start, val_end = 2023, 2024

    # ── キャッシュ読み込み or 構築 ──
    if not args.rebuild and os.path.exists(CACHE_FILE):
        print(f'キャッシュ読み込み: {CACHE_FILE}')
        import pickle
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
    else:
        print('Phase1: データ読み込み＆スコア計算...')
        races  = load_data(args.data, 2015, 2024)
        jstats = load_jstats(args.data)
        print(f'  {len(races):,}レース読み込み完了')
        cache  = build_cache(races, jstats)
        import pickle
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        print(f'  キャッシュ保存: {CACHE_FILE}')

    # ── Phase2: グリッドサーチ（最適化期間） ──
    print(f'\nPhase2: グリッドサーチ ({opt_start}〜{opt_end}年)...')
    results = grid_search(cache, opt_start, opt_end, args.min_bets)

    print_results(results, args.top, f'最適化期間 {opt_start}〜{opt_end}年')

    # ── 検証期間で上位条件を評価 ──
    if val_start:
        print(f'検証期間 ({val_start}〜{val_end}年) で上位10件を評価:')
        print()
        print(f'{"ROI":>7} {"的中率":>7} {"件数":>6} {"損益":>9} '
              f'{"オッズ帯":>14} {"人気帯":>9} {"gap":>5} {"頭数":>4}')
        print('-' * 80)
        for r in results[:10]:
            rv = evaluate(cache, r['odds_min'], r['odds_max'],
                          r['pop_min'], r['pop_max'],
                          r['min_gap'], r['min_field'],
                          val_start, val_end)
            if rv is None:
                print(f'  (検証データなし) オッズ{r["odds_min"]:.0f}〜{r["odds_max"]:.0f} '
                      f'{r["pop_min"]}-{r["pop_max"]}人気')
                continue
            odds_str = f'{r["odds_min"]:.0f}〜{r["odds_max"]:.0f}'
            pop_str  = f'{r["pop_min"]}-{r["pop_max"]}人気'
            print(f'{rv["roi"]:>6.1f}%  {rv["win_rate"]:>5.1f}%  {rv["total"]:>6,}'
                  f'  {rv["ret"]-rv["invest"]:>+9,}  {odds_str:>14}  {pop_str:>9}'
                  f'  {r["min_gap"]:>4.2f}  {r["min_field"]:>4}')
        print()

import os
if __name__ == '__main__':
    main()

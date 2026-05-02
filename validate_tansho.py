"""
validate_tansho.py - 2015〜2024年データで単勝予測を検証

スコアリングモデル（backtest_csv.pyと同一ロジック）で
各レースの予測1位馬に単勝ベットした場合のROI・的中率を算出。

使い方:
    python validate_tansho.py              # 2015-2024全期間
    python validate_tansho.py 2020 2024    # 年範囲指定
"""

import sys, glob, csv, io, re, math
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ─────────────────────────────────────────────────────────
# backtest_csv.py と同一のスコアリング定数・関数
# ─────────────────────────────────────────────────────────
JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

W_MARKET     = 0.27
W_F3RANK     = 0.23
W_JOCKEY     = 0.18
W_PREV_RANK  = 0.13
W_WEIGHT_CHG = 0.09
W_CORNER     = 0.10

def parse_weight_change(s):
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
    chg = parse_weight_change(weight_str)
    if chg is None: return 0.5
    a = abs(chg)
    if a <= 2:  return 1.0
    if a <= 6:  return 0.7
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

def compute_horse_score(h, jstats, prev_history, field_size):
    return (W_MARKET     * score_market(h['odds'], field_size) +
            W_F3RANK     * score_f3rank(h['name'], prev_history) +
            W_JOCKEY     * score_jockey(h['jockey'], jstats) +
            W_PREV_RANK  * score_prev_rank(h['name'], prev_history) +
            W_WEIGHT_CHG * score_weight_chg(h['weight']) +
            W_CORNER     * score_corner(h['name'], prev_history))

def update_prev_history(race_horses, prev_history):
    valid = [(float(h['f3']), h['name']) for h in race_horses if h['f3'].strip()]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(race_horses)
    for h in race_horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            last_corner = int(nums[-1]) if nums else None
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': last_corner,
            }
        except:
            pass

def parse_payout(s):
    result = {}
    if not s or not s.strip():
        return result
    for part in s.split('|'):
        part = part.strip()
        if ':' not in part:
            continue
        key, pay = part.rsplit(':', 1)
        try:
            result[key.strip()] = int(pay.strip())
        except:
            pass
    return result

# ─────────────────────────────────────────────────────────
# データ読み込み（年範囲フィルタ付き）
# ─────────────────────────────────────────────────────────
def load_csv_data(data_dir, year_start, year_end):
    races = {}
    pattern = f'{data_dir}/raceresults_*.csv'
    files = sorted(glob.glob(pattern))
    loaded = 0
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        if not m: continue
        year = int(m.group(1))
        if not (year_start <= year <= year_end):
            continue
        ym = f'{m.group(1)}-{m.group(2)}'
        loaded += 1
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                race_id = row['race_id']
                if row['場コード'] not in JRA_VENUES:
                    continue
                if race_id not in races:
                    races[race_id] = {
                        'horses': [],
                        'ym': ym,
                        'tansho_raw': row.get('単勝払戻', ''),
                    }
                try:
                    finish_rank = int(row['着順'])
                    odds        = float(row['単勝オッズ'])
                    popularity  = int(row['人気'])
                except:
                    continue
                races[race_id]['horses'].append({
                    'name':        row['馬名'],
                    'umaban':      row['馬番'].strip(),
                    'finish_rank': finish_rank,
                    'odds':        odds,
                    'popularity':  popularity,
                    'jockey':      row['騎手'],
                    'f3':          row.get('上がり3F', '').strip(),
                    'weight':      row.get('馬体重', '').strip(),
                    'corner':      row.get('通過順', '').strip(),
                })
    print(f'読み込み: {loaded}ファイル / {len(races)}レース（{year_start}〜{year_end}年）')
    return races

def load_jstats(data_dir):
    result = {}
    try:
        with open(f'{data_dir}/jstats.csv', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                result[row['jockey']] = float(row['j_score'])
        print(f'騎手スコア: {len(result)}名')
    except FileNotFoundError:
        print('[WARN] jstats.csv なし。騎手スコアは中立値')
    return result

# ─────────────────────────────────────────────────────────
# 検証本体
# ─────────────────────────────────────────────────────────
def validate(races, jstats):
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    prev_history = {}

    total = hit = invest = ret = 0
    monthly_rows = []

    # 人気別集計
    pop_stats = defaultdict(lambda: {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0})
    # オッズ帯別集計
    odds_stats = defaultdict(lambda: {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0})

    for ym in sorted(by_month.keys()):
        m_total = m_hit = m_invest = m_ret = 0

        for race_id, info in sorted(by_month[ym]):
            horses = info['horses']
            if len(horses) < 4:
                continue

            field_size = len(horses)

            # スコア計算 → 最高スコア馬を選択
            scored = []
            for h in horses:
                sc = compute_horse_score(h, jstats, prev_history, field_size)
                scored.append((sc, h))
            scored.sort(key=lambda x: -x[0])
            best_score, best_horse = scored[0]

            # 単勝払戻を取得
            tansho = parse_payout(info['tansho_raw'])

            # 的中判定（馬番で照合）
            umaban = best_horse['umaban']
            won = (best_horse['finish_rank'] == 1)
            payout_rate = tansho.get(umaban, 0) if won else 0

            bet = 100  # 均一100円ベット
            m_total += 1
            m_invest += bet
            m_ret += payout_rate  # 払戻は100円単位

            if won:
                m_hit += 1

            # 人気別
            pop_key = best_horse['popularity']
            pop_stats[pop_key]['total']  += 1
            pop_stats[pop_key]['invest'] += bet
            if won:
                pop_stats[pop_key]['hit'] += 1
                pop_stats[pop_key]['ret'] += payout_rate

            # オッズ帯別
            o = best_horse['odds']
            if o < 3:       ob = '〜2.9倍'
            elif o < 5:     ob = '3〜4.9倍'
            elif o < 10:    ob = '5〜9.9倍'
            elif o < 20:    ob = '10〜19.9倍'
            else:           ob = '20倍〜'
            odds_stats[ob]['total']  += 1
            odds_stats[ob]['invest'] += bet
            if won:
                odds_stats[ob]['hit'] += 1
                odds_stats[ob]['ret'] += payout_rate

            # 前走履歴更新
            update_prev_history(horses, prev_history)

        total   += m_total
        hit     += m_hit
        invest  += m_invest
        ret     += m_ret

        roi = ret / invest * 100 if invest else 0
        win_rate = m_hit / m_total * 100 if m_total else 0
        monthly_rows.append((ym, m_total, m_hit, win_rate, m_invest, m_ret,
                              m_ret / m_invest * 100 if m_invest else 0))

    return total, hit, invest, ret, monthly_rows, pop_stats, odds_stats

# ─────────────────────────────────────────────────────────
# 出力
# ─────────────────────────────────────────────────────────
def print_results(total, hit, invest, ret, monthly_rows, pop_stats, odds_stats,
                  year_start, year_end):
    roi = ret / invest * 100 if invest else 0
    win_rate = hit / total * 100 if total else 0

    print()
    print('=' * 60)
    print(f'  単勝検証結果 ({year_start}〜{year_end}年)')
    print('=' * 60)
    print(f'  対象レース数  : {total:,}')
    print(f'  的中数        : {hit:,}')
    print(f'  的中率        : {win_rate:.1f}%')
    print(f'  投資額合計    : {invest:,}円')
    print(f'  回収額合計    : {ret:,}円')
    print(f'  ROI           : {roi:.1f}%')
    print(f'  損益           : {ret - invest:+,}円')
    print()

    print('── 月別 ──────────────────────────────────')
    print(f'{"年月":<9} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 45)
    prev_year = None
    for ym, mt, mh, mwr, mi, mr, mroi in monthly_rows:
        year = ym[:4]
        if prev_year and year != prev_year:
            print()
        print(f'{ym:<9} {mt:>6,} {mh:>5,} {mwr:>6.1f}%  {mroi:>7.1f}%')
        prev_year = year

    print()
    print('── 人気別 ────────────────────────────────')
    print(f'{"人気":<6} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 38)
    for pop in sorted(pop_stats.keys()):
        s = pop_stats[pop]
        wr  = s['hit'] / s['total'] * 100 if s['total'] else 0
        roi_ = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        print(f'{pop:<6} {s["total"]:>6,} {s["hit"]:>5,} {wr:>6.1f}%  {roi_:>7.1f}%')

    print()
    print('── オッズ帯別 ────────────────────────────')
    print(f'{"オッズ帯":<12} {"レース":>6} {"的中":>5} {"的中率":>7} {"ROI":>8}')
    print('-' * 44)
    for ob in ['〜2.9倍', '3〜4.9倍', '5〜9.9倍', '10〜19.9倍', '20倍〜']:
        if ob not in odds_stats: continue
        s = odds_stats[ob]
        wr  = s['hit'] / s['total'] * 100 if s['total'] else 0
        roi_ = s['ret'] / s['invest'] * 100 if s['invest'] else 0
        print(f'{ob:<12} {s["total"]:>6,} {s["hit"]:>5,} {wr:>6.1f}%  {roi_:>7.1f}%')
    print()

# ─────────────────────────────────────────────────────────
# エントリーポイント
# ─────────────────────────────────────────────────────────
if __name__ == '__main__':
    args = sys.argv[1:]
    year_start = int(args[0]) if len(args) >= 1 else 2015
    year_end   = int(args[1]) if len(args) >= 2 else 2024
    data_dir   = args[2] if len(args) >= 3 else 'data'

    races  = load_csv_data(data_dir, year_start, year_end)
    jstats = load_jstats(data_dir)
    total, hit, invest, ret, monthly_rows, pop_stats, odds_stats = validate(races, jstats)
    print_results(total, hit, invest, ret, monthly_rows, pop_stats, odds_stats,
                  year_start, year_end)

"""
backtest_strategies.py - 3つの新戦略を複数パラメータでバックテスト

Strategy A: FUKUSHO（隠れ末脚型複勝）
Strategy B: ZANZOU（前走惜敗型複勝）   ← 複数パラメータで探索
Strategy C: GRADE_DOWN（重賞降格馬複勝）← gradeデータ取得後に有効
"""
import sys, glob, csv, io, re
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

MARGIN_MAP = {
    '': 0.0, 'ハナ': 0.1, 'アタマ': 0.15, 'クビ': 0.25,
    '1/2': 0.5, '3/4': 0.75,
    '1': 1.0, '1.1/4': 1.25, '1.1/2': 1.5, '1.3/4': 1.75,
    '2': 2.0, '2.1/4': 2.25, '2.1/2': 2.5, '2.3/4': 2.75,
    '3': 3.0, '3.1/2': 3.5, '4': 4.0, '5': 5.0,
    '6': 6.0, '7': 7.0, '8': 8.0, '大': 99.0,
}

def parse_margin(s):
    s = str(s).strip()
    return MARGIN_MAP.get(s, 99.0 if not s.replace('.','').isdigit() else float(s))


# ─────────────────────────────────────────
# データ読み込み
# ─────────────────────────────────────────
def load_csv_data(data_dir):
    races = {}
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        file_ym = f'{m.group(1)}-{m.group(2)}' if m else None
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                race_id = row['race_id']
                if row['場コード'] not in JRA_VENUES:
                    continue
                if race_id not in races:
                    races[race_id] = {
                        'horses':      [],
                        'ym':          file_ym,
                        'grade':       row.get('grade', '').strip(),
                        'course':      row.get('コース', '').strip(),
                        'fukusho_raw': row.get('複勝払戻', ''),
                    }
                try:
                    races[race_id]['horses'].append({
                        'name':        row['馬名'],
                        'umaban':      row['馬番'].strip(),
                        'finish_rank': int(row['着順']),
                        'odds':        float(row['単勝オッズ']),
                        'popularity':  int(row['人気']),
                        'f3':          row.get('上がり3F', '').strip(),
                        'corner':      row.get('通過順', '').strip(),
                        'margin':      parse_margin(row.get('着差', '')),
                    })
                except:
                    pass
    print(f'読み込み: {len(files)}ファイル / {len(races)}レース')
    return races


def parse_payout(s):
    result = {}
    for part in (s or '').split('|'):
        part = part.strip()
        if ':' not in part:
            continue
        key, pay = part.rsplit(':', 1)
        try:
            result[key.strip()] = int(pay.strip())
        except:
            pass
    return result


# ─────────────────────────────────────────
# 前走履歴
# ─────────────────────────────────────────
def update_prev_history(race_id, info, prev_history):
    horses = info['horses']
    grade  = info.get('grade', '')
    course = info.get('course', '')
    valid  = [(float(h['f3']), h['name']) for h in horses if h['f3']]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(horses)
    for h in horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': int(nums[-1]) if nums else None,
                'margin':      h['margin'],
                'grade':       grade,
                'course':      course,
            }
        except:
            pass


# ─────────────────────────────────────────
# Strategy A: FUKUSHO（複数パラメータ）
# ─────────────────────────────────────────
FUKUSHO_CONFIGS = [
    # label, prev_f3rank, prev_finish_min, odds_min, odds_max, pop_min, pop_max
    ('F1: 上がり1位/7着↓ 14-18倍 6-12人気',  1, 7,  14.0, 18.0, 6, 12),
    ('F2: 上がり1位/7着↓ 12-20倍 5-13人気',  1, 7,  12.0, 20.0, 5, 13),
    ('F3: 上がり1位/6着↓ 10-20倍 5-12人気',  1, 6,  10.0, 20.0, 5, 12),
    ('F4: 上がり1-2位/7着↓ 12-22倍 5-14人気',2, 7,  12.0, 22.0, 5, 14),
]

def select_fukusho(races_m, prev_history, cfg):
    _, f3max, finish_min, odds_min, odds_max, pop_min, pop_max = cfg
    cands = []
    for race_id, info in races_m:
        if len(info['horses']) < 8:
            continue
        for h in info['horses']:
            ph = prev_history.get(h['name'])
            if ph is None or ph['f3rank'] > f3max:
                continue
            if ph['finish_rank'] < finish_min:
                continue
            if not (odds_min <= h['odds'] <= odds_max):
                continue
            if not (pop_min <= h['popularity'] <= pop_max):
                continue
            cands.append((h, race_id, info))
    return cands


# ─────────────────────────────────────────
# Strategy B: ZANZOU（複数パラメータ）
# ─────────────────────────────────────────
ZANZOU_CONFIGS = [
    # label, finish_min, finish_max, margin_max, odds_min, odds_max, pop_min, pop_max
    ('Z1: 2着 着差≤0.5 8-16倍 3-8人気',  2, 2, 0.5,  8.0, 16.0, 3,  8),
    ('Z2: 2着 着差≤1.0 8-18倍 3-10人気', 2, 2, 1.0,  8.0, 18.0, 3, 10),
    ('Z3: 2-3着 着差≤0.5 8-16倍 3-8人気',2, 3, 0.5,  8.0, 16.0, 3,  8),
    ('Z4: 2-3着 着差≤0.75 8-18倍 3-10人気',2,3, 0.75, 8.0, 18.0, 3, 10),
    ('Z5: 2-4着 着差≤0.5 8-16倍 3-8人気',2, 4, 0.5,  8.0, 16.0, 3,  8),
]

def select_zanzou(races_m, prev_history, cfg):
    _, fin_min, fin_max, margin_max, odds_min, odds_max, pop_min, pop_max = cfg
    cands = []
    for race_id, info in races_m:
        if len(info['horses']) < 8:
            continue
        cur_course = info.get('course', '')
        for h in info['horses']:
            ph = prev_history.get(h['name'])
            if ph is None:
                continue
            if not (fin_min <= ph['finish_rank'] <= fin_max):
                continue
            if ph['margin'] > margin_max:
                continue
            if cur_course and ph.get('course') and cur_course != ph['course']:
                continue
            if not (odds_min <= h['odds'] <= odds_max):
                continue
            if not (pop_min <= h['popularity'] <= pop_max):
                continue
            cands.append((h, race_id, info))
    return cands


# ─────────────────────────────────────────
# Strategy C: GRADE_DOWN
# ─────────────────────────────────────────
GRADED = {'G1', 'G2', 'G3'}

GRADE_DOWN_CONFIGS = [
    # label, odds_min, odds_max, pop_min, pop_max
    ('G1: G1〜G3降格 8-20倍 4-12人気', 8.0, 20.0, 4, 12),
    ('G2: G1〜G3降格 5-15倍 3-10人気', 5.0, 15.0, 3, 10),
]

def select_grade_down(races_m, prev_history, cfg):
    _, odds_min, odds_max, pop_min, pop_max = cfg
    cands = []
    for race_id, info in races_m:
        if len(info['horses']) < 8:
            continue
        if info.get('grade', '') in GRADED:
            continue
        for h in info['horses']:
            ph = prev_history.get(h['name'])
            if ph is None or ph.get('grade', '') not in GRADED:
                continue
            if not (odds_min <= h['odds'] <= odds_max):
                continue
            if not (pop_min <= h['popularity'] <= pop_max):
                continue
            cands.append((h, race_id, info))
    return cands


# ─────────────────────────────────────────
# バックテスト共通ロジック
# ─────────────────────────────────────────
BET = 1000

def calc_return(h, info):
    fukusho = parse_payout(info['fukusho_raw'])
    if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
        return int(BET * fukusho[h['umaban']] / 100)
    return 0


def backtest_config(races, select_fn, cfg):
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    prev_history = {}
    stats = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
    monthly = []

    for ym in sorted(by_month.keys()):
        races_m = by_month[ym]
        cands   = select_fn(races_m, prev_history, cfg)
        m = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}

        seen = set()
        for h, race_id, info in cands:
            if race_id in seen:
                continue
            seen.add(race_id)
            ret = calc_return(h, info)
            for s in (stats, m):
                s['total']  += 1
                s['invest'] += BET
                s['ret']    += ret
                if ret > 0:
                    s['hit'] += 1

        for race_id, info in races_m:
            update_prev_history(race_id, info, prev_history)

        monthly.append({'ym': ym, **m})

    return stats, monthly


# ─────────────────────────────────────────
# 表示
# ─────────────────────────────────────────
def fmt_stat(s):
    if s['invest'] == 0:
        return '  件数なし'
    r = s['ret'] / s['invest'] * 100
    hr = s['hit'] / s['total'] * 100
    sign = '★' if r >= 110 else ' '
    return (f'{sign} {s["total"]:3d}件 的中{hr:4.1f}% '
            f'投資¥{s["invest"]//1000:4d}k 払戻¥{s["ret"]//1000:4d}k '
            f'回収率{r:5.1f}%')


def print_section(title, configs, select_fn, races):
    print(f'\n{"="*72}')
    print(f'  {title}')
    print(f'{"="*72}')
    best = None
    for cfg in configs:
        label = cfg[0]
        stats, _ = backtest_config(races, select_fn, cfg)
        line = fmt_stat(stats)
        print(f'  [{label}]')
        print(f'   {line}')
        if stats['invest'] > 0:
            r = stats['ret'] / stats['invest'] * 100
            if best is None or r > best[0]:
                best = (r, label, stats)
    if best:
        print(f'\n  → 最良パラメータ: [{best[1]}]  回収率 {best[0]:.1f}%')


def print_monthly_best(title, cfg, select_fn, races):
    stats, monthly = backtest_config(races, select_fn, cfg)
    if stats['invest'] == 0:
        return
    print(f'\n─ 月別詳細: {title} ─')
    print(f'{"月":<10} {"件数":>6} {"的中":>6} {"回収率":>8}')
    for row in monthly:
        if row['invest'] == 0:
            continue
        r = row['ret'] / row['invest'] * 100
        mark = '●' if r >= 100 else '  '
        print(f'{row["ym"]:<10} {row["total"]:>6} {row["hit"]:>6} {r:>7.0f}% {mark}')
    total_r = stats['ret'] / stats['invest'] * 100
    print(f'{"合計":<10} {stats["total"]:>6} {stats["hit"]:>6} {total_r:>7.0f}%')


if __name__ == '__main__':
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    data_dir = sys.argv[1] if len(sys.argv) > 1 else 'data'
    races = load_csv_data(data_dir)

    print_section('Strategy A: FUKUSHO（隠れ末脚型複勝）', FUKUSHO_CONFIGS, select_fukusho, races)
    print_section('Strategy B: ZANZOU（前走惜敗型複勝）',  ZANZOU_CONFIGS,  select_zanzou,  races)
    print_section('Strategy C: GRADE_DOWN（重賞降格馬複勝）', GRADE_DOWN_CONFIGS, select_grade_down, races)

    # 最良パラメータの月別詳細
    print(f'\n{"="*72}')
    print('  最良パラメータ 月別詳細')
    print(f'{"="*72}')
    print_monthly_best('FUKUSHO F2', FUKUSHO_CONFIGS[1], select_fukusho, races)
    print_monthly_best('ZANZOU Z1',  ZANZOU_CONFIGS[0],  select_zanzou,  races)

    print(f'\n{"─"*72}')
    print('【参考】現行ANA戦略: 340件 的中率37.9% 回収率128.6%')
    print(f'{"─"*72}')

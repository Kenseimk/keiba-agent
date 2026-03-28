"""
race_specific.py - レース名に特化した過去データ分析

使い方:
  from race_specific import race_specific_analysis

  # predict.py や judge.py から呼び出す
  result = race_specific_analysis(
      race_name='有馬記念',
      horses=horses,          # 今年の出走馬リスト
      prev_history=prev_history,
      data_dir='data',
  )
"""
import csv, re, os, glob
from collections import defaultdict

# ── 複勝圏（3着以内）の定義 ──────────────────────────────
def _is_placed(finish_rank, field_size):
    """field_size に応じた複勝圏判定"""
    if field_size <= 7:   return finish_rank <= 2
    return finish_rank <= 3


# ══════════════════════════════════════════════════════════
# 過去データ読み込み（race_name列で絞り込み）
# ══════════════════════════════════════════════════════════
def load_race_history(race_name: str, data_dir: str = 'data') -> list:
    """
    raceresults_*.csv から指定レース名の過去結果を全件返す。

    Returns: list of race_entries
      race_entry = {
        'year', 'race_id', 'grade', 'field_size',
        'horses': [{name, finish_rank, odds, popularity, f3, corner, weight}, ...]
      }
    """
    files = sorted(glob.glob(os.path.join(data_dir, 'raceresults_*.csv')))
    # race_id → rows のグループ化
    raw = defaultdict(list)
    for fpath in files:
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rn = row.get('race_name', '').strip()
                if _name_match(rn, race_name):
                    raw[row['race_id']].append(row)

    results = []
    for race_id, rows in sorted(raw.items()):
        year  = rows[0].get('年', race_id[:4])
        grade = rows[0].get('grade', '')
        horses = []
        for r in rows:
            try:
                horses.append({
                    'name':        r['馬名'],
                    'finish_rank': int(r['着順']),
                    'odds':        float(r['単勝オッズ']),
                    'popularity':  int(r['人気']),
                    'f3':          r.get('上がり3F', '').strip(),
                    'corner':      r.get('通過順', '').strip(),
                    'weight':      r.get('馬体重', '').strip(),
                    'jockey':      r.get('騎手', '').strip(),
                })
            except (ValueError, KeyError):
                pass
        if horses:
            results.append({
                'year':       year,
                'race_id':    race_id,
                'grade':      grade,
                'field_size': len(horses),
                'horses':     horses,
            })
    return results


def _name_match(stored: str, query: str) -> bool:
    """レース名の表記ゆれを許容したマッチング"""
    if not stored or not query:
        return False
    # 完全一致 or クエリがストアドに含まれる（またはその逆）
    s = stored.strip()
    q = query.strip()
    return s == q or q in s or s in q


# ══════════════════════════════════════════════════════════
# 統計分析
# ══════════════════════════════════════════════════════════
def analyze_race_pattern(history: list) -> dict:
    """
    過去N年分のレース結果から統計を計算する。

    Returns:
      {
        'n_years': int,
        'pop_place_rate': {1: 0.85, 2: 0.60, ...},  # 人気別複勝率
        'odds_buckets': [{'range': '2-5', 'place_rate': 0.62, 'n': 30}, ...],
        'winner_profile': {'median_odds': 4.5, 'median_pop': 2, ...},
        'f3rank_place_rate': {1: 0.55, 2: 0.40, ...},  # 上がり順位別複勝率
        'corner_pos_place_rate': {'先行': 0.4, '差し': 0.35, ...},
      }
    """
    if not history:
        return {}

    pop_placed  = defaultdict(lambda: [0, 0])  # pop -> [placed, total]
    odds_data   = []  # [(odds, placed)]
    f3rank_data = defaultdict(lambda: [0, 0])
    corner_data = defaultdict(lambda: [0, 0])
    winner_odds = []
    winner_pops = []

    for entry in history:
        fs = entry['field_size']
        horses = entry['horses']
        # 上がり3F順位を計算
        f3_valid = [(float(h['f3']), h['name']) for h in horses if _is_float(h['f3'])]
        f3_valid.sort()
        f3rank_map = {name: rank + 1 for rank, (_, name) in enumerate(f3_valid)}

        for h in horses:
            placed = _is_placed(h['finish_rank'], fs)
            pop    = h['popularity']

            pop_placed[pop][1] += 1
            if placed:
                pop_placed[pop][0] += 1

            odds_data.append((h['odds'], placed))

            f3r = f3rank_map.get(h['name'])
            if f3r:
                f3rank_data[f3r][1] += 1
                if placed:
                    f3rank_data[f3r][0] += 1

            # コーナー位置（最終コーナー）
            cpos = _last_corner(h['corner'], fs)
            if cpos:
                label = _corner_label(cpos, fs)
                corner_data[label][1] += 1
                if placed:
                    corner_data[label][0] += 1

            if h['finish_rank'] == 1:
                winner_odds.append(h['odds'])
                winner_pops.append(h['popularity'])

    # 人気別複勝率（1〜12番人気まで）
    pop_place_rate = {}
    for pop in range(1, 13):
        placed, total = pop_placed[pop]
        if total >= 3:
            pop_place_rate[pop] = round(placed / total, 3)

    # オッズ帯別
    buckets_def = [(1, 2), (2, 4), (4, 7), (7, 12), (12, 20), (20, 40), (40, 999)]
    odds_buckets = []
    for lo, hi in buckets_def:
        subset = [(o, p) for o, p in odds_data if lo <= o < hi]
        if subset:
            rate = sum(1 for _, p in subset if p) / len(subset)
            odds_buckets.append({
                'range': f'{lo}-{hi}',
                'place_rate': round(rate, 3),
                'n': len(subset),
            })

    # 上がり順位別
    f3rank_rate = {}
    for rank in range(1, 6):
        placed, total = f3rank_data[rank]
        if total >= 3:
            f3rank_rate[rank] = round(placed / total, 3)

    # コーナー別
    corner_rate = {label: round(v[0]/v[1], 3) for label, v in corner_data.items() if v[1] >= 3}

    # 勝者プロファイル
    winner_profile = {}
    if winner_odds:
        winner_odds_s = sorted(winner_odds)
        winner_pops_s = sorted(winner_pops)
        n = len(winner_odds_s)
        winner_profile = {
            'median_odds': winner_odds_s[n // 2],
            'median_pop':  winner_pops_s[n // 2],
            'avg_odds':    round(sum(winner_odds_s) / n, 1),
            'avg_pop':     round(sum(winner_pops_s) / n, 1),
            'pop1_wins':   sum(1 for p in winner_pops if p == 1),
            'pop1_rate':   round(sum(1 for p in winner_pops if p == 1) / n, 3),
        }

    return {
        'n_years':            len(history),
        'pop_place_rate':     pop_place_rate,
        'odds_buckets':       odds_buckets,
        'winner_profile':     winner_profile,
        'f3rank_place_rate':  f3rank_rate,
        'corner_place_rate':  corner_rate,
    }


def _is_float(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _last_corner(corner_str: str, field_size: int):
    """通過順文字列から最終コーナー位置を返す (int or None)"""
    if not corner_str:
        return None
    parts = [p.strip() for p in re.split(r'[-→]', corner_str)]
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return None


def _corner_label(pos: int, field_size: int) -> str:
    """コーナー位置をラベルに変換"""
    if field_size <= 0:
        return '不明'
    ratio = pos / field_size
    if ratio <= 0.25:   return '逃げ'
    if ratio <= 0.50:   return '先行'
    if ratio <= 0.75:   return '差し'
    return '追い込み'


# ══════════════════════════════════════════════════════════
# レース特化スコアリング
# ══════════════════════════════════════════════════════════
def race_specific_candidates(horses: list, stats: dict, prev_history: dict) -> list:
    """
    過去統計に基づいてレース特化スコアを計算し、候補馬リストを返す。

    score = 人気複勝率 * 0.4 + オッズ帯複勝率 * 0.3 + 前走上がり率 * 0.2 + コーナー率 * 0.1
    """
    if not stats or not horses:
        return []

    pop_rate    = stats.get('pop_place_rate', {})
    odds_bkts   = stats.get('odds_buckets', [])
    f3rank_rate = stats.get('f3rank_place_rate', {})
    corner_rate = stats.get('corner_place_rate', {})

    # オッズ帯複勝率のルックアップ関数
    def odds_place_rate(odds):
        for bkt in odds_bkts:
            lo, hi = map(float, bkt['range'].split('-'))
            if lo <= odds < hi:
                return bkt['place_rate']
        return 0.0

    results = []
    for h in horses:
        pop  = h['popularity']
        odds = h['odds']
        ph   = prev_history.get(h['name'])

        s_pop   = pop_rate.get(pop, 0.2)
        s_odds  = odds_place_rate(odds)
        s_f3    = 0.3  # デフォルト（データなし）
        s_cnr   = 0.3  # デフォルト
        if ph:
            s_f3  = f3rank_rate.get(ph['f3rank'], 0.3)
            # 前走コーナー位置 → このレースでの傾向と照合
            last_c = ph.get('last_corner')
            if last_c and ph.get('field_size', 0) > 0:
                label = _corner_label(last_c, ph['field_size'])
                s_cnr = corner_rate.get(label, 0.3)

        score = s_pop * 0.4 + s_odds * 0.3 + s_f3 * 0.2 + s_cnr * 0.1
        results.append({
            'name':       h['name'],
            'odds':       odds,
            'pop':        pop,
            'score':      round(score, 4),
            'pop_rate':   round(s_pop, 3),
            'odds_rate':  round(s_odds, 3),
            'f3_rate':    round(s_f3, 3),
            'corner_rate': round(s_cnr, 3),
            'prev_f3rank': ph['f3rank']      if ph else None,
            'prev_finish': ph['finish_rank'] if ph else None,
        })

    results.sort(key=lambda x: -x['score'])
    return results


# ══════════════════════════════════════════════════════════
# メイン分析エントリポイント（predict.py から呼び出し）
# ══════════════════════════════════════════════════════════
def race_specific_analysis(race_name: str, horses: list,
                            prev_history: dict, data_dir: str = 'data') -> dict:
    """
    レース名に特化した分析を実行して結果を返す。

    Returns:
      {
        'race_name':  str,
        'n_years':    int,
        'stats':      dict,   # analyze_race_pattern の結果
        'candidates': list,   # race_specific_candidates の結果
        'no_data':    bool,   # 過去データが不足している場合 True
      }
    """
    history = load_race_history(race_name, data_dir)

    if len(history) < 2:
        return {
            'race_name': race_name,
            'n_years':   len(history),
            'stats':     {},
            'candidates': [],
            'no_data':   True,
        }

    stats      = analyze_race_pattern(history)
    candidates = race_specific_candidates(horses, stats, prev_history)

    return {
        'race_name':  race_name,
        'n_years':    len(history),
        'stats':      stats,
        'candidates': candidates,
        'no_data':    False,
    }


# ══════════════════════════════════════════════════════════
# 表示ヘルパー（predict.py 用）
# ══════════════════════════════════════════════════════════
def print_race_specific(result: dict, grade: str = '', top_n: int = 5):
    """race_specific_analysis の結果をコンソールに表示"""
    rn     = result['race_name']
    n      = result['n_years']
    grade_str = f'({grade})' if grade else ''

    print(f'\n  ━━ {rn}{grade_str} 特化分析  過去{n}年分 ━━')

    if result['no_data']:
        print(f'  ⚠️  過去データ不足（{n}年分のみ）。汎用分析のみ参照してください。')
        return

    stats = result['stats']
    wp    = stats.get('winner_profile', {})
    if wp:
        print(f'  過去勝者: 平均{wp["avg_pop"]:.1f}番人気 / 平均オッズ{wp["avg_odds"]:.1f}倍 '
              f'/ 1番人気勝率{wp["pop1_rate"]*100:.0f}%')

    print(f'  人気別複勝率: ' + '  '.join(
        f'{pop}番人気={v*100:.0f}%'
        for pop, v in sorted(stats.get('pop_place_rate', {}).items())
        if pop <= 6
    ))

    print()
    print(f'  【{rn} 特化スコア TOP{top_n}】')
    for i, c in enumerate(result['candidates'][:top_n], 1):
        f3_str  = f'前走上がり{c["prev_f3rank"]}位' if c['prev_f3rank'] else '前走データなし'
        fin_str = f'/{c["prev_finish"]}着' if c['prev_finish'] else ''
        print(f'  {i}. {c["pop"]:2d}番人気 {c["odds"]:5.1f}倍  {c["name"]}')
        print(f'     スコア:{c["score"]:.3f}  '
              f'(人気率:{c["pop_rate"]:.2f} / オッズ率:{c["odds_rate"]:.2f} / '
              f'上がり率:{c["f3_rate"]:.2f})  {f3_str}{fin_str}')

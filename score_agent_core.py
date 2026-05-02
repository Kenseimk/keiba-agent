"""
score_agent_core.py  スコアエージェント コアエンジン
======================================================
全馬を多因子スコア化して1-3着を予測する。

スコア因子 (重み):
  近走着順スコア      3.0  … 前5走を指数減衰で加重平均
  上がり3F相対スコア  2.0  … レース内順位 (速い=高スコア)
  騎手スコア          1.5  … jstats.csv の 0〜10 スコア
  同コース適性        1.5  … horse_course_stats.csv
  脚質スコア          1.5  … 通過順×コース・距離補正
  馬体重変動          0.5  … 直近変動 ±kg
  ── 追加因子 ──
  距離適性            *    … 同距離帯(±200m)での勝率
  馬場状態適性        *    … 同馬場状態(良/稍重/重/不良)での勝率
  前走着差            *    … 前走で何馬身差だったか
  休養間隔            *    … 前走からの月数
  同会場成績          *    … 同会場コードでの勝率
  勝率スコア          *    … 過去10走の勝率

最大理論値: 10 × Σ重み = 100 点
"""

import os, re, glob, csv
from collections import defaultdict

import pandas as pd

# ─── 除外キーワード (レース名に含まれたら対象外) ──────
EXCLUDE_CLASS = ['新馬', '未勝利', '1勝クラス', '500万下', '障害', 'ハードル', 'スティープル', 'JS']

# ─── スコア重みプリセット ──────────────────────────────
#
#  DEFAULT  : 初期固定重み（参考用）
#  SANPUKU  : 三連複的中率最大化 (6.9%)  ← グリッドサーチ結果
#  WIN      : 1着的中率最大化   (22.9%) ← グリッドサーチ結果
#
WEIGHT_PRESETS = {
    'default': {
        'recent_rank':   3.0,
        'agari_rank':    2.0,
        'jockey':        1.5,
        'course_fit':    1.5,
        'running_style': 1.5,
        'weight_change': 0.5,
        # 追加因子 (既存プリセットは無効化)
        'dist_fit':      0.0,
        'track_fit':     0.0,
        'last_margin':   0.0,
        'rest_interval': 0.0,
        'venue_fit':     0.0,
        'win_rate':      0.0,
    },
    'sanpuku': {                # 三連複的中率最大化
        'recent_rank':   2.0,
        'agari_rank':    2.0,
        'jockey':        2.0,
        'course_fit':    1.0,
        'running_style': 0.5,
        'weight_change': 0.0,
        'dist_fit':      0.0,
        'track_fit':     0.0,
        'last_margin':   0.0,
        'rest_interval': 0.0,
        'venue_fit':     0.0,
        'win_rate':      0.0,
    },
    'win': {                    # 1着的中率最大化 (旧)
        'recent_rank':   2.0,
        'agari_rank':    1.0,
        'jockey':        2.0,
        'course_fit':    1.5,
        'running_style': 1.0,
        'weight_change': 0.0,
        'dist_fit':      0.0,
        'track_fit':     0.0,
        'last_margin':   0.0,
        'rest_interval': 0.0,
        'venue_fit':     0.0,
        'win_rate':      0.0,
    },
    'win_v2': {                 # グリッドサーチ最適化済み (2015-2022学習 / 2023-2024検証80.6%)
        'recent_rank':   2.0,
        'agari_rank':    1.0,
        'jockey':        2.0,
        'course_fit':    1.5,
        'running_style': 1.0,
        'weight_change': 0.0,
        'dist_fit':      0.0,   # 距離適性 (効果なし)
        'track_fit':     0.0,   # 馬場状態適性 (データ不足)
        'last_margin':   1.0,   # 前走着差 (有効)
        'rest_interval': 1.5,   # 休養間隔 (有効)
        'venue_fit':     0.0,   # 同会場成績 (効果なし)
        'win_rate':      0.0,   # 勝率 (効果なし)
    },
}

# デフォルトで使う重み（score_all_horses のデフォルト引数）
WEIGHTS = WEIGHT_PRESETS['default']
WEIGHT_SUM = sum(WEIGHTS.values())

# ─── ヘルパー ──────────────────────────────────────────
def _float(v, default=None):
    try:   return float(v)
    except: return default

def parse_time(t: str):
    """'1:12.4' → 72.4秒  '72.4' → 72.4  失敗→None"""
    try:
        s = str(t).strip()
        if ':' in s:
            m, sec = s.split(':', 1)
            return int(m) * 60 + float(sec)
        return float(s)
    except:
        return None

def _int(v, default=None):
    try:   return int(v)
    except: return default

def parse_avg_pos(passage: str):
    """通過順文字列 → 平均ポジション  (例 '3-4-4-2' → 3.25)"""
    if not passage:
        return None
    parts = [float(x) for x in str(passage).split('-') if x.strip().isdigit()]
    return sum(parts) / len(parts) if parts else None

def parse_bw_change(weight_str: str) -> int:
    """'498(+4)' → 4,  '490(-2)' → -2,  '490(0)' → 0"""
    m = re.search(r'\(([+-]?\d+)\)', str(weight_str))
    return int(m.group(1)) if m else 0

def rank_to_score(rank: int) -> float:
    """着順 → 0〜10 スコア"""
    table = {1: 10.0, 2: 8.0, 3: 6.0, 4: 5.0, 5: 4.0}
    if rank in table:
        return table[rank]
    return max(0.0, 3.0 - (rank - 6) * 0.3)


def parse_margin(margin_str: str, rank: int) -> float:
    """
    着差文字列 → 馬身数 (1着は 0.0)
    例: '' → 0.0, 'クビ' → 0.2, '1' → 1.0, '1.1/2' → 1.5, '大差' → 20.0
    """
    if rank == 1 or not margin_str or str(margin_str).strip() in ('', '0', '---'):
        return 0.0
    s = str(margin_str).strip()
    if 'ハナ'  in s: return 0.05
    if 'アタマ' in s: return 0.1
    if 'クビ'  in s: return 0.2
    if '大差'  in s: return 20.0
    # "1.1/2" → "1+1/2", "1/2" → "0+1/2"
    s2 = s.replace('.', '+')
    total = 0.0
    for part in s2.split('+'):
        part = part.strip()
        if '/' in part:
            nums = part.split('/')
            try: total += float(nums[0]) / float(nums[1])
            except: pass
        else:
            try: total += float(part)
            except: pass
    return total if total > 0 else 1.0


# ─── 履歴DB構築 ───────────────────────────────────────
def load_history_db(data_dir: str = 'data') -> dict:
    """
    全 raceresults_YYYYMM.csv を読み込み、馬別の過去成績辞書を構築。
    各レース内で上がり3Fの相対順位も計算する。

    戻り値: {馬名: [record, ...]} (日付降順ソート済み)
    """
    # まず race_id ごとにグループ化
    race_db: dict[str, list] = defaultdict(list)

    for fpath in sorted(glob.glob(os.path.join(data_dir, 'raceresults_*.csv'))):
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                race_db[row['race_id']].append(row)

    horse_db: dict[str, list] = defaultdict(list)

    for race_id, horses in race_db.items():
        n_field = len(horses)

        # 上がり3F順位 (小さい=速い=上位)
        agari_list = []
        for h in horses:
            a = _float(h.get('上がり3F'))
            if a is not None:
                agari_list.append((h['馬名'], a))
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i + 1 for i, (name, _) in enumerate(agari_list)}
        n_agari = len(agari_list)

        for h in horses:
            rank = _int(h.get('着順'))
            if rank is None:
                continue   # 失格・除外等をスキップ

            margin_raw = h.get('着差', '').strip()
            record = {
                'race_id':     race_id,
                'race_ym':     race_id[:6],         # YYYYMM (ソートキー)
                'venue_code':  race_id[4:6],
                'race_num':    _int(race_id[10:12], 0),
                'rank':        rank,
                'field_size':  n_field,
                'jockey':      h.get('騎手', ''),
                'odds':        _float(h.get('単勝オッズ')),
                'pop':         _int(h.get('人気')),
                'agari':       _float(h.get('上がり3F')),
                'agari_rank':  agari_rank_map.get(h['馬名'], -1),
                'agari_field': n_agari,
                'avg_pos':     parse_avg_pos(h.get('通過順', '')),
                'bw_chg':      parse_bw_change(h.get('馬体重', '')),
                # 追加フィールド
                'dist':        _int(h.get('距離')),
                'track_cond':  h.get('馬場状態', '').strip(),
                'margin':      parse_margin(margin_raw, rank),
                'race_time':   parse_time(h.get('タイム', '')),
            }
            horse_db[h['馬名']].append(record)

    # 日付降順ソート (最新レースが先頭)
    for name in horse_db:
        horse_db[name].sort(key=lambda r: r['race_ym'], reverse=True)

    return dict(horse_db)


def load_models(data_dir: str = 'data'):
    """jstats.csv と horse_course_stats.csv を読み込んで返す"""
    jstats = pd.read_csv(
        os.path.join(data_dir, 'jstats.csv'),
        encoding='utf-8-sig', index_col='jockey'
    )
    # 騎手スコア調整: sqrt変換で圧縮 → 0〜10 スケール
    # ・パーセンタイル変換は分布を平坦化しすぎてシグナルが落ちた (ROI低下)
    # ・sqrt変換: 高スコアの伸びを緩やかに、低スコアの差は保持
    #   raw=10→10, raw=5→7.07, raw=2→4.47, raw=0.5→2.24
    _max_score = jstats['j_score'].max()
    jstats['j_pct'] = (jstats['j_score'] / _max_score).pow(0.5) * 10

    dc_db = pd.read_csv(
        os.path.join(data_dir, 'horse_course_stats.csv'),
        encoding='utf-8-sig'
    )
    return jstats, dc_db


# ─── 個別スコア関数 ────────────────────────────────────
_DECAY5 = [0.40, 0.25, 0.17, 0.11, 0.07]   # 前5走の指数減衰重み

def score_recent_rank(history: list, n: int = 5) -> float:
    """近走着順スコア  前n走を指数減衰で加重平均"""
    if not history:
        return 5.0
    tw = ts = 0.0
    for i, rec in enumerate(history[:n]):
        w = _DECAY5[i]
        s = rank_to_score(rec['rank'])
        tw += w;  ts += w * s
    return ts / tw if tw else 5.0


def score_agari_rank(history: list, n: int = 5) -> float:
    """上がり3F相対順位スコア  1位=10点, 最下位=0点 (線形)"""
    if not history:
        return 5.0
    tw = ts = 0.0
    for i, rec in enumerate(history[:n]):
        ar = rec.get('agari_rank', -1)
        nf = rec.get('agari_field', 12)
        if ar < 1:
            continue
        s = max(0.0, 10.0 * (1 - (ar - 1) / max(nf - 1, 1)))
        w = _DECAY5[i]
        tw += w;  ts += w * s
    return ts / tw if tw else 5.0


def score_running_style(history: list, course: str, dist: int, n: int = 5) -> float:
    """
    脚質スコア: 通過順から算出した平均ポジションをコース・距離で補正
      ダート:       先行 (avg_pos<=4) を 1.2倍
      長距離(2400+): 後ろからでも届く → 0.7掛け
      芝中距離:     標準 0.9倍
    """
    positions = [r['avg_pos'] for r in history[:n] if r.get('avg_pos') is not None]
    if not positions:
        return 5.0

    avg = sum(positions) / len(positions)
    est_field = 12
    raw = max(0.0, min(10.0, (est_field - avg + 1) / est_field * 10))

    is_dirt = 'ダ' in str(course)
    dist    = int(dist) if dist else 1800

    if is_dirt:
        adj = raw * 1.2 if avg <= 4 else raw * 0.85
    elif dist >= 2400:
        adj = raw * 0.7
    else:
        adj = raw * 0.9

    return min(10.0, max(0.0, adj))


def score_weight_change(history: list) -> float:
    """
    最直近の馬体重変動スコア
      ±4kg以内: 8.0  ±5〜8kg: 6.5〜7.0  9kg以上: 5.0以下
    """
    if not history:
        return 5.0
    bw = history[0].get('bw_chg', 0) or 0
    ab = abs(bw)
    if ab <= 4:   return 8.0
    if ab <= 8:   return 7.0 if bw > 0 else 6.5
    if ab <= 12:  return 5.0
    return 3.0


def score_dist_fit(history: list, dist: int, margin: int = 200) -> float:
    """距離適性: 同距離帯(±margin m)での勝率スコア"""
    if not history or not dist:
        return 5.0
    nearby = [r for r in history if r.get('dist') and abs(r['dist'] - dist) <= margin]
    if len(nearby) < 2:
        return 5.0
    wins = sum(1 for r in nearby if r['rank'] == 1)
    wr = wins / len(nearby)
    weight = min(len(nearby) / 5.0, 1.0)
    raw = wr * 10 + 2   # 0勝=2, 100%=12→clamp10
    return max(0.0, min(10.0, weight * raw + (1 - weight) * 5.0))


def score_track_fit(history: list, track_cond: str) -> float:
    """馬場状態適性: 同馬場状態(良/稍重/重/不良)での勝率・連対率スコア"""
    if not history or not track_cond:
        return 5.0
    same = [r for r in history if r.get('track_cond') == track_cond]
    if len(same) < 2:
        return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    weight = min(len(same) / 5.0, 1.0)
    raw = wins / len(same) * 8 + places / len(same) * 2 + 2
    return max(0.0, min(10.0, weight * raw + (1 - weight) * 5.0))


def score_last_margin(history: list) -> float:
    """前走着差スコア: 小さいほど高スコア (1着=10.0, 大差=1.0)"""
    if not history:
        return 5.0
    m = history[0].get('margin')
    if m is None:
        return 5.0
    if m <= 0.0:   return 10.0
    if m <= 0.1:   return 9.5
    if m <= 0.3:   return 9.0
    if m <= 0.5:   return 8.5
    if m <= 1.0:   return 8.0
    if m <= 2.0:   return 7.0
    if m <= 3.0:   return 6.0
    if m <= 5.0:   return 5.0
    if m <= 8.0:   return 4.0
    if m >= 20.0:  return 1.0
    return max(1.0, 5.0 - m * 0.3)


def score_rest_interval(history: list, current_ym: str) -> float:
    """
    休養間隔スコア: 前走からの月数差
      1ヶ月=9.0 (最適), 2ヶ月=8.0, 3-4ヶ月=7.0, 5-6ヶ月=6.0, 7ヶ月以上=4.0
    """
    if not history or not current_ym or len(current_ym) < 6:
        return 5.0
    last_ym = history[0].get('race_ym', '')
    if not last_ym or len(last_ym) < 6:
        return 5.0
    try:
        cy, cm = int(current_ym[:4]), int(current_ym[4:6])
        ly, lm = int(last_ym[:4]), int(last_ym[4:6])
        months = (cy - ly) * 12 + (cm - lm)
    except:
        return 5.0
    if months <= 0:  return 6.0
    if months == 1:  return 9.0
    if months == 2:  return 8.0
    if months <= 4:  return 7.0
    if months <= 6:  return 6.0
    return 4.0


def score_venue_fit(history: list, venue_code: str) -> float:
    """同会場成績スコア: 同venue_codeでの勝率"""
    if not history or not venue_code:
        return 5.0
    same = [r for r in history if r.get('venue_code') == venue_code]
    if len(same) < 2:
        return 5.0
    wins = sum(1 for r in same if r['rank'] == 1)
    weight = min(len(same) / 5.0, 1.0)
    raw = wins / len(same) * 10 + 2
    return max(0.0, min(10.0, weight * raw + (1 - weight) * 5.0))


def score_win_rate(history: list, n: int = 10) -> float:
    """過去N走の勝率スコア"""
    if not history:
        return 5.0
    recent = history[:n]
    wins = sum(1 for r in recent if r['rank'] == 1)
    wr = wins / len(recent)
    if wr >= 0.30: return 9.0
    if wr >= 0.20: return 7.5
    if wr >= 0.10: return 6.5
    if wr >= 0.05: return 5.5
    return 4.0


def score_jockey(jockey: str, jstats: pd.DataFrame) -> float:
    """騎手スコア: パーセンタイル変換済み (0〜10)。未登録は中央値相当の 5.0"""
    if jstats is not None and jockey in jstats.index:
        col = 'j_pct' if 'j_pct' in jstats.columns else 'j_score'
        return float(jstats.loc[jockey, col])
    return 5.0


def odds_fallback_score(pop: int, odds: float) -> float:
    """
    過去データなし馬のフォールバックスコア (近走着順・上がりの代替)
    人気・オッズから市場評価を逆算して 0〜10 に変換する。

    1番人気 → 9.0,  2番人気 → 7.5,  3番人気 → 6.5
    4〜5番人気 → 5.5〜5.0,  6番人気以下 → 徐々に低下
    """
    pop = int(pop or 99)
    table = {1: 9.0, 2: 7.5, 3: 6.5, 4: 5.5, 5: 5.0}
    if pop in table:
        return table[pop]
    return max(1.0, 5.0 - (pop - 5) * 0.35)


def score_course_fit(horse_name: str, course: str, dist: int, dc_db: pd.DataFrame) -> float:
    """
    同コース・同距離の過去勝率スコア
    サンプル数が少ない (n<5) ほど中立値 5.0 方向に引き寄せる
    """
    if dc_db is None:
        return 5.0
    course_str = 'ダート' if 'ダ' in str(course) else '芝'
    key = f"{int(dist)}_{course_str}"
    rows = dc_db[(dc_db['horse'] == horse_name) & (dc_db['dist_course'] == key)]
    if len(rows) == 0:
        return 5.0
    n  = float(rows['dc_n'].iloc[0])
    wr = float(rows['dc_wr'].iloc[0])
    if n == 0:
        return 5.0
    weight = min(n / 5.0, 1.0)
    raw    = wr * 10 + 2
    return max(0.0, min(10.0, weight * raw + (1 - weight) * 5.0))


# ─── 全馬スコア計算 ────────────────────────────────────
def score_all_horses(
    race_info: dict,
    horse_db:  dict,
    jstats:    pd.DataFrame,
    dc_db:     pd.DataFrame,
    weights:   dict = None,
    track_cond: str = '',
    venue_code: str = '',
    race_ym:    str = '',
) -> list:
    """
    race_info: fetch_race.py の出力形式
      {race_name, dist, course, n_horses, horses:[{name,jockey}], odds:[{name,pop,odds}]}
    戻り値: スコア降順の馬リスト
      [{name, jockey, odds, pop, score, n_races, breakdown}, ...]
    """
    w          = weights if weights is not None else WEIGHTS
    course     = race_info.get('course', 'ダート')
    dist       = int(race_info.get('dist', 1800) or 1800)
    track_cond = track_cond or race_info.get('track_cond', '')
    venue_code = venue_code or race_info.get('venue_code', '')
    race_ym    = race_ym    or race_info.get('race_ym', '')

    # 馬情報と人気/オッズを名前で統合
    horse_map = {h['name']: h for h in race_info.get('horses', [])}
    odds_map  = {h['name']: h for h in race_info.get('odds',   [])}
    all_names = set(list(horse_map.keys()) + list(odds_map.keys()))

    results = []
    for name in all_names:
        h_info = horse_map.get(name, {})
        o_info = odds_map.get(name, {})

        jockey = h_info.get('jockey') or o_info.get('jockey', '')
        odds   = o_info.get('odds', 0.0)
        pop    = o_info.get('pop', 99)

        history = horse_db.get(name, [])

        if not history:
            # 過去データなし → 人気・オッズで代替スコアを算出
            fb = odds_fallback_score(pop, odds)
            rr = fb
            ar = fb * 0.9    # 上がりは少し保守的に
            rs = 5.0         # 脚質不明
            wc = 5.0         # 体重変動不明
            df = 5.0; tf = 5.0; lm = 5.0; ri = 5.0; vf = 5.0; wr = 5.0
        else:
            rr = score_recent_rank(history)
            ar = score_agari_rank(history)
            rs = score_running_style(history, course, dist)
            wc = score_weight_change(history)
            df = score_dist_fit(history, dist)
            tf = score_track_fit(history, track_cond)
            lm = score_last_margin(history)
            ri = score_rest_interval(history, race_ym)
            vf = score_venue_fit(history, venue_code)
            wr = score_win_rate(history)
        js  = score_jockey(jockey, jstats)
        cf  = score_course_fit(name, course, dist, dc_db)
        wp  = horse_win_prob(history, dist)

        total = (
            rr * w['recent_rank']   +
            ar * w['agari_rank']    +
            js * w['jockey']        +
            cf * w['course_fit']    +
            rs * w['running_style'] +
            wc * w['weight_change'] +
            df * w.get('dist_fit',      0.0) +
            tf * w.get('track_fit',     0.0) +
            lm * w.get('last_margin',   0.0) +
            ri * w.get('rest_interval', 0.0) +
            vf * w.get('venue_fit',     0.0) +
            wr * w.get('win_rate',      0.0)
        )

        results.append({
            'name':       name,
            'jockey':     jockey,
            'odds':       float(odds or 0),
            'pop':        int(pop or 99),
            'score':      round(total, 1),
            'win_prob':   round(wp * 100, 1),   # % 表示用
            'n_races':    len(history),
            'no_history': len(history) == 0,
            'breakdown': {
                '近走着順':     round(rr, 1),
                '上がり順位':   round(ar, 1),
                '騎手':         round(js, 1),
                'コース適性':   round(cf, 1),
                '脚質':         round(rs, 1),
                '馬体重':       round(wc, 1),
                '距離適性':     round(df, 1),
                '馬場適性':     round(tf, 1),
                '前走着差':     round(lm, 1),
                '休養間隔':     round(ri, 1),
                '同会場':       round(vf, 1),
                '勝率':         round(wr, 1),
            },
        })

    results.sort(key=lambda x: x['score'], reverse=True)
    return results


# ─── ロジスティック回帰用 因子ベクトル ────────────────────
LOGISTIC_FACTORS = [
    'recent_rank', 'agari_rank', 'jockey', 'course_fit',
    'running_style', 'weight_change', 'dist_fit', 'last_margin',
    'rest_interval', 'venue_fit', 'win_rate', 'track_fit',
    'has_course_data',   # バイナリ: このコース・距離帯の履歴あり=1
    'course_time',       # 同コース・距離での最速タイム(秒) ※低い=速い、Noneはレース平均で補完
]

def score_course_time(history: list, dist: int, course: str, margin: int = 100):
    """
    同コース・同距離帯(±margin m)での最速タイム(秒)を返す。
    データなし → None（呼び出し側でレース平均に置換する）
    タイムは秒数そのまま返す（低い=速い=良い）。
    race_relative_features でのNaN処理に委ねる。
    """
    if not history:
        return None
    is_dirt = 'ダ' in str(course)
    same = [
        r['race_time'] for r in history
        if r.get('dist') and abs(r['dist'] - dist) <= margin
        and r.get('race_time') is not None
    ]
    return min(same) if same else None


def check_has_course_data(horse_name: str, course: str, dist: int, dc_db) -> float:
    """このコース・距離帯の履歴があれば 1.0, なければ 0.0"""
    if dc_db is None:
        return 0.0
    course_str = 'ダート' if 'ダ' in str(course) else '芝'
    key  = f"{int(dist)}_{course_str}"
    rows = dc_db[(dc_db['horse'] == horse_name) & (dc_db['dist_course'] == key)]
    if len(rows) == 0:
        return 0.0
    return 1.0 if float(rows['dc_n'].iloc[0]) > 0 else 0.0


def compute_horse_factors(
    name:       str,
    jockey:     str,
    course:     str,
    dist:       int,
    history:    list,
    jstats,
    dc_db,
    track_cond: str = '',
    venue_code: str = '',
    race_ym:    str = '',
    pop:        int = 99,
    odds:       float = 0.0,
) -> list:
    """
    ロジスティック回帰用の因子スコアベクトルを返す。
    LOGISTIC_FACTORS と同順。過去データなし馬はオッズ/人気でフォールバック。
    """
    hcd = check_has_course_data(name, course, dist, dc_db)
    ct  = score_course_time(history, dist, course)   # None の場合あり

    if not history:
        fb = odds_fallback_score(pop, odds)
        return [fb, fb * 0.9, score_jockey(jockey, jstats),
                score_course_fit(name, course, dist, dc_db),
                5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
                hcd, ct]   # ct=None → race_relative_features でレース平均に置換

    rr = score_recent_rank(history)
    ar = score_agari_rank(history)
    js = score_jockey(jockey, jstats)
    cf = score_course_fit(name, course, dist, dc_db)
    rs = score_running_style(history, course, dist)
    wc = score_weight_change(history)
    df = score_dist_fit(history, dist)
    lm = score_last_margin(history)
    ri = score_rest_interval(history, race_ym)
    vf = score_venue_fit(history, venue_code)
    wr = score_win_rate(history)
    tf = score_track_fit(history, track_cond)
    return [rr, ar, js, cf, rs, wc, df, lm, ri, vf, wr, tf, hcd, ct]


def horse_win_prob(
    history: list,
    dist: int = None,
    prior: float = 0.08,
) -> float:
    """
    馬の勝利確率を履歴から独立に推定する（合計100%にはならない）。

    - サンプルが少ない馬ほど prior（ベースライン≈1/12頭）に引き寄せるベイズ平滑化
    - 距離±300m条件の勝率で追加補正（3走以上ある場合のみ）
    - 戻り値: 0.0〜1.0 の確率（%ではなく小数）
    """
    if not history:
        return prior

    n    = len(history)
    wins = sum(1 for r in history if r['rank'] == 1)

    # ベイズ平滑化: 10走で実績を100%信頼、それ以下はpriorに引き寄せ
    alpha   = min(n / 10.0, 1.0)
    overall = wins / n
    prob    = alpha * overall + (1 - alpha) * prior

    # 距離帯補正（±300m, 3走以上）
    if dist:
        nearby = [r for r in history if r.get('dist') and abs(r['dist'] - dist) <= 300]
        if len(nearby) >= 3:
            dist_wins = sum(1 for r in nearby if r['rank'] == 1)
            dist_rate = dist_wins / len(nearby)
            beta = min(len(nearby) / 8.0, 1.0)
            prob = beta * dist_rate + (1 - beta) * prob

    return prob


def should_exclude(race_name: str) -> bool:
    """スコアエージェント対象外のレース名か判定"""
    return any(kw in str(race_name) for kw in EXCLUDE_CLASS)

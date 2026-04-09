"""
uscore.py  U score インスパイア型 期待値スコアリング
=====================================================
U score (@umasugikeiba) の分析哲学を参考に、
20の評価因子で各馬の「推定勝率」を算出し、
期待値（EV = 推定勝率 × オッズ）を主軸にした U_score を返す。

  U_score = EV × 100
  100 = 損益分岐点
  120以上 → 強推奨、110以上 → 推奨、100以上 → 参加圏

実装因子 (20):
  能力指数、加速力・瞬発力、騎手、コース適性、
  小回り適性、坂適性、馬場相性、枠適性、
  父母系血統(代替:前走レベル)、ローテーション、
  前走内容・レベル・位置・上がり、不完全燃焼度、
  脚質、PCI、野芝/洋芝適性、ダート適性、
  季節適性、遠征

使い方:
  from uscore import load_uscore_db, analyze_race_uscore
  horse_db = load_uscore_db('data')
  results  = analyze_race_uscore(race_info, horse_db, jstats, dc_db)
"""

import math, os, re, glob, csv
from collections import defaultdict

import pandas as pd

# ══════════════════════════════════════════════════════
# 会場メタデータ
# ══════════════════════════════════════════════════════

# 小回りコース (急カーブ・内枠有利傾向)
TIGHT_TRACKS  = {'01', '02', '03', '07', '10'}   # 札幌・函館・福島・中京・小倉

# ゴール前急坂あり会場
SLOPE_TRACKS  = {'06', '09'}                      # 中山・阪神

# 洋芝会場
YOSHIBA_TRACKS = {'01', '02'}                     # 札幌・函館

# 地方グループ (遠征判定)
REGION = {
    '01': 'north', '02': 'north',
    '03': 'east',  '04': 'east',  '05': 'east',  '06': 'east',
    '07': 'west',  '08': 'west',  '09': 'west',  '10': 'west',
}

# グレード換算スコア (前走レベル補正用)
GRADE_SCORE = {'G1': 10.0, 'G2': 8.5, 'G3': 7.5,
               'L': 7.0, 'OP': 6.5, '3勝': 6.0,
               '2勝': 5.0, '1勝': 4.0, '未勝利': 3.0, '新馬': 2.5}

# ══════════════════════════════════════════════════════
# グレード推定 (CSVのgradeが空の場合にレース名から推定)
# ══════════════════════════════════════════════════════

def _infer_grade_from_name(race_name: str) -> str:
    """レース名からグレード/クラスを推定する。gradeカラムが空の場合に使用。"""
    if not race_name or not race_name.strip():
        return '1勝'   # 名前なし = 低クラス (1勝クラス相当)

    n = race_name.strip()

    if '新馬' in n: return '新馬'
    if '未勝利' in n: return '未勝利'
    if '1勝クラス' in n or '500万' in n: return '1勝'
    if '2勝クラス' in n or '1000万' in n: return '2勝'
    if '3勝クラス' in n or '1600万' in n: return '3勝'

    _G1 = ['天皇賞', '有馬記念', '日本ダービー', '桜花賞', '菊花賞', 'オークス',
           '朝日杯', 'ホープフルS', 'スプリンターズS', 'マイルCS', 'エリザベス女王杯',
           '宝塚記念', 'ジャパンC', 'フェブラリーS', '高松宮記念', 'ヴィクトリアマイル',
           'NHKマイルC', '安田記念', '秋華賞', '皐月賞', 'チャンピオンズC', '東京大賞典',
           '帝王賞', 'かしわ記念', 'JBCクラシック', 'JBCスプリント', 'さきたま杯',
           '優駿牝馬', '阪神ジュベナイルF']
    if any(k in n for k in _G1): return 'G1'

    _G2 = ['中山記念', '京都記念', 'AJCC', '小倉大賞典', 'ダービー卿', 'スプリングS',
           'フローラS', '青葉賞', '毎日王冠', 'アルゼンチン共和国杯', '目黒記念',
           '函館記念', '札幌記念', '新潟記念', 'セントウルS', '神戸新聞杯',
           'ニュージーランドT', 'アーリントンC', 'きさらぎ賞', 'ローズS',
           'ラジオNIKKEI', '七夕賞', '関屋記念', 'エルムS', '共同通信杯',
           '京成杯', '東スポ杯', 'マイラーズC', '富士S', '中京記念',
           '福島記念', '新潟大賞典', '愛知杯', 'ダイヤモンドS', 'レパードS',
           'オールカマー', 'クイーンS', 'ターコイズS', 'カーネーションC']
    if any(k in n for k in _G2): return 'G2'

    _G3 = ['根岸S', 'チューリップ賞', 'フィリーズR', 'ファルコンS', '葵S',
           '函館スプリントS', '函館2歳S', '京阪杯', 'スワンS', '東京新聞杯',
           'クイーンC', 'シンザン記念', 'アルテミスS', '北九州短距離S',
           'ジュニアC', '牝馬S', 'エプソムC', 'プロキオンS', '朱鷺S',
           'BSN賞', 'TV西日本', 'CBC賞', '中京2歳S', '小倉2歳S', '函館SS']
    if any(k in n for k in _G3): return 'G3'

    # ステークス/S/C = OP相当 (OP戦に多いパターン)
    if any(k in n for k in ['ステークス', 'オープン']):
        return 'OP'
    if n.endswith('S') or n.endswith('C'):
        return 'OP'

    # 特別/賞/杯 = 3勝クラス相当に留める (G1〜低クラスまで幅広いため保守的に)
    return '3勝'


# ══════════════════════════════════════════════════════
# ヘルパー
# ══════════════════════════════════════════════════════

def _float(v, default=None):
    try:    return float(v)
    except: return default

def _int(v, default=0):
    try:    return int(v)
    except: return default

def clamp(v, lo=0.0, hi=10.0):
    return max(lo, min(hi, float(v)))

_DECAY5 = [0.40, 0.25, 0.17, 0.11, 0.07]

def weighted_avg(history, key_fn, n=5):
    """前n走を指数減衰で加重平均。値がNoneの走はスキップ"""
    tw = ts = 0.0
    for i, rec in enumerate(history[:n]):
        v = key_fn(rec)
        if v is None: continue
        w = _DECAY5[i] if i < len(_DECAY5) else 0.05
        tw += w; ts += w * v
    return ts / tw if tw > 0 else None

def _parse_bw(s):
    m = re.search(r'\(([+-]?\d+)\)', str(s))
    return int(m.group(1)) if m else 0

def _parse_margin(s, rank):
    if rank == 1 or not s or str(s).strip() in ('', '0', '---'): return 0.0
    s = str(s).strip()
    MAP = {'ハナ': 0.05, 'アタマ': 0.1, 'クビ': 0.2, '大差': 20.0, '大': 20.0}
    if s in MAP: return MAP[s]
    s2 = s.replace('.', '+')
    total = 0.0
    for part in s2.split('+'):
        part = part.strip()
        if '/' in part:
            a, b = part.split('/')
            try: total += float(a) / float(b)
            except: pass
        else:
            try: total += float(part)
            except: pass
    return total if total > 0 else 1.0

def _parse_avg_pos(passage):
    if not passage: return None
    parts = [float(x) for x in str(passage).split('-') if x.strip().isdigit()]
    return sum(parts) / len(parts) if parts else None

def _gate_band(g):
    g = _int(g, 5)
    if g <= 2: return 'inner'
    if g <= 5: return 'mid'
    return 'outer'

# ══════════════════════════════════════════════════════
# 拡張履歴DB ローダー
# ══════════════════════════════════════════════════════

def load_uscore_db(data_dir: str = 'data') -> dict:
    """
    raceresults_YYYYMM.csv を全て読み込み、馬別の過去成績辞書を構築。
    score_agent_core の load_history_db より多くのフィールドを取得する:
      course, gate_num, grade, margin, venue_code (場コード)
    """
    race_db = defaultdict(list)

    for fpath in sorted(glob.glob(os.path.join(data_dir, 'raceresults_*.csv'))):
        # ファイル名から YYYYMM を取得 (正確な月情報)
        fname = os.path.basename(fpath)
        m = re.search(r'raceresults_(\d{6})', fname)
        file_ym = m.group(1) if m else ''

        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                row['_file_ym'] = file_ym
                race_db[row['race_id']].append(row)

    horse_db = defaultdict(list)

    for race_id, horses in race_db.items():
        n_field = len(horses)

        # 上がり3F順位 (小さい=速い=1位)
        agari_list = [(h['馬名'], _float(h.get('上がり3F')))
                      for h in horses if _float(h.get('上がり3F')) is not None]
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i + 1 for i, (name, _) in enumerate(agari_list)}
        n_agari = len(agari_list)

        for h in horses:
            rank = _int(h.get('着順'))
            if rank == 0 or rank is None:
                continue  # 失格・除外等スキップ

            grade_raw = (h.get('grade') or '').strip()
            if not grade_raw:
                grade_raw = _infer_grade_from_name(h.get('race_name', ''))
            record = {
                'race_id':     race_id,
                'race_ym':     h['_file_ym'],          # YYYYMM (ファイル名由来)
                'venue_code':  (h.get('場コード') or '').strip().zfill(2),
                'rank':        rank,
                'field_size':  n_field,
                'jockey':      h.get('騎手', ''),
                'odds':        _float(h.get('単勝オッズ')),
                'pop':         _int(h.get('人気'), 0),
                'agari':       _float(h.get('上がり3F')),
                'agari_rank':  agari_rank_map.get(h['馬名'], -1),
                'agari_field': n_agari,
                'avg_pos':     _parse_avg_pos(h.get('通過順', '')),
                'bw_chg':      _parse_bw(h.get('馬体重', '')),
                'margin':      _parse_margin(h.get('着差', ''), rank),
                # 拡張フィールド
                'dist':        _int(h.get('距離')),
                'course':      (h.get('コース') or '').strip(),   # 芝 or ダート
                'track_cond':  (h.get('馬場状態') or '').strip(),
                'gate_num':    _int(h.get('枠番'), 0),
                'grade':       grade_raw,
                'trainer':     (h.get('調教師') or '').strip(),
            }
            horse_db[h['馬名']].append(record)

    # 日付降順ソート (最新が先頭)
    for name in horse_db:
        horse_db[name].sort(key=lambda r: r['race_ym'], reverse=True)

    return dict(horse_db)


# ══════════════════════════════════════════════════════
# 因子スコア関数 (各0〜10点)
# ══════════════════════════════════════════════════════

def f_ability(history: list) -> float:
    """能力指数: 近走着順を指数減衰で加重平均"""
    PTS = {1: 10.0, 2: 8.5, 3: 7.0, 4: 5.5, 5: 4.5}
    def pts(rec):
        r = rec.get('rank', 99)
        return PTS.get(r, clamp(4.0 - (r - 6) * 0.4, 0, 4))
    v = weighted_avg(history, pts)
    return v if v is not None else 5.0


def f_acceleration(history: list) -> float:
    """
    加速力・瞬発力: 上がり3Fレース内相対順位
    + 最速時の2位との差（絶対アドバンテージ）を加味
    """
    def agari_sc(rec):
        ar = rec.get('agari_rank', -1)
        nf = rec.get('agari_field', 12)
        if ar < 1: return None
        return clamp(10.0 * (1 - (ar - 1) / max(nf - 1, 1)))
    v = weighted_avg(history, agari_sc)
    return v if v is not None else 5.0


def f_trainer(trainer: str, trainer_stats: dict) -> float:
    """
    調教師スコア: 過去成績からの勝率・複勝率を評価 (0〜10)
    trainer_stats: {trainer_name: {'wr': 勝率, 'pr': 複勝率, 'n': 出走数}}
    """
    if not trainer or not trainer_stats:
        return 5.0
    stats = trainer_stats.get(trainer)
    if not stats or stats.get('n', 0) < 10:
        return 5.0
    wr = stats.get('wr', 0.0)   # 勝率
    pr = stats.get('pr', 0.0)   # 複勝率
    w  = min(stats['n'] / 100.0, 1.0)
    raw = wr * 8.0 + pr * 2.0 + 2.0
    return clamp(w * raw + (1 - w) * 5.0)


def build_trainer_stats(horse_db: dict) -> dict:
    """
    horse_db から調教師別の勝率・複勝率を集計する。
    戻り値: {trainer: {'n': 出走数, 'wr': 勝率, 'pr': 複勝率}}
    """
    from collections import defaultdict
    stats = defaultdict(lambda: {'n': 0, 'wins': 0, 'places': 0})
    for name, records in horse_db.items():
        for rec in records:
            t = rec.get('trainer', '').strip()
            if not t:
                continue
            stats[t]['n'] += 1
            if rec.get('rank') == 1:
                stats[t]['wins'] += 1
            if rec.get('rank', 99) <= 3:
                stats[t]['places'] += 1
    result = {}
    for t, s in stats.items():
        n = s['n']
        result[t] = {
            'n':  n,
            'wr': s['wins']  / n if n > 0 else 0.0,
            'pr': s['places'] / n if n > 0 else 0.0,
        }
    return result


def _lookup_jockey_stats(jockey: str, jstats: dict):
    """
    騎手名を jstats dict から検索。
    完全一致 → 前方一致（CSVフルネーム vs JSON略称の差を吸収）の順で試みる。
    """
    if not jockey or not jstats:
        return None
    s = jstats.get(jockey)
    if s:
        return s
    # 前方一致フォールバック (例: "横山和" → "横山和生")
    for full_name, val in jstats.items():
        if full_name.startswith(jockey) and len(full_name) > len(jockey):
            return val
    return None


def f_jockey(jockey: str, jstats) -> float:
    """
    騎手スコア: jstats (DataFrame or dict) から評価 (0〜10)
    dict形式: {jockey_name: {'n': 出走数, 'wr': 勝率, 'pr': 複勝率}}

    スコア目安:
      ルメール (wr≈0.25)  → ~9.5
      川田・武豊 (wr≈0.18) → ~7
      平均 (wr≈0.07)      → ~4.5
      下位 (wr≈0.03)      → ~2
    """
    if jstats is None or not jockey:
        return 4.5
    # DataFrame形式 (旧: jstats.csv)
    if hasattr(jstats, 'index'):
        if jockey in jstats.index:
            return clamp(float(jstats.loc[jockey, 'j_score']))
        return 4.5
    # dict形式 (新: build_jockey_stats の戻り値)
    stats = _lookup_jockey_stats(jockey, jstats)
    if not stats or stats.get('n', 0) < 20:
        return 4.5
    wr = stats.get('wr', 0.0)
    pr = stats.get('pr', 0.0)
    w  = min(stats['n'] / 200.0, 1.0)
    # wr 0.25 → ~10, wr 0.07 → ~4.5, wr 0.03 → ~2
    raw = wr * 30.0 + pr * 8.0
    return clamp(w * raw + (1 - w) * 4.5)


def build_jockey_stats(horse_db: dict) -> dict:
    """
    horse_db から騎手別の勝率・複勝率を集計する。
    戻り値: {jockey: {'n': 出走数, 'wr': 勝率, 'pr': 複勝率}}
    """
    from collections import defaultdict
    stats = defaultdict(lambda: {'n': 0, 'wins': 0, 'places': 0})
    for name, records in horse_db.items():
        for rec in records:
            j = rec.get('jockey', '').strip()
            if not j:
                continue
            stats[j]['n'] += 1
            if rec.get('rank') == 1:
                stats[j]['wins'] += 1
            if rec.get('rank', 99) <= 3:
                stats[j]['places'] += 1
    result = {}
    for j, s in stats.items():
        n = s['n']
        result[j] = {
            'n':  n,
            'wr': s['wins']  / n if n > 0 else 0.0,
            'pr': s['places'] / n if n > 0 else 0.0,
        }
    return result


def f_course_fit(name: str, course: str, dist: int, dc_db) -> float:
    """コース適性: 同コース・同距離の勝率スコア"""
    if dc_db is None: return 5.0
    course_str = 'ダート' if 'ダ' in str(course) else '芝'
    key = f"{int(dist)}_{course_str}"
    rows = dc_db[(dc_db['horse'] == name) & (dc_db['dist_course'] == key)]
    if len(rows) == 0: return 5.0
    n  = float(rows['dc_n'].iloc[0])
    wr = float(rows['dc_wr'].iloc[0])
    if n == 0: return 5.0
    w   = min(n / 5.0, 1.0)
    raw = wr * 10 + 2
    return clamp(w * raw + (1 - w) * 5.0)


def f_tight_track(history: list, venue_code: str) -> float:
    """小回り適性: 小回り会場(札幌/函館/福島/中京/小倉)での勝率・連対率"""
    if not history: return 5.0
    is_tight = venue_code in TIGHT_TRACKS
    same = [r for r in history if (r.get('venue_code') in TIGHT_TRACKS) == is_tight]
    if len(same) < 2: return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 4.0, 1.0)
    return clamp(w * (wins/len(same)*8 + places/len(same)*2 + 2) + (1-w) * 5.0)


def f_slope(history: list, venue_code: str) -> float:
    """坂適性: ゴール前急坂会場(中山/阪神)での成績"""
    if not history: return 5.0
    is_slope = venue_code in SLOPE_TRACKS
    same = [r for r in history if (r.get('venue_code') in SLOPE_TRACKS) == is_slope]
    if len(same) < 2: return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 4.0, 1.0)
    return clamp(w * (wins/len(same)*8 + places/len(same)*2 + 2) + (1-w) * 5.0)


def f_track_cond(history: list, track_cond: str) -> float:
    """
    馬場相性: 同馬場状態(良/稍重/重/不良)での成績
    同馬場が2戦未満の場合は「良系(良)」「重系(稍重/重/不良)」でグループ補完
    """
    if not history or not track_cond: return 5.0

    def _score(races):
        n = len(races)
        if n == 0: return None
        wins   = sum(1 for r in races if r['rank'] == 1)
        places = sum(1 for r in races if r['rank'] <= 3)
        w = min(n / 5.0, 1.0)
        return clamp(w * (wins/n*7 + places/n*3 + 2) + (1-w) * 5.0)

    # 完全一致
    exact = [r for r in history if r.get('track_cond') == track_cond]
    if len(exact) >= 2:
        return _score(exact)

    # グループ補完: 良系 vs 重系
    WET = {'稍重', '重', '不良'}
    is_wet = track_cond in WET
    group = [r for r in history if (r.get('track_cond','') in WET) == is_wet]
    s = _score(group)
    return s if s is not None else 5.0


def f_gate(history: list, gate_num: int) -> float:
    """枠適性: 同枠帯(内/中/外)での成績"""
    if not history or not gate_num: return 5.0
    target_band = _gate_band(gate_num)
    same = [r for r in history if _gate_band(r.get('gate_num', 5)) == target_band]
    if len(same) < 2: return 5.0
    wins = sum(1 for r in same if r['rank'] == 1)
    w = min(len(same) / 4.0, 1.0)
    return clamp(w * (wins/len(same)*10 + 2) + (1-w) * 5.0)


def f_rotation(history: list, current_ym: str) -> float:
    """
    ローテーション: 前走からの月数
    1ヶ月=9.5(ピーク), 連戦=5.5, 長期休養=3.5
    """
    if not history or not current_ym: return 5.0
    last_ym = history[0].get('race_ym', '')
    if not last_ym: return 5.0
    try:
        cy, cm = int(current_ym[:4]), int(current_ym[4:6])
        ly, lm = int(last_ym[:4]),    int(last_ym[4:6])
        months = (cy - ly) * 12 + (cm - lm)
    except: return 5.0
    if months <= 0: return 5.5    # 同月連戦
    if months == 1: return 9.5
    if months == 2: return 8.5
    if months <= 4: return 7.0
    if months <= 6: return 5.5
    if months <= 9: return 4.5
    return 3.5                    # 長期休養


def f_prev_content(history: list) -> float:
    """前走内容: 着順 + 着差の複合評価"""
    if not history: return 5.0
    rec    = history[0]
    rank   = rec.get('rank', 99)
    margin = rec.get('margin', 10.0)
    PTS = {1: 10.0, 2: 8.5, 3: 7.0, 4: 5.5, 5: 4.5}
    base  = PTS.get(rank, clamp(4.0 - (rank - 6) * 0.4, 0, 4))
    if   margin <= 0.2: bonus =  1.0
    elif margin <= 0.5: bonus =  0.5
    elif margin <= 1.0: bonus =  0.0
    elif margin <= 2.0: bonus = -0.5
    else:               bonus = -1.5
    return clamp(base + bonus)


def f_prev_level(history: list) -> float:
    """
    前走レベル: グレード列のみで評価。データなし=5.0(中立)
    G1=10, G2=8.5 ... 未勝利=3.0
    フィールドサイズは使わない（頭数≠レベルのため）
    """
    if not history: return 5.0
    grade = history[0].get('grade', '').strip()
    return GRADE_SCORE.get(grade, 5.0)


def f_prev_position(history: list) -> float:
    """前走位置: 前走コーナー通過順の相対評価"""
    if not history: return 5.0
    rec     = history[0]
    avg_pos = rec.get('avg_pos')
    if avg_pos is None: return 5.0
    fs = rec.get('field_size', 12)
    return clamp((fs - avg_pos) / max(fs - 1, 1) * 10)


def f_prev_agari(history: list) -> float:
    """前走上がり: 前走の上がり3F順位（1位=10点）"""
    if not history: return 5.0
    rec = history[0]
    ar  = rec.get('agari_rank', -1)
    nf  = rec.get('agari_field', 12)
    if ar < 1: return 5.0
    return clamp(10.0 * (1 - (ar - 1) / max(nf - 1, 1)))


def f_unfulfilled(history: list) -> float:
    """
    不完全燃焼度: 上がり上位なのに着順が悪かった馬
    → 次走巻き返し期待
    """
    if not history: return 5.0
    rec  = history[0]
    rank = rec.get('rank', 99)
    ar   = rec.get('agari_rank', -1)
    nf   = rec.get('agari_field', 12)
    if ar < 1: return 5.0
    agari_rel = (nf - ar) / max(nf - 1, 1)   # 1=最速
    if agari_rel >= 0.7 and rank > 6:  return 9.0   # 上がり上位30%かつ7着以下
    if agari_rel >= 0.5 and rank > 4:  return 7.5
    if agari_rel >= 0.3 and rank > 3:  return 6.5
    return 5.0


def f_running_style(history: list, course: str, dist: int) -> float:
    """脚質: 平均ポジション × コース・距離補正"""
    positions = [r['avg_pos'] for r in history[:5] if r.get('avg_pos') is not None]
    if not positions: return 5.0
    avg = sum(positions) / len(positions)
    est = 12
    raw = clamp((est - avg + 1) / est * 10)
    is_dirt = 'ダ' in str(course)
    dist_i  = _int(dist, 1800)
    if is_dirt:    adj = raw * 1.2 if avg <= 4 else raw * 0.85
    elif dist_i >= 2400: adj = raw * 0.7
    else:          adj = raw * 0.9
    return clamp(adj)


def f_pci(history: list) -> float:
    """
    PCI (Pace Change Index): 先行しながら上がりも速い=高スコア
    上がり相対スコア × 0.6 + 位置取り相対スコア × 0.4
    """
    if not history: return 5.0
    rec = history[0]
    ar, nf  = rec.get('agari_rank', -1), rec.get('agari_field', 12)
    avg_pos = rec.get('avg_pos')
    if ar < 1 or avg_pos is None: return 5.0
    agari_rel = (nf - ar) / max(nf - 1, 1)
    pos_rel   = (nf - avg_pos) / max(nf - 1, 1)
    return clamp((agari_rel * 0.6 + pos_rel * 0.4) * 10)


def f_grass_type(history: list, venue_code: str) -> float:
    """野芝/洋芝適性: 洋芝会場(札幌・函館)での実績"""
    if not history: return 5.0
    is_yoshi = venue_code in YOSHIBA_TRACKS
    same = [r for r in history if (r.get('venue_code') in YOSHIBA_TRACKS) == is_yoshi]
    if len(same) < 2: return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 4.0, 1.0)
    return clamp(w * (wins/len(same)*8 + places/len(same)*2 + 2) + (1-w) * 5.0)


def f_dirt_fit(history: list, course: str) -> float:
    """ダート/芝適性: 今走の馬場種別での通算成績"""
    if not history: return 5.0
    is_dirt = 'ダ' in str(course)
    same = [r for r in history
            if ('ダ' in str(r.get('course', ''))) == is_dirt]
    if len(same) < 2: return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 5.0, 1.0)
    return clamp(w * (wins/len(same)*8 + places/len(same)*2 + 2) + (1-w) * 5.0)


def _dist_zone(dist: int) -> str:
    """距離帯区分"""
    if dist <= 1400:  return 'sprint'    # 短距離 〜1400m
    if dist <= 1800:  return 'mile'      # マイル 1401〜1800m
    if dist <= 2200:  return 'middle'    # 中距離 1801〜2200m
    return 'long'                        # 長距離 2201m〜


def f_dist_fit(history: list, dist: int) -> float:
    """
    距離帯適性: 同距離帯（短距離/マイル/中距離/長距離）での成績
    完全一致距離でも補完（±200m以内）も試みる
    """
    if not history or not dist: return 5.0
    zone = _dist_zone(dist)

    # 同距離帯の全戦績
    same_zone = [r for r in history if _dist_zone(r.get('dist') or 0) == zone]
    if len(same_zone) < 2:
        return 5.0

    wins   = sum(1 for r in same_zone if r['rank'] == 1)
    places = sum(1 for r in same_zone if r['rank'] <= 3)
    w = min(len(same_zone) / 4.0, 1.0)

    # 完全一致距離での勝率でボーナス
    exact = [r for r in same_zone if abs((r.get('dist') or 0) - dist) <= 100]
    if len(exact) >= 2:
        ex_wins = sum(1 for r in exact if r['rank'] == 1)
        ex_wr   = ex_wins / len(exact)
        bonus   = ex_wr * 2.0   # 最大+2点
    else:
        bonus = 0.0

    raw = wins/len(same_zone)*8 + places/len(same_zone)*2 + 2 + bonus
    return clamp(w * raw + (1-w) * 5.0)


def f_place_ability(history: list) -> float:
    """
    連対特化能力: 2着に最高点、3着も重視するスコア
    ◎ではなく○を選ぶ際の補助スコアとして使う
    """
    # 2着=10, 3着=9, 1着=7（勝ち過ぎは相手が弱い可能性）, 以下減点
    PTS = {1: 7.0, 2: 10.0, 3: 9.0, 4: 5.5, 5: 4.0}
    def pts(rec):
        r = rec.get('rank', 99)
        return PTS.get(r, clamp(3.0 - (r - 6) * 0.4, 0, 3))
    v = weighted_avg(history, pts)
    return v if v is not None else 5.0


def f_season(history: list, current_ym: str) -> float:
    """季節適性: 同季節(±1ヶ月)での成績"""
    if not history or not current_ym: return 5.0
    try: cur_m = int(current_ym[4:6])
    except: return 5.0
    def same_season(ym):
        try:
            m    = int(ym[4:6])
            diff = abs(m - cur_m)
            return min(diff, 12 - diff) <= 1
        except: return False
    same = [r for r in history if same_season(r.get('race_ym', ''))]
    if len(same) < 2: return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 4.0, 1.0)
    return clamp(w * (wins/len(same)*8 + places/len(same)*2 + 2) + (1-w) * 5.0)


def f_jockey_horse_fit(history: list, jockey: str) -> float:
    """
    騎手×馬の相性: 同じ騎手とのコンビ時の成績
    3戦以上の組み合わせがあれば勝率・連対率を評価
    """
    if not history or not jockey:
        return 5.0
    same = [r for r in history if r.get('jockey', '') == jockey]
    if len(same) < 2:
        return 5.0
    wins   = sum(1 for r in same if r['rank'] == 1)
    places = sum(1 for r in same if r['rank'] <= 3)
    w = min(len(same) / 4.0, 1.0)
    score = w * (wins / len(same) * 8.0 + places / len(same) * 3.0 + 2.0) + (1 - w) * 5.0
    return clamp(score)


def f_weight_fit(history: list, bw_now: float) -> float:
    """
    体重最適範囲: 過去の勝利時・好走時の体重と今回の体重を比較
    馬体重が好走時の範囲内なら加点、大きく外れていたら減点
    bw_now: 今回の馬体重 (kg)。Noneまたは0なら5.0返す
    """
    if not history or not bw_now or bw_now <= 0:
        return 5.0
    # 好走時(3着以内)の体重を収集
    good_weights = [
        r['bw'] for r in history
        if r.get('rank', 99) <= 3 and r.get('bw') and r['bw'] > 0
    ]
    if len(good_weights) < 2:
        return 5.0
    avg = sum(good_weights) / len(good_weights)
    std = (sum((x - avg) ** 2 for x in good_weights) / len(good_weights)) ** 0.5
    std = max(std, 4.0)   # 最低4kg の余裕
    diff = abs(bw_now - avg)
    if diff <= std * 0.5:
        return 8.0    # ベスト体重圏内
    elif diff <= std * 1.0:
        return 6.5    # やや外れ
    elif diff <= std * 2.0:
        return 4.0    # 大きく外れ
    else:
        return 2.5    # 過去最大乖離


def f_weight_carried(history: list, kg_now: float) -> float:
    """
    斤量変化: 前走からの斤量増減を評価
    増量なら減点、同斤なら中立、減量なら加点
    kg_now: 今回の斤量 (None なら 5.0)
    """
    if not history or not kg_now or kg_now <= 0:
        return 5.0
    prev_kg = history[0].get('weight_carried') or history[0].get('kg')
    if not prev_kg or prev_kg <= 0:
        return 5.0
    delta = kg_now - prev_kg
    if delta <= -2.0:
        return 8.0    # 2kg以上減量 → 有利
    elif delta < 0:
        return 7.0    # 1kg減量
    elif delta == 0:
        return 5.5    # 同斤
    elif delta <= 1.0:
        return 4.0    # 1kg増量
    else:
        return 2.5    # 2kg以上増量 → 不利


def f_training(horse_name: str, oikiri_db: dict) -> float:
    """
    調教スコア: fetch_oikiri.py で取得した oikiri_db を使用 (0〜10)
    評価 S→9.5, A→7.5, B→6.0, C→4.0, D→2.5, 不明→5.0
    坂路タイム補正あり (詳細は fetch_oikiri.oikiri_score 参照)
    """
    if not oikiri_db:
        return 5.0
    try:
        from fetch_oikiri import oikiri_score
        return oikiri_score(horse_name, oikiri_db)
    except ImportError:
        return 5.0


def f_travel(history: list, venue_code: str) -> float:
    """
    遠征: 前走会場との関係
    同会場継続=8.0, 同地方=6.5, 遠征=5.0
    """
    if not history: return 5.0
    prev_venue = history[0].get('venue_code', '')
    if not prev_venue: return 5.0
    if prev_venue == venue_code: return 8.0
    if REGION.get(prev_venue) == REGION.get(venue_code): return 6.5
    return 5.0


def _odds_fallback(pop: int, odds: float) -> float:
    """過去データなし馬の能力推定 (人気から代替)"""
    pop = _int(pop, 99)
    TABLE = {1: 9.0, 2: 7.5, 3: 6.5, 4: 5.5, 5: 5.0}
    return TABLE.get(pop, max(1.0, 5.0 - (pop - 5) * 0.3))


# ══════════════════════════════════════════════════════
# 重みプリセット
# ══════════════════════════════════════════════════════

USCORE_WEIGHTS = {
    'ability':          3.0,   # 能力指数
    'acceleration':     2.5,   # 加速力・瞬発力
    'training':         2.5,   # 調教評価 (oikiri_db がある場合のみ有効)
    'jockey':           2.0,   # 騎手
    'trainer':          2.0,   # 調教師
    'course_fit':       2.0,   # コース適性
    'prev_content':     2.0,   # 前走内容
    'unfulfilled':      1.5,   # 不完全燃焼度
    'track_cond':       1.5,   # 馬場相性
    'running_style':    1.5,   # 脚質
    'rotation':         1.5,   # ローテーション
    'prev_agari':       1.5,   # 前走上がり
    'prev_level':       1.0,   # 前走レベル
    'prev_position':    1.0,   # 前走位置
    'tight_track':      1.0,   # 小回り適性
    'slope':            1.0,   # 坂適性
    'grass_type':       1.0,   # 野芝/洋芝適性
    'dirt_fit':         1.0,   # ダート適性
    'pci':              1.0,   # PCI
    'gate':             0.5,   # 枠適性
    'season':           0.5,   # 季節適性
    'travel':           0.5,   # 遠征
    'jockey_horse_fit': 1.5,   # 騎手×馬の相性
    'weight_fit':       1.0,   # 体重最適範囲
    'weight_carried':   0.5,   # 斤量変化
    'dist_fit':         1.5,   # 距離帯適性
}

_WSUM = sum(USCORE_WEIGHTS.values())


# ══════════════════════════════════════════════════════
# メイン計算
# ══════════════════════════════════════════════════════

def calc_horse_factors(
    name, jockey, odds_val, pop, gate_num,
    history, jstats, dc_db,
    course, dist, track_cond, venue_code, race_ym,
    trainer_stats:   dict  = None,
    jockey_stats:    dict  = None,
    oikiri_db:       dict  = None,
    bw_now:          float = None,   # 今回の馬体重 (kg)
    kg_now:          float = None,   # 今回の斤量 (kg)
) -> dict:
    """1頭分の全因子スコアを計算して返す"""
    has = len(history) > 0
    fb  = _odds_fallback(pop, odds_val)
    trainer = history[0].get('trainer', '') if has else ''

    factors = {
        'ability':          f_ability(history)                       if has else fb,
        'acceleration':     f_acceleration(history)                  if has else fb * 0.9,
        'training':         f_training(name, oikiri_db),
        'jockey':           f_jockey(jockey, jockey_stats if jockey_stats is not None else jstats),
        'trainer':          f_trainer(trainer, trainer_stats),
        'course_fit':       f_course_fit(name, course, dist, dc_db),
        'prev_content':     f_prev_content(history)                  if has else fb * 0.9,
        'unfulfilled':      f_unfulfilled(history)                   if has else 5.0,
        'track_cond':       f_track_cond(history, track_cond)        if has else 5.0,
        'running_style':    f_running_style(history, course, dist)   if has else 5.0,
        'rotation':         f_rotation(history, race_ym)             if has else 5.0,
        'prev_agari':       f_prev_agari(history)                    if has else 5.0,
        'prev_level':       f_prev_level(history)                    if has else 5.0,
        'prev_position':    f_prev_position(history)                 if has else 5.0,
        'tight_track':      f_tight_track(history, venue_code)       if has else 5.0,
        'slope':            f_slope(history, venue_code)             if has else 5.0,
        'grass_type':       f_grass_type(history, venue_code)        if has else 5.0,
        'dirt_fit':         f_dirt_fit(history, course)              if has else 5.0,
        'pci':              f_pci(history)                           if has else 5.0,
        'gate':             f_gate(history, gate_num)                if has else 5.0,
        'season':           f_season(history, race_ym)               if has else 5.0,
        'travel':           f_travel(history, venue_code)            if has else 5.0,
        'jockey_horse_fit': f_jockey_horse_fit(history, jockey)      if has else 5.0,
        'weight_fit':       f_weight_fit(history, bw_now)            if has else 5.0,
        'weight_carried':   f_weight_carried(history, kg_now)        if has else 5.0,
        'dist_fit':         f_dist_fit(history, dist)                if has else 5.0,
    }

    raw_score  = sum(USCORE_WEIGHTS[k] * v for k, v in factors.items())
    norm_score = raw_score / _WSUM   # 0〜10

    # 出走数が少ない馬は平均(5.0)に回帰させる (5戦未満で信頼度低下)
    HISTORY_CONFIDENCE_MIN = 5
    n_races = len(history)
    confidence = min(n_races / HISTORY_CONFIDENCE_MIN, 1.0)
    norm_score = norm_score * confidence + 5.0 * (1.0 - confidence)

    return {
        'name':       name,
        'jockey':     jockey,
        'odds':       float(odds_val or 0),
        'pop':        _int(pop, 99),
        'gate_num':   gate_num,
        'n_races':    n_races,
        'norm_score': round(norm_score, 3),
        'factors':    {k: round(v, 2) for k, v in factors.items()},
    }


def _softmax_probs(scores: list, temperature: float = 2.0) -> list:
    """
    スコア列をsoftmax変換して確率に変換。
    temperature が大きいほど平均化される（デフォルト2.0）
    """
    scaled = [s / temperature for s in scores]
    mx     = max(scaled)
    exps   = [math.exp(s - mx) for s in scaled]
    total  = sum(exps)
    return [e / total for e in exps]


def analyze_race_uscore(
    race_info:     dict,
    horse_db:      dict,
    jstats,
    dc_db,
    temperature:   float = 1.5,
    market_alpha:  float = 0.4,
    trainer_stats: dict  = None,
    jockey_stats:  dict  = None,
    oikiri_db:     dict  = None,
) -> list:
    """
    レース内全馬のU scoreを算出する。

    引数:
      race_info    : fetch_race.py の出力形式
      horse_db     : load_uscore_db() の返り値
      jstats       : jstats.csv (DataFrame, index=jockey) または None
      dc_db        : horse_course_stats.csv (DataFrame) または None
      temperature  : softmax温度 (デフォルト0.8 / 低いほど差が出る)
      market_alpha : 市場オッズの混合割合 0.0〜1.0 (デフォルト0.3)
                     0.0=モデルのみ / 1.0=市場オッズのみ
      jockey_stats : build_jockey_stats() の戻り値 (dict)

    戻り値: U score 降順の馬リスト
      [{name, jockey, odds, pop, win_prob, ev, u_score, norm_score, breakdown}, ...]
    """
    course     = race_info.get('course', 'ダート')
    dist       = _int(race_info.get('dist', 1800), 1800)
    track_cond = race_info.get('track_cond', '')
    # venue_code: JSON にない場合は race_id[4:6] から取得
    race_id    = race_info.get('race_id', '')
    venue_code = (race_info.get('venue_code') or
                  (race_id[4:6] if len(race_id) >= 6 else '')).strip().zfill(2)
    # race_ym: JSON にない場合は race_id 先頭4桁(年) + race_info の file_ym から
    race_ym    = (race_info.get('race_ym') or
                  race_info.get('_file_ym') or
                  race_id[:4] + '01')

    horse_map = {h['name']: h for h in race_info.get('horses', [])}
    odds_map  = {h['name']: h for h in race_info.get('odds',   [])}
    all_names = list(set(list(horse_map.keys()) + list(odds_map.keys())))

    horse_data = []
    for name in all_names:
        h_info   = horse_map.get(name, {})
        o_info   = odds_map.get(name, {})
        jockey   = h_info.get('jockey') or o_info.get('jockey', '')
        odds_val = float(o_info.get('odds') or 0)
        pop      = _int(o_info.get('pop'), 99)
        gate_num = _int(h_info.get('gate_num') or h_info.get('umaban'), 0)
        bw_now   = _float(h_info.get('bw') or h_info.get('body_weight'))
        kg_now   = _float(h_info.get('kg') or h_info.get('weight_carried'))
        history  = horse_db.get(name, [])

        hd = calc_horse_factors(
            name, jockey, odds_val, pop, gate_num,
            history, jstats, dc_db,
            course, dist, track_cond, venue_code, race_ym,
            trainer_stats=trainer_stats,
            jockey_stats=jockey_stats,
            oikiri_db=oikiri_db,
            bw_now=bw_now,
            kg_now=kg_now,
        )
        horse_data.append(hd)

    if not horse_data:
        return []

    # ── モデルスコアから推定勝率 ───────────────────────
    norm_scores  = [h['norm_score'] for h in horse_data]
    model_probs  = _softmax_probs(norm_scores, temperature)

    # ── 連対特化スコア（○選定用）────────────────────────
    # f_place_ability を使った連対重視スコアを計算
    place_raw = []
    for h in horse_data:
        hist = horse_db.get(h['name'], [])
        pa   = f_place_ability(hist) if hist else 5.0
        place_raw.append(pa)
    place_probs = _softmax_probs(place_raw, temperature)

    # ── 市場オッズから暗示勝率 (vig除去後に正規化) ──────
    raw_odds = [h['odds'] for h in horse_data]
    inv_odds = [1.0 / o if o > 0 else 0.0 for o in raw_odds]
    total_inv = sum(inv_odds) or 1.0
    market_probs = [v / total_inv for v in inv_odds]   # vig除去済み正規化

    # ── ベイズ混合: α × 市場 + (1-α) × モデル ──────────
    blended_probs = [
        market_alpha * mp + (1 - market_alpha) * sp
        for mp, sp in zip(market_probs, model_probs)
    ]

    # EV計算用オッズ上限 (高オッズ馬のスコア膨張を防ぐ)
    EV_ODDS_CAP = 25.0

    results = []
    for i, (h, prob) in enumerate(zip(horse_data, blended_probs)):
        raw_ev  = prob * h['odds'] if h['odds'] > 0 else 0.0
        ev_odds = min(h['odds'], EV_ODDS_CAP) if h['odds'] > 0 else 0.0
        ev      = prob * ev_odds
        u_score = round(ev * 100, 1)   # 100 = 損益分岐点

        results.append({
            'name':        h['name'],
            'jockey':      h['jockey'],
            'odds':        h['odds'],
            'pop':         h['pop'],
            'gate_num':    h['gate_num'],
            'n_races':     h['n_races'],
            'win_prob':    round(prob * 100, 1),           # 混合後勝率
            'model_prob':  round(model_probs[i] * 100, 1),  # モデルのみ
            'market_prob': round(market_probs[i] * 100, 1), # 市場のみ
            'place_prob':  round(place_probs[i] * 100, 1),  # 連対特化スコア
            'ev':          round(raw_ev, 3),
            'ev_capped':   round(ev, 3),
            'u_score':     u_score,
            'norm_score':  h['norm_score'],
            'breakdown':   h['factors'],
        })

    results.sort(key=lambda x: x['u_score'], reverse=True)
    return results


# ══════════════════════════════════════════════════════
# ベット推奨判定
# ══════════════════════════════════════════════════════

BET_THRESHOLD = {
    'strong': 120,   # 強推奨 (EV 1.2以上)
    'normal': 110,   # 推奨  (EV 1.1以上)
    'entry':  100,   # 参加圏 (EV 1.0以上)
}

# オッズが高すぎる馬は市場の「実力外」評価を尊重して除外
BET_ODDS_MAX = 30.0   # 30倍超はベット推奨対象外
BET_ODDS_MIN =  2.0   # 2倍未満（過大人気）も除外

def check_bet_uscore(results: list) -> dict | None:
    """
    U score 上位馬のベット推奨を判定。
    条件:
      - U score 100以上
      - 2位との差 10pt以上
      - オッズ 2.0〜30.0倍
    """
    if len(results) < 2:
        return None
    top = results[0]
    gap = top['u_score'] - results[1]['u_score']

    if top['u_score'] < BET_THRESHOLD['entry']:
        return None
    if gap < 10.0:
        return None
    if not (BET_ODDS_MIN <= top['odds'] <= BET_ODDS_MAX):
        return None

    level = ('強推奨' if top['u_score'] >= BET_THRESHOLD['strong']
             else '推奨' if top['u_score'] >= BET_THRESHOLD['normal']
             else '参加圏')
    return {
        'horse':        top['name'],
        'odds':         top['odds'],
        'pop':          top['pop'],
        'win_prob':     top['win_prob'],
        'model_prob':   top.get('model_prob', 0),
        'market_prob':  top.get('market_prob', 0),
        'u_score':      top['u_score'],
        'ev':           top['ev'],
        'gap':          round(gap, 1),
        'level':        level,
    }


# ══════════════════════════════════════════════════════
# コンソール表示
# ══════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════
# クラスフィルタ
# ══════════════════════════════════════════════════════

EXCLUDE_CLASS = ['新馬', '未勝利', '1勝クラス', '500万下',
                 '障害', 'ハードル', 'スティープル', 'JS']

def should_exclude_uscore(race_name: str) -> bool:
    """新馬・未勝利・1勝クラス・障害は対象外"""
    return any(kw in str(race_name) for kw in EXCLUDE_CLASS)


FACTOR_JA = {
    'ability':       '能力指数',
    'acceleration':  '加速力/瞬発力',
    'jockey':        '騎手',
    'course_fit':    'コース適性',
    'prev_content':  '前走内容',
    'unfulfilled':   '不完全燃焼度',
    'track_cond':    '馬場相性',
    'running_style': '脚質',
    'rotation':      'ローテーション',
    'prev_agari':    '前走上がり',
    'prev_level':    '前走レベル',
    'prev_position': '前走位置',
    'tight_track':   '小回り適性',
    'slope':         '坂適性',
    'grass_type':    '野芝/洋芝',
    'dirt_fit':      'ダート適性',
    'pci':           'PCI',
    'gate':          '枠適性',
    'season':        '季節適性',
    'travel':        '遠征',
}


N_DELTA_MARKS = 4   # △の頭数（win_prob 5〜8位）


def assign_marks(results: list) -> dict:
    """
    win_prob 降順で ◎○▲☆△ を割り当てる。
    戻り値: {馬名: 印文字}
    """
    sorted_by_wp = sorted(results, key=lambda x: x['win_prob'], reverse=True)
    marks = {}
    for i, h in enumerate(sorted_by_wp):
        if i == 0:
            marks[h['name']] = '◎'
        elif i == 1:
            marks[h['name']] = '○'
        elif i == 2:
            marks[h['name']] = '▲'
        elif i == 3:
            marks[h['name']] = '☆'
        elif i < 4 + N_DELTA_MARKS:
            marks[h['name']] = '△'
        else:
            marks[h['name']] = ''
    return marks


def format_sanrentan_tickets(marks: dict) -> str:
    """
    PDFパターンCの三連単マルチ流し買い目を文字列で返す。
    C1: ◎○→▲△☆  C2: ◎▲→△☆
    """
    honmei  = [n for n, m in marks.items() if m == '◎']
    rentan  = [n for n, m in marks.items() if m in ('◎', '○')]
    sankaku = [n for n, m in marks.items() if m in ('▲', '☆', '△')]
    hosi    = [n for n, m in marks.items() if m in ('☆', '△')]
    sante_c2 = [n for n, m in marks.items() if m in ('▲',)]
    c1_axis  = sorted(rentan)
    c2_axis  = honmei + sante_c2

    lines = []
    if len(c1_axis) >= 2 and sankaku:
        s = '/'.join(c1_axis)
        t = '/'.join(sankaku)
        lines.append(f'C1: [{s}]→[{t}] マルチ ({2*len(sankaku)}通)')
    if len(c2_axis) >= 2 and hosi:
        s = '/'.join(c2_axis)
        t = '/'.join(hosi)
        lines.append(f'C2: [{s}]→[{t}] マルチ ({2*len(hosi)}通)')
    return '\n'.join(lines)


def print_uscore_result(race_info: dict, results: list) -> None:
    race_name = (race_info.get('race_name') or '').strip()
    dist      = race_info.get('dist', 0)
    course    = race_info.get('course', '?')
    n         = race_info.get('n_horses', len(results))
    rnum      = race_info.get('rnum', '?')
    race_id   = race_info.get('race_id', '')

    if not race_name:
        race_name = f'{rnum}R ({race_id})'

    marks = assign_marks(results)
    # win_prob 降順で表示
    results_by_wp = sorted(results, key=lambda x: x['win_prob'], reverse=True)

    print(f'\n{"="*76}')
    print(f'  {rnum}R  {race_name}  {course}{dist}m  {n}頭')
    print(f'  [U score]  印=win_prob順  U=EV×100 (100=損益分岐点)')
    print(f'{"="*76}')
    print(f'  {"印":2} {"馬名":14} {"騎手":8} {"人気":>4} {"オッズ":>6}  '
          f'{"勝率%":>6}  {"市場%":>6}  {"Uscore":>7}  {"走数"}')
    print(f'  {"-"*78}')

    for h in results_by_wp[:10]:
        mark = marks.get(h['name'], '')
        nr   = f'({h["n_races"]}走)' if h['n_races'] > 0 else '(履歴なし)'
        print(f'  {mark:2} {h["name"]:14} {h["jockey"]:8} {h["pop"]:>4} '
              f'{h["odds"]:>6.1f}  {h["win_prob"]:>5.1f}%  '
              f'{h["market_prob"]:>5.1f}%  '
              f'{h["u_score"]:>7.1f}  {nr}')

    # 三連単マルチ買い目
    tickets = format_sanrentan_tickets(marks)
    if tickets:
        print(f'\n  [三連単マルチ流し (パターンC)]')
        for line in tickets.split('\n'):
            print(f'  {line}')

    # ベット推奨
    bet = check_bet_uscore(results)
    if bet:
        print(f'\n  {"="*68}')
        print(f'  ★ U score ベット推奨 [{bet["level"]}]')
        print(f'    {bet["horse"]}  {bet["odds"]:.1f}倍 {bet["pop"]}人気')
        print(f'    推定勝率: {bet["win_prob"]:.1f}%  EV: {bet["ev"]:.3f}  '
              f'U score: {bet["u_score"]}  (2位差: {bet["gap"]}pt)')
        print(f'  {"="*68}')

    # 上位3頭の因子ブレークダウン
    print(f'\n  [因子ブレークダウン 上位3頭]')
    print(f'  {"因子":14}', end='')
    for h in results[:3]:
        print(f'  {h["name"][:10]:>10}', end='')
    print()
    print(f'  {"-"*56}')
    for key, label in FACTOR_JA.items():
        print(f'  {label:14}', end='')
        for h in results[:3]:
            v = h['breakdown'].get(key, 0)
            w = USCORE_WEIGHTS.get(key, 1)
            bar = '█' * int(v / 2)  # 0-10 → 0-5文字
            print(f'  {v:>4.1f}(×{w:.1f}) ', end='')
        print()
    print()


# ══════════════════════════════════════════════════════
# スタンドアロン実行
# ══════════════════════════════════════════════════════

if __name__ == '__main__':
    import datetime, argparse, json, sys, io

    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    except AttributeError:
        pass

    parser = argparse.ArgumentParser(description='U score エンジン')
    parser.add_argument('--json',  help='races_YYYYMMDD.json のパス')
    parser.add_argument('--data',  default='data', help='データディレクトリ')
    parser.add_argument('--temp',  type=float, default=2.0, help='softmax温度')
    args = parser.parse_args()

    print('U score エンジン 起動中...')
    print(f'  履歴DB構築中...')
    horse_db = load_uscore_db(args.data)
    jstats   = pd.read_csv(f'{args.data}/jstats.csv',
                           encoding='utf-8-sig', index_col='jockey')
    dc_db    = pd.read_csv(f'{args.data}/horse_course_stats.csv',
                           encoding='utf-8-sig')
    print(f'  馬履歴DB: {len(horse_db)}頭  騎手DB: {len(jstats)}名')

    if not args.json:
        print('--json オプションでレースJSONを指定してください')
        sys.exit(0)

    # ファイル名から日付(YYYYMMDD)を取得 → race_ym(YYYYMM)を補完
    m = re.search(r'(\d{8})', os.path.basename(args.json))
    file_ym = m.group(1)[:6] if m else ''   # 例: '202603'

    with open(args.json, encoding='utf-8') as f:
        data = json.load(f)
    all_races = data.get('all_races', [])

    # クラスフィルタ適用
    target, skipped = [], []
    for r in all_races:
        name = r.get('race_name', '')
        if should_exclude_uscore(name):
            skipped.append(f'{r.get("rnum","?")}R {name}')
        else:
            r['_file_ym'] = file_ym   # race_ym補完用
            target.append(r)

    print(f'  全{len(all_races)}R → 対象:{len(target)}R  除外:{len(skipped)}R')
    if skipped:
        for s in skipped:
            print(f'    [除外] {s}')
    print()

    for race_info in target:
        results = analyze_race_uscore(race_info, horse_db, jstats, dc_db,
                                      temperature=args.temp)
        if results:
            print_uscore_result(race_info, results)

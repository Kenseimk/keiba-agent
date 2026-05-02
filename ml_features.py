# -*- coding: utf-8 -*-
"""
ml_features.py  ML特徴量抽出の共通モジュール
train_ml_model.py と backtest で共有する
"""
import numpy as np
from uscore import calc_horse_factors

FACTOR_KEYS = [
    'ability', 'acceleration', 'training', 'jockey', 'trainer',
    'course_fit', 'prev_content', 'unfulfilled', 'track_cond',
    'running_style', 'rotation', 'prev_agari', 'prev_level',
    'prev_position', 'tight_track', 'slope', 'grass_type',
    'dirt_fit', 'pci', 'gate', 'season', 'travel',
    'jockey_horse_fit', 'weight_fit', 'weight_carried', 'dist_fit',
]
REL_FEAT_IDX = list(range(len(FACTOR_KEYS)))  # 26因子を相対化

COURSE_MAP = {'芝': 0, 'ダート': 1}
COND_MAP   = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
GRADE_NUM  = {'新馬': 0, '未勝利': 1, '1勝': 2, '2勝': 3, '3勝': 4,
              'OP': 5, 'オープン': 5, 'L': 6, 'G3': 7, 'G2': 8, 'G1': 9}


# ── 追加特徴量ヘルパー ────────────────────────────────
# 馬場状態による速度補正係数 (良=基準)
_COND_ADJ = {
    '芝':   {'良': 1.000, '稍重': 0.997, '重': 0.991, '不良': 0.983},
    'ダート': {'良': 1.000, '稍重': 0.999, '重': 0.996, '不良': 0.992},
}


def _months_diff(ym1: str, ym2: str) -> int:
    """ym1 - ym2 (months). '202303' - '202301' = 2"""
    try:
        y1, m1 = int(ym1[:4]), int(ym1[4:])
        y2, m2 = int(ym2[:4]), int(ym2[4:])
        return max((y1 - y2) * 12 + (m1 - m2), 0)
    except Exception:
        return 0


def _agari_norm(rec: dict) -> float:
    """上がり3F順位を 0(最下位)〜1(最速) に正規化"""
    n = max(rec.get('agari_field', 1), 2)
    r = rec.get('agari_rank', n)
    return (n - r) / (n - 1)


def _finish_norm(rec: dict) -> float:
    """着順を 0(最下位)〜1(1着) に正規化"""
    n = max(rec.get('field_size', 2), 2)
    r = rec.get('rank', n)
    return (n - r) / (n - 1)


def _speed_adj(rec: dict) -> float | None:
    """
    speed_mps を馬場状態で補正した速度指数を返す。
    同距離帯の比較をしやすくするため、距離カテゴリで正規化も行う。
    """
    spd = rec.get('speed_mps')
    if spd is None or spd <= 0:
        return None
    course = 'ダート' if 'ダ' in str(rec.get('course', '')) else '芝'
    cond   = rec.get('track_cond', '良')
    adj    = _COND_ADJ.get(course, _COND_ADJ['芝']).get(cond, 1.0)
    return spd * adj


def extra_features(name, h, info, horse_db) -> list:
    """
    12次元の追加特徴量ベクトルを返す
      [months_rest, agari_avg, finish_cv, venue_wr, dist_wr, course_switch,
       speed_avg, speed_trend, time_rank_avg, first_pos_avg, closing_avg, bw_chg_last]
    """
    history   = horse_db.get(name, [])
    ym        = info['file_ym']
    venue     = info.get('venue_code', '')
    dist      = info.get('dist', 1800)
    course    = 'ダート' if 'ダ' in str(info.get('course', '')) else '芝'

    # ① 前走からの休養月数
    months_rest = _months_diff(ym, history[0]['race_ym']) if history else 6

    # ② 上がり3F相対順位の平均 (直近5走)
    agari_vals = [_agari_norm(r) for r in history[:5]]
    agari_avg  = float(np.mean(agari_vals)) if agari_vals else 0.5

    # ③ 着順安定性 (標準偏差: 低いほど安定)
    finish_vals = [_finish_norm(r) for r in history[:8]]
    finish_cv   = float(np.std(finish_vals)) if len(finish_vals) >= 2 else 0.3

    # ④ 同会場の勝率 (直近10戦)
    venue_hist = [r for r in history[:15] if r.get('venue_code') == venue]
    venue_wr   = (sum(1 for r in venue_hist if r.get('rank') == 1) /
                  len(venue_hist)) if venue_hist else 0.1

    # ⑤ 同距離帯(±200m)の勝率 (直近10戦)
    dist_hist = [r for r in history[:15]
                 if abs(r.get('dist', 0) - dist) <= 200]
    dist_wr   = (sum(1 for r in dist_hist if r.get('rank') == 1) /
                 len(dist_hist)) if dist_hist else 0.1

    # ⑥ コース転換フラグ (芝↔ダート)
    if history:
        prev_course = 'ダート' if 'ダ' in str(history[0].get('course', '')) else '芝'
        course_switch = 1.0 if prev_course != course else 0.0
    else:
        course_switch = 0.0

    # ── 速度指数・ペース系特徴量 ─────────────────────────
    # 同コース・同距離帯の直近レースを使う
    same_course_hist = [r for r in history[:10]
                        if abs(r.get('dist', 0) - dist) <= 300
                        and (('ダ' in str(r.get('course', ''))) == (course == 'ダート'))]

    # ⑦ 速度指数平均 (直近3走、同コース帯)
    speeds = [_speed_adj(r) for r in same_course_hist[:3]]
    speeds = [s for s in speeds if s is not None]
    speed_avg = float(np.mean(speeds)) if speeds else 0.0

    # ⑧ 速度トレンド (最新 - 1走前)
    if len(speeds) >= 2:
        speed_trend = speeds[0] - speeds[1]
    else:
        speed_trend = 0.0

    # ⑨ タイム順位 (フィールド内での速さの位置: 0=最下位, 1=最速)
    time_ranks = []
    for r in history[:5]:
        tr = r.get('time_rank', -1)
        nt = r.get('n_timed', 1)
        if tr > 0 and nt > 1:
            time_ranks.append((nt - tr) / (nt - 1))  # 0〜1
    time_rank_avg = float(np.mean(time_ranks)) if time_ranks else 0.5

    # ⑩ 道中位置 (最初のコーナー平均: フィールドサイズで正規化)
    first_positions = []
    for r in history[:5]:
        fp = r.get('first_pos')
        fs = max(r.get('field_size', 1), 2)
        if fp is not None:
            first_positions.append(fp / fs)  # 0〜1 (0=先頭)
    first_pos_avg = float(np.mean(first_positions)) if first_positions else 0.5

    # ⑪ 末脚: avg_pos → rank の変化量 (正 = 追込み、負 = 失速)
    closings = []
    for r in history[:5]:
        ap = r.get('avg_pos')
        rk = r.get('rank', 99)
        fs = max(r.get('field_size', 1), 2)
        if ap is not None and rk < 90:
            closings.append((ap - rk) / fs)
    closing_avg = float(np.mean(closings)) if closings else 0.0

    # ⑫ 前走体重変化
    bw_chg_last = history[0].get('bw_chg', 0) if history else 0
    if bw_chg_last is None:
        bw_chg_last = 0

    # ── 着差特徴量 ──────────────────────────────────────
    # margin: 0=1着, 正=負けた着差(馬身数)

    # ⑲ 前走着差 (直前1走)
    margin_prev = float(history[0].get('margin', 5.0)) if history else 5.0

    # ⑳ 直近5走の着差平均
    margins5 = [r.get('margin') for r in history[:5] if r.get('margin') is not None]
    margin_avg5 = float(np.mean(margins5)) if margins5 else 5.0

    # ㉑ 着差トレンド (最新 - 1走前: 負=改善, 正=悪化)
    if len(margins5) >= 2:
        margin_trend = margins5[0] - margins5[1]
    else:
        margin_trend = 0.0

    # ㉒ 直近3走の1着回数 (0〜3)
    wins_recent3 = sum(1 for r in history[:3] if r.get('rank') == 1)

    # ── 区間速度（ラップ）特徴量 ──────────────────────────
    # 同コース帯の直近5走で前半/後半速度を分析

    # ⑬ 後半(上がり600m)速度ランク平均 (0=最低, 1=最速)
    late_ranks = []
    for r in same_course_hist[:5]:
        lr = r.get('late_rank', -1)
        np_ = r.get('n_pace', 1)
        if lr > 0 and np_ > 1:
            late_ranks.append((np_ - lr) / (np_ - 1))
    late_rank_avg = float(np.mean(late_ranks)) if late_ranks else 0.5

    # ⑭ 前半速度ランク平均 (0=最低, 1=最速=先行)
    front_ranks = []
    for r in same_course_hist[:5]:
        fr = r.get('front_rank', -1)
        np_ = r.get('n_pace', 1)
        if fr > 0 and np_ > 1:
            front_ranks.append((np_ - fr) / (np_ - 1))
    front_rank_avg = float(np.mean(front_ranks)) if front_ranks else 0.5

    # ⑮ ペース差平均 (late_speed - front_speed): 正=末脚型、負=先行型
    pace_gaps = [r.get('pace_gap') for r in same_course_hist[:5]
                 if r.get('pace_gap') is not None]
    pace_gap_avg = float(np.mean(pace_gaps)) if pace_gaps else 0.0

    # ⑯ ペース差の標準偏差 (安定性)
    pace_gap_std = float(np.std(pace_gaps)) if len(pace_gaps) >= 2 else 0.3

    # ⑰ 後半速度トレンド (最新 vs 1走前)
    late_speeds_hist = [r.get('late_speed') for r in same_course_hist[:3]
                        if r.get('late_speed') is not None]
    if len(late_speeds_hist) >= 2:
        late_speed_trend = late_speeds_hist[0] - late_speeds_hist[1]
    else:
        late_speed_trend = 0.0

    # ⑱ 前半速度平均 (condition-adjusted)
    cond_adj_val = _COND_ADJ.get(course, _COND_ADJ['芝']).get(
        info.get('track_cond', '良'), 1.0)
    front_speeds_hist = [r.get('front_speed') for r in same_course_hist[:3]
                         if r.get('front_speed') is not None]
    front_speed_avg = (float(np.mean(front_speeds_hist)) * cond_adj_val
                       if front_speeds_hist else 0.0)

    return [months_rest, agari_avg, finish_cv, venue_wr, dist_wr, course_switch,
            speed_avg, speed_trend, time_rank_avg, first_pos_avg, closing_avg,
            float(bw_chg_last),
            late_rank_avg, front_rank_avg, pace_gap_avg, pace_gap_std,
            late_speed_trend, front_speed_avg,
            margin_prev, margin_avg5, margin_trend, float(wins_recent3),
]


EXTRA_NAMES = ['months_rest', 'agari_avg', 'finish_cv', 'venue_wr', 'dist_wr', 'course_switch',
               'speed_avg', 'speed_trend', 'time_rank_avg', 'first_pos_avg', 'closing_avg',
               'bw_chg_last',
               'late_rank_avg', 'front_rank_avg', 'pace_gap_avg', 'pace_gap_std',
               'late_speed_trend', 'front_speed_avg',
               'margin_prev', 'margin_avg5', 'margin_trend', 'wins_recent3',
               'jockey_dist_wr', 'jockey_venue_wr', 'trainer_dist_wr', 'jockey_course_wr',
               'jockey_trainer_wr', 'jockey_changed', 'dist_change_km', 'venue_changed']


def extract_features(name, h, info, horse_db, trainer_stats, jockey_stats):
    course_raw = info.get('course', '')
    course     = 'ダート' if 'ダ' in str(course_raw) else '芝'
    dist       = info.get('dist', 1800)
    ym         = info['file_ym']
    venue_code = info.get('venue_code', '')
    track_cond = info.get('track_cond', '')

    try:
        fdata = calc_horse_factors(
            name, h['jockey'], h['odds'], h['pop'], h['gate_num'],
            horse_db.get(name, []), None, None,
            course, dist, track_cond, venue_code, ym,
            trainer_stats=trainer_stats,
            jockey_stats=jockey_stats,
            oikiri_db=None,
            bw_now=h.get('bw'), kg_now=h.get('kg'),
        )
    except Exception:
        return None

    factors = fdata['factors']
    feat = [factors.get(k, 5.0) for k in FACTOR_KEYS]  # 26次元

    # レース特徴 (10次元)
    feat += [
        info['n_field'],
        dist / 1000.0,
        h['gate_num'],
        min(fdata.get('n_races', 0), 30),
        COURSE_MAP.get(course, 0),
        COND_MAP.get(track_cond, 0),
    ]

    history    = horse_db.get(name, [])
    prev_grade = GRADE_NUM.get(history[0].get('grade', '') if history else '', 2)
    best_grade = max((GRADE_NUM.get(r.get('grade', ''), 2) for r in history[:5]), default=2)
    prev_rank  = history[0].get('rank', 99) if history else 99
    prev_field = history[0].get('field_size', 12) if history else 12
    feat += [prev_grade, best_grade, min(prev_rank, 18), prev_field]

    # 追加特徴量
    feat += extra_features(name, h, info, horse_db)

    # 騎手×距離帯・会場別成績 / 調教師×距離帯成績 (4次元)
    def _dist_band(d):
        if d <= 1400: return 'sp'
        if d <= 1800: return 'mi'
        if d <= 2200: return 'md'
        return 'lo'

    jockey = h.get('jockey', '')
    band   = _dist_band(dist)
    js = None
    if jockey_stats and jockey:
        js = jockey_stats.get(jockey)
        if js is None:
            for k, v in jockey_stats.items():
                if k.startswith(jockey) and len(k) > len(jockey):
                    js = v; break

    default_jwr = js['wr'] if js else 0.07
    jockey_dist_wr  = js['dist_wr'].get(band, default_jwr)  if js and 'dist_wr'  in js else default_jwr
    jockey_venue_wr = js['venue_wr'].get(venue_code, default_jwr) if js and 'venue_wr' in js and venue_code else default_jwr

    trainer = h.get('trainer', '')
    ts = trainer_stats.get(trainer) if trainer_stats and trainer else None
    default_twr = ts['wr'] if ts else 0.07
    trainer_dist_wr = ts['dist_wr'].get(band, default_twr) if ts and 'dist_wr' in ts else default_twr

    # 騎手×コース別（既存factor jockeyの補完）
    jockey_course_wr = (js['wr_dirt'] if (js and course == 'ダート') else
                        js['wr_turf'] if js else 0.07)

    # 騎手×調教師コンビ勝率
    jockey_trainer_wr = (js['trainer_wr'].get(trainer, default_jwr)
                         if js and 'trainer_wr' in js and trainer else default_jwr)

    # 騎手変更フラグ (1=乗替り)
    prev_jockey = history[0].get('jockey', '') if history else ''
    jockey_changed = 1.0 if (prev_jockey and jockey and prev_jockey != jockey) else 0.0

    # 距離変化 (km: 正=距離延長, 負=距離短縮)
    prev_dist = history[0].get('dist', dist) if history else dist
    dist_change_km = (dist - prev_dist) / 1000.0

    # 会場変更フラグ (1=輸送あり)
    prev_venue = history[0].get('venue_code', '') if history else ''
    venue_changed = 1.0 if (prev_venue and venue_code and prev_venue != venue_code) else 0.0

    feat += [jockey_dist_wr, jockey_venue_wr, trainer_dist_wr, jockey_course_wr,
             jockey_trainer_wr, jockey_changed, dist_change_km, venue_changed]

    return feat


def add_relative_features(race_feats):
    n = len(race_feats)
    if n < 2:
        return [f + [0.0] * (len(REL_FEAT_IDX) * 2) for f in race_feats]

    arr   = np.array([[f[i] for i in REL_FEAT_IDX] for f in race_feats], dtype=np.float32)
    means = arr.mean(axis=0)
    stds  = arr.std(axis=0) + 1e-6

    result = []
    for i, feat in enumerate(race_feats):
        z_scores  = ((arr[i] - means) / stds).tolist()
        rel_ranks = [(arr[i, j] > arr[:, j]).sum() / (n - 1) for j in range(len(REL_FEAT_IDX))]
        result.append(feat + z_scores + rel_ranks)
    return result


def race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats,
                  model_win, model_place, model_win_rank=None):
    """
    1レース分のML予測確率を返す
    model_win_rank: LambdaRank モデル (リスト可)。指定時は◎ランキングに使用し、
                    win_prob フィルタリングには model_win (binary) を使用。
    戻り値: [{name, win_prob, place_prob, odds, pop, rank, umaban}, ...]
    """
    race_feats = []
    race_meta  = []
    for h in info['horse_list']:
        name = h['name']
        feat = extract_features(name, h, info, horse_db, trainer_stats, jockey_stats)
        if feat is None:
            continue
        race_feats.append(feat)
        race_meta.append(h)

    if len(race_feats) < 4:
        return []

    feats_rel = add_relative_features(race_feats)
    X = np.array(feats_rel, dtype=np.float32)

    def _predict(model, X):
        if isinstance(model, list):
            return np.mean([m.predict(X) for m in model], axis=0)
        return model.predict(X)

    raw_win   = _predict(model_win,   X)
    raw_place = _predict(model_place, X)

    # スコアをレース内正規化 (min-shift→sum-normalize)
    def normalize(x):
        x = x.astype(np.float64)
        x = x - x.min()
        s = x.sum()
        return x / s if s > 0 else np.full_like(x, 1.0 / len(x))

    win_probs   = normalize(raw_win)
    place_probs = normalize(raw_place)

    # ハイブリッド: LambdaRank スコアで◎ランキングを上書き
    if model_win_rank is not None:
        raw_rank = _predict(model_win_rank, X)
        # ランキングスコアは win_prob の相対順序のみに使用 (softmax 正規化)
        exp_r = np.exp(raw_rank - raw_rank.max())
        rank_probs = exp_r / exp_r.sum()
        # フィルタ用 win_prob は binary、ランキング用は lambda の混合
        win_probs = 0.5 * win_probs + 0.5 * rank_probs

    result = []
    for i, h in enumerate(race_meta):
        result.append({
            'name':       h['name'],
            'win_prob':   float(win_probs[i]   * 100),
            'place_prob': float(place_probs[i]  * 100),
            'odds':       h['odds'],
            'pop':        h['pop'],
            'rank':       h['rank'],
            'umaban':     h['umaban'],
        })
    return result

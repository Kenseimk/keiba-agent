"""
analyze_chaos.py  荒れるレース特徴の数値化・分析
========================================================
荒れの定義: 1番人気が3着以内に入らない (pop1_rank > 3)

実行例:
  python analyze_chaos.py                          # 202301-202603
  python analyze_chaos.py --start 202501 --end 202603
  python analyze_chaos.py --start 202301 --end 202603 --top 20
"""

import os, sys, io, math, argparse
from collections import defaultdict

import locale
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

from uscore_backtest import load_all_csv_races, _build_race_records

# ──────────────────────────────────────────────
# ユーティリティ
# ──────────────────────────────────────────────

def _entropy(probs):
    """Shannon entropy (nats). probs はリスト, 合計=1 に正規化して計算"""
    s = sum(probs)
    if s <= 0:
        return 0.0
    e = 0.0
    for p in probs:
        pp = p / s
        if pp > 0:
            e -= pp * math.log(pp)
    return e


def _std(vals):
    """標準偏差 (population std)"""
    n = len(vals)
    if n < 2:
        return 0.0
    mean = sum(vals) / n
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / n)


def _mean(vals):
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


# ──────────────────────────────────────────────
# レース単位の荒れ指標を計算
# ──────────────────────────────────────────────

def compute_chaos_features(race_id: str, info: dict) -> dict | None:
    """
    1レースの荒れ指標を辞書で返す。
    1番人気が特定できない場合は None を返す。
    """
    recs = _build_race_records(race_id, info)
    if not recs:
        return None

    horse_list = list(recs.values())
    n = len(horse_list)
    if n < 4:
        return None

    # ── 1番人気の着順 ──
    pop1_horses = [h for h in horse_list if h['pop'] == 1]
    if not pop1_horses:
        return None
    pop1_rank = pop1_horses[0]['rank']
    chaos = int(pop1_rank > 3)

    # 勝ち馬オッズ
    winner_horses = [h for h in horse_list if h['rank'] == 1]
    winner_odds = winner_horses[0]['odds'] if winner_horses else None

    # ── 市場系指標 ──
    # オッズが有効な馬のみ使用
    odds_list = [(h['pop'], h['odds']) for h in horse_list if h['odds'] and h['odds'] > 1.0]
    odds_list.sort(key=lambda x: x[0])  # pop順

    odds_vals = [o for _, o in odds_list]
    inv_odds  = [1.0 / o for o in odds_vals if o > 0]
    inv_sum   = sum(inv_odds)

    # オッズエントロピー
    odds_entropy = _entropy(inv_odds) if inv_odds else 0.0

    # inv_sum3: 上位3頭の 1/odds 合計（低い=拮抗）
    inv_sum3 = sum(1.0 / o for o in odds_vals[:3]) if len(odds_vals) >= 3 else None

    # 1番人気・2番人気のオッズ
    pop1_odds = odds_vals[0] if len(odds_vals) >= 1 else None
    pop2_odds = odds_vals[1] if len(odds_vals) >= 2 else None
    odds_ratio_12 = (pop2_odds / pop1_odds) if (pop1_odds and pop2_odds and pop1_odds > 0) else None

    # ── 脚質系指標 ──
    # first_pos: 1コーナー通過順。先行=3番手以内
    fp_vals = [h['first_pos'] for h in horse_list if h['first_pos'] is not None]
    senko_ratio = sum(1 for fp in fp_vals if fp <= 3) / n if fp_vals else None

    # avg_pos の分散（脚質多様性）
    avg_pos_vals = [h['avg_pos'] for h in horse_list if h['avg_pos'] is not None]
    running_style_std = _std(avg_pos_vals) if len(avg_pos_vals) >= 3 else None

    # ── ペース系指標 ──
    pg_vals = [h['pace_gap'] for h in horse_list if h['pace_gap'] is not None]
    pace_gap_mean  = _mean(pg_vals) if pg_vals else None
    pace_gap_std   = _std(pg_vals) if len(pg_vals) >= 3 else None
    pace_gap_range = (max(pg_vals) - min(pg_vals)) if len(pg_vals) >= 2 else None

    # ── 能力系指標 ──
    ls_vals = [h['late_speed'] for h in horse_list if h['late_speed'] is not None]
    late_speed_std = _std(ls_vals) if len(ls_vals) >= 3 else None

    sp_vals = [h['speed_mps'] for h in horse_list if h['speed_mps'] is not None]
    speed_mps_std = _std(sp_vals) if len(sp_vals) >= 3 else None

    # agari差（最速上がり - 最遅上がり）
    ag_vals = [h['agari'] for h in horse_list if h['agari'] is not None and h['agari'] > 0]
    agari_gap = (max(ag_vals) - min(ag_vals)) if len(ag_vals) >= 2 else None

    # ── フィジカル指標 ──
    bw_chg_vals = [abs(h['bw_chg']) for h in horse_list
                   if h['bw_chg'] is not None and h['bw_chg'] != 0]
    bw_chg_abs_mean = _mean(bw_chg_vals) if bw_chg_vals else None

    # ── メタ情報 ──
    file_ym    = info['file_ym']
    dist       = info['dist']
    course     = info['course']
    track_cond = info['track_cond']
    # race_id から R番号を推定 (末尾2桁)
    rnum = int(race_id[-2:]) if len(race_id) >= 2 and race_id[-2:].isdigit() else None

    return {
        # メタ
        'race_id':          race_id,
        'file_ym':          file_ym,
        'dist':             dist,
        'course':           course,
        'track_cond':       track_cond,
        'n_field':          n,
        'rnum':             rnum,
        # 荒れラベル
        'chaos':            chaos,
        'pop1_rank':        pop1_rank,
        'winner_odds':      winner_odds,
        # 市場系
        'odds_entropy':     odds_entropy,
        'inv_sum3':         inv_sum3,
        'pop1_odds':        pop1_odds,
        'odds_ratio_12':    odds_ratio_12,
        # 脚質系
        'senko_ratio':      senko_ratio,
        'running_style_std':running_style_std,
        # ペース系
        'pace_gap_mean':    pace_gap_mean,
        'pace_gap_std':     pace_gap_std,
        'pace_gap_range':   pace_gap_range,
        # 能力系
        'late_speed_std':   late_speed_std,
        'speed_mps_std':    speed_mps_std,
        'agari_gap':        agari_gap,
        # フィジカル
        'bw_chg_abs_mean':  bw_chg_abs_mean,
    }


# ──────────────────────────────────────────────
# 分析・出力
# ──────────────────────────────────────────────

def analyze(start_ym: str, end_ym: str, top_n: int = 10):
    print(f"データ読み込み中...")
    races = load_all_csv_races()

    # 期間フィルタ
    target = {rid: info for rid, info in races.items()
              if start_ym <= info['file_ym'] <= end_ym}

    print(f"対象レース数: {len(target)}")

    records = []
    for race_id, info in sorted(target.items()):
        feat = compute_chaos_features(race_id, info)
        if feat is not None:
            records.append(feat)

    if not records:
        print("データなし")
        return

    total = len(records)
    n_chaos = sum(r['chaos'] for r in records)
    base_rate = n_chaos / total

    print(f"\n{'='*60}")
    print(f"荒れ指標分析  {start_ym} - {end_ym}")
    print(f"{'='*60}")
    print(f"総レース数: {total:,}   荒れ数: {n_chaos:,}   ベース荒れ率: {base_rate*100:.1f}%")
    print(f"（荒れ = 1番人気が3着以内に入らない）\n")

    # ── 指標別分析 ──
    # 各指標について閾値を設定し、荒れ率との関係を出力
    # 連続値指標: 荒れ vs 非荒れの平均値比較
    CONTINUOUS_FEATURES = [
        ('odds_entropy',      '高いほど拮抗',      None),
        ('inv_sum3',          '低いほど拮抗',       None),
        ('pop1_odds',         '高いほど拮抗',       None),
        ('odds_ratio_12',     '高いほど拮抗',       None),
        ('n_field',           '多いほど荒れやすい', None),
        ('senko_ratio',       '先行馬比率',         None),
        ('running_style_std', '脚質の多様性',       None),
        ('pace_gap_mean',     'ペース差平均',       None),
        ('pace_gap_std',      'ペース差分散',       None),
        ('pace_gap_range',    'ペース差レンジ',     None),
        ('late_speed_std',    '末脚分散',           None),
        ('speed_mps_std',     '速度分散',           None),
        ('agari_gap',         '上がり差',           None),
        ('bw_chg_abs_mean',   '馬体重変動',         None),
    ]

    print(f"{'─'*65}")
    print(f"{'指標':<20} {'荒れ平均':>9} {'非荒れ平均':>10} {'差':>8} {'荒れ率変化':>10}")
    print(f"{'─'*65}")

    for feat_name, desc, _ in CONTINUOUS_FEATURES:
        chaos_vals    = [r[feat_name] for r in records if r['chaos'] == 1 and r[feat_name] is not None]
        nonchaos_vals = [r[feat_name] for r in records if r['chaos'] == 0 and r[feat_name] is not None]
        if not chaos_vals or not nonchaos_vals:
            continue
        cm = _mean(chaos_vals)
        nm = _mean(nonchaos_vals)
        diff = cm - nm
        print(f"{feat_name:<20} {cm:>9.4f} {nm:>10.4f} {diff:>+8.4f}   ({desc})")

    # ── 閾値別荒れ率 ──
    THRESHOLD_RULES = [
        # (指標名, 条件, ラベル)
        ('odds_entropy',    lambda v: v > 0.90, 'entropy>0.90'),
        ('odds_entropy',    lambda v: v > 0.85, 'entropy>0.85'),
        ('odds_entropy',    lambda v: v > 0.80, 'entropy>0.80'),
        ('inv_sum3',        lambda v: v < 0.55, 'inv_sum3<0.55'),
        ('inv_sum3',        lambda v: v < 0.50, 'inv_sum3<0.50'),
        ('inv_sum3',        lambda v: v < 0.45, 'inv_sum3<0.45'),
        ('pop1_odds',       lambda v: v > 3.0,  'pop1_odds>3.0'),
        ('pop1_odds',       lambda v: v > 4.0,  'pop1_odds>4.0'),
        ('pop1_odds',       lambda v: v > 5.0,  'pop1_odds>5.0'),
        ('odds_ratio_12',   lambda v: v > 1.5,  'ratio12>1.5'),
        ('odds_ratio_12',   lambda v: v > 2.0,  'ratio12>2.0'),
        ('n_field',         lambda v: v >= 15,  'n_field>=15'),
        ('n_field',         lambda v: v >= 13,  'n_field>=13'),
        ('n_field',         lambda v: v <= 10,  'n_field<=10'),
        ('senko_ratio',     lambda v: v > 0.35, 'senko>0.35'),
        ('senko_ratio',     lambda v: v < 0.20, 'senko<0.20'),
        ('pace_gap_std',    lambda v: v > 0.40, 'pg_std>0.40'),
        ('pace_gap_std',    lambda v: v > 0.35, 'pg_std>0.35'),
        ('late_speed_std',  lambda v: v > 0.35, 'ls_std>0.35'),
        ('agari_gap',       lambda v: v > 3.0,  'agari_gap>3.0'),
        ('agari_gap',       lambda v: v > 2.5,  'agari_gap>2.5'),
        ('running_style_std',lambda v: v > 3.5, 'rs_std>3.5'),
    ]

    print(f"\n{'─'*65}")
    print(f"{'条件':<20} {'該当数':>7} {'荒れ数':>7} {'荒れ率':>7} {'vs基準':>8}")
    print(f"{'─'*65}")

    for feat_name, cond_fn, label in THRESHOLD_RULES:
        subset = [r for r in records if r[feat_name] is not None and cond_fn(r[feat_name])]
        if not subset:
            continue
        n_sub   = len(subset)
        n_c     = sum(r['chaos'] for r in subset)
        rate    = n_c / n_sub
        vs_base = rate - base_rate
        pct     = n_sub / total * 100
        print(f"{label:<20} {n_sub:>7,} ({pct:4.1f}%) {n_c:>6,}  {rate*100:>6.1f}%  {vs_base*100:>+7.1f}pp")

    # ── R番号別 ──
    print(f"\n--- R番号別荒れ率 ---")
    rnum_stats = defaultdict(lambda: [0, 0])
    for r in records:
        rn = r.get('rnum')
        if rn and 1 <= rn <= 12:
            rnum_stats[rn][0] += 1
            rnum_stats[rn][1] += r['chaos']
    for rn in sorted(rnum_stats):
        tot, nc = rnum_stats[rn]
        rate = nc / tot
        bar = '#' * int(rate * 40)
        print(f"  R{rn:02d}: {rate*100:5.1f}%  {bar}  (n={tot:,})")

    # ── 馬場状態別 ──
    print(f"\n--- 馬場状態別荒れ率 ---")
    baba_stats = defaultdict(lambda: [0, 0])
    for r in records:
        bb = r.get('track_cond') or '不明'
        baba_stats[bb][0] += 1
        baba_stats[bb][1] += r['chaos']
    for bb, (tot, nc) in sorted(baba_stats.items(), key=lambda x: -x[1][1]/x[1][0]):
        rate = nc / tot
        print(f"  {bb:4}: {rate*100:5.1f}%  (n={tot:,})")

    # ── 距離帯別 ──
    print(f"\n--- 距離帯別荒れ率 ---")
    dist_bands = [
        ('短距離(~1399m)',  lambda d: d <= 1399),
        ('マイル(1400-1799)', lambda d: 1400 <= d <= 1799),
        ('中距離(1800-2199)', lambda d: 1800 <= d <= 2199),
        ('長距離(2200m~)',  lambda d: d >= 2200),
    ]
    for label, cond_fn in dist_bands:
        subset = [r for r in records if r['dist'] and cond_fn(r['dist'])]
        if not subset:
            continue
        nc = sum(r['chaos'] for r in subset)
        rate = nc / len(subset)
        print(f"  {label}: {rate*100:5.1f}%  (n={len(subset):,})")

    # ── 複合スコア ──
    print(f"\n--- 複合カオススコア別荒れ率 ---")
    print(f"（エントロピー高 + inv_sum3低 + 頭数多 + pg_std高 + pop1_odds高）")

    def calc_chaos_score(r):
        score = 0
        if r['odds_entropy'] is not None and r['odds_entropy'] > 0.85:
            score += 1
        if r['inv_sum3'] is not None and r['inv_sum3'] < 0.50:
            score += 1
        if r['n_field'] is not None and r['n_field'] >= 15:
            score += 1
        if r['pace_gap_std'] is not None and r['pace_gap_std'] > 0.35:
            score += 1
        if r['pop1_odds'] is not None and r['pop1_odds'] > 4.0:
            score += 1
        return score

    score_stats = defaultdict(lambda: [0, 0, []])
    for r in records:
        s = calc_chaos_score(r)
        score_stats[s][0] += 1
        score_stats[s][1] += r['chaos']
        if r['winner_odds']:
            score_stats[s][2].append(r['winner_odds'])

    for s in sorted(score_stats):
        tot, nc, wo = score_stats[s]
        rate = nc / tot
        avg_wo = _mean(wo) if wo else 0
        bar = '#' * int(rate * 40)
        print(f"  score={s}: {rate*100:5.1f}%  avg_winner_odds={avg_wo:5.1f}  {bar}  (n={tot:,})")

    # ── 高配当レースTOP分析 ──
    print(f"\n--- 勝ち馬オッズ別荒れ率 ---")
    odds_bands = [
        ('~3.9倍',   lambda o: o <= 3.9),
        ('4-9.9倍',  lambda o: 4.0 <= o <= 9.9),
        ('10-19倍',  lambda o: 10.0 <= o <= 19.9),
        ('20-49倍',  lambda o: 20.0 <= o <= 49.9),
        ('50倍~',    lambda o: o >= 50.0),
    ]
    for label, cond_fn in odds_bands:
        subset = [r for r in records if r['winner_odds'] and cond_fn(r['winner_odds'])]
        if not subset:
            continue
        nc = sum(r['chaos'] for r in subset)
        avg_scores = [calc_chaos_score(r) for r in subset]
        avg_score = _mean(avg_scores)
        print(f"  {label}: n={len(subset):,}  pop1外れ率={nc/len(subset)*100:.1f}%  avg_chaos_score={avg_score:.2f}")

    print(f"\n{'='*60}")
    print("完了")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='荒れるレース特徴分析')
    parser.add_argument('--start', default='202301', help='開始年月 (YYYYMM)')
    parser.add_argument('--end',   default='202603', help='終了年月 (YYYYMM)')
    parser.add_argument('--top',   type=int, default=10, help='上位N件表示')
    args = parser.parse_args()
    analyze(args.start, args.end, args.top)

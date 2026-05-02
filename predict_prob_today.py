# -*- coding: utf-8 -*-
"""
predict_prob_today.py  本日レースの◎確率予測
=============================================
チューニング期間 (202301-202412) で学習した確率モデルを使い、
指定した日付の各レースで「◎が1着 / 3着以内に入る確率」を出力する。

実行:
  python predict_prob_today.py --date 20260412
  python predict_prob_today.py --date 20260412 --track_cond 良
  python predict_prob_today.py --date 20260412 --track_cond 重 --min_prob 0.40
"""
import os, json, math, glob, argparse, re
os.environ['PYTHONIOENCODING'] = 'utf-8'
from collections import defaultdict


# ══════════════════════════════════════════════════════
# 追い切りスコア
# ══════════════════════════════════════════════════════

# eval評価 → スコア
EVAL_SCORE = {'A': 1.0, 'B': 0.75, 'C': 0.45, 'D': 0.10, '': 0.50}

# 調教スタイル → 努力度スコア
# 強め/仕掛 = 応答良好、馬也 = 省エネ、一杯 = 過負荷
STYLE_SCORE = {
    '強め':  0.85,
    'Ｇ強':  0.80,
    '仕掛':  0.70,
    '直一':  0.65,
    '馬也':  0.50,
    'Ｇ一':  0.35,
    '一杯':  0.15,
    '':      0.50,
}


def load_oikiri(date_str, data_dir='data'):
    """追い切りデータを {race_id: {horse_name: {...}}} で返す"""
    path = os.path.join(data_dir, f'oikiri_{date_str}.json')
    if not os.path.exists(path):
        return {}
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    return d.get('races', {})


def compute_oikiri_scores(oikiri_race):
    """
    1レース分の追い切りデータから各馬のスコアを計算。
    スコア: 0.0〜1.0  (高いほど状態良好)
    """
    if not oikiri_race:
        return {}

    # コース別にtime_1fを収集して正規化用の統計を作る
    course_times = defaultdict(list)
    for hdata in oikiri_race.values():
        t = hdata.get('time_1f')
        c = hdata.get('course', '')
        if t and t > 0:
            course_times[c].append(t)

    course_stats = {}
    for c, ts in course_times.items():
        if len(ts) >= 2:
            mean = sum(ts) / len(ts)
            std  = math.sqrt(sum((v-mean)**2 for v in ts) / len(ts))
            course_stats[c] = (mean, std if std > 0 else 1.0)

    scores = {}
    for hname, hdata in oikiri_race.items():
        ev    = hdata.get('eval', '')
        style = hdata.get('style', '')
        t1f   = hdata.get('time_1f')
        course = hdata.get('course', '')

        eval_s  = EVAL_SCORE.get(ev, 0.50)
        style_s = STYLE_SCORE.get(style, 0.50)

        # time_1f: コース内でzスコア化 (速いほど高スコア)
        if t1f and course in course_stats:
            mean, std = course_stats[course]
            z = (mean - t1f) / std   # 速い(小さい) → 正のz
            time_s = 1 / (1 + math.exp(-z * 1.2))   # sigmoid
        else:
            time_s = 0.50

        # 重み: eval 40%, style 35%, time 25%
        oikiri_score = 0.40 * eval_s + 0.35 * style_s + 0.25 * time_s
        scores[hname] = {
            'score':  oikiri_score,
            'eval':   ev,
            'style':  style,
            'time_1f': t1f,
            'eval_text': hdata.get('eval_text', ''),
        }

    return scores

from backtest_GEKIUMA_TENKAI import (
    build_horse_profile, predict_pace_v2, score_horse_relative,
    build_jockey_style_db, build_course_pace_db, build_cond_pace_db,
    load_sanrenpuku, _run_core,
    TUNE_START, TUNE_END, BLIND_START, BLIND_END,
    DEFAULT_MIN_RECORDS, DEFAULT_RECENCY_DECAY,
    DEFAULT_SPEED_ADV_W, DEFAULT_PACE_FIT_W, DEFAULT_N_FIELD_MIN,
    _dist_band,
)
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from analyze_axis_prob import (
    logistic_fit, predict_prob, FEATURES, collect,
)

# ── ベストパラメータ ──
MIN_RECORDS   = 3
RECENCY_DECAY = 0.85
SPEED_ADV_W   = 0.45
PACE_FIT_W    = 0.25

# venue_code → 会場名
VENUE_NAME = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
    '05': '東京', '06': '中山', '07': '中京', '08': '京都',
    '09': '阪神', '10': '小倉',
}


def load_races_json(date_str, data_dir='data'):
    """races_YYYYMMDD.json を読み込む"""
    path = os.path.join(data_dir, f'races_{date_str}.json')
    if not os.path.exists(path):
        raise FileNotFoundError(f'ファイルが見つかりません: {path}')
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    races = d.get('all_races') or d.get('candidates') or []
    return races


def build_race_info(race_json, track_cond='良'):
    """JSON形式のレース情報を内部形式に変換"""
    race_id    = race_json['race_id']
    venue_code = race_id[4:6] if len(race_id) >= 6 else '05'

    # 人気マップ (pop→name, name→pop/odds)
    pop_map  = {}
    odds_map = {}
    for o in race_json.get('odds', []):
        name = o['name']
        pop_map[name]  = int(o.get('pop', 99))
        odds_map[name] = float(o.get('odds', 0))

    # 馬リスト構築 (jockeyはhorses側から取得)
    jockey_map = {h['name']: h.get('jockey', '') for h in race_json.get('horses', [])}
    all_names  = list(set(list(pop_map.keys()) + list(jockey_map.keys())))

    horse_list = []
    for i, name in enumerate(all_names):
        horse_list.append({
            'name':     name,
            'jockey':   jockey_map.get(name, ''),
            'pop':      pop_map.get(name, 99),
            'odds':     odds_map.get(name, 0),
            'umaban':   str(i + 1),   # 確定前なので仮番号
            'gate_num': 8,            # 枠番未確定 → 中央値(8)で代替
            'rank':     99,           # 未確定
        })

    return {
        'race_id':    race_id,
        'race_name':  race_json.get('race_name', ''),
        'course':     race_json.get('course', 'ダート'),
        'dist':       int(race_json.get('dist', 1800)),
        'track_cond': track_cond,
        'venue_code': venue_code,
        'n_field':    len(horse_list),
        'file_ym':    race_id[:6],
        'horse_list': horse_list,
    }


def score_race(info, horse_db, jockey_style_db, course_pace_db, cond_pace_db,
               oikiri_scores=None):
    """1レースをスコアリングして (profiles, scores, adj_senko_map, pace_label, pace_score) を返す"""
    course     = info['course']
    dist       = info['dist']
    track_cond = info['track_cond']
    venue_code = info['venue_code']

    # 各馬プロファイル
    profiles = {}
    for h in info['horse_list']:
        name = h['name']
        hist = horse_db.get(name, [])
        profiles[name] = build_horse_profile(
            hist, course, dist, MIN_RECORDS, RECENCY_DECAY,
            target_cond=track_cond)

    n_valid = sum(1 for p in profiles.values() if p)
    if n_valid < 3:
        return None, None, None, None, None

    jockey_map = {h['name']: h.get('jockey', '') for h in info['horse_list']}
    gate_map   = {h['name']: h.get('gate_num', 8) for h in info['horse_list']}

    pace_label, pace_score = predict_pace_v2(
        profiles, gate_map, jockey_map,
        jockey_style_db, course_pace_db, cond_pace_db,
        venue_code, dist, course, track_cond)

    # 騎手+枠番補正済みsenko
    JOCKEY_W = 0.25; GATE_W = 0.15
    adj_senko_map = {}
    for h in info['horse_list']:
        nm  = h['name']
        p   = profiles.get(nm)
        if not p:
            continue
        raw_s = p.get('senko_rel', 0.5) or 0.5
        j_sty = jockey_style_db.get(h.get('jockey', ''), raw_s)
        gnum  = h.get('gate_num', 8)
        n_fld = max(len(profiles), 1)
        gate_pct = (gnum - 1) / max(n_fld - 1, 1)
        adj_senko_map[nm] = (1-JOCKEY_W)*raw_s + JOCKEY_W*j_sty + gate_pct*GATE_W

    consist_w = max(0, 1 - SPEED_ADV_W - PACE_FIT_W)
    comp_w    = 0.10
    base_scores = {
        name: score_horse_relative(
            name, p, profiles, pace_score,
            SPEED_ADV_W, PACE_FIT_W, consist_w, comp_w,
            adj_senko=adj_senko_map.get(name))
        for name, p in profiles.items()
    }

    # 追い切りスコアで補正 (±10%以内)
    OIKIRI_W = 0.10
    scores = {}
    for name, base in base_scores.items():
        if oikiri_scores and name in oikiri_scores:
            ok_s = oikiri_scores[name]['score']  # 0〜1
            # 0.5を中心に ±OIKIRI_W 補正
            adj = base * (1 + OIKIRI_W * (ok_s - 0.5) * 2)
        else:
            adj = base
        scores[name] = adj

    return profiles, scores, adj_senko_map, pace_label, pace_score


def compute_features_live(info, profiles, scores, adj_senko_map, pace_score, pace_label):
    """確率モデル用特徴量を計算 (実結果なし)"""
    top_names = sorted(scores, key=lambda n: scores[n], reverse=True)
    if not top_names:
        return None

    axis = top_names[0]
    s_vals = [scores[n] for n in top_names if scores.get(n) is not None]
    s1 = s_vals[0] if len(s_vals) > 0 else 0.5
    s2 = s_vals[1] if len(s_vals) > 1 else s1
    s3 = s_vals[2] if len(s_vals) > 2 else s2

    total_score = sum(s_vals) or 1.0
    mean_score  = total_score / len(s_vals)
    field_std   = math.sqrt(sum((v-mean_score)**2 for v in s_vals)/len(s_vals)) if len(s_vals)>1 else 0.0
    score_share = s1 / total_score

    axis_style  = (profiles[axis] or {}).get('style', '不明')
    same_style_n = sum(1 for n2, p2 in profiles.items()
                       if n2 != axis and p2 and p2.get('style') == axis_style)

    raw_senko   = (profiles[axis] or {}).get('senko_rel', 0.5) or 0.5
    adj_s       = adj_senko_map.get(axis, raw_senko)
    agari_pct   = (profiles[axis] or {}).get('agari_pct', 0.5) or 0.5
    axis_pace_fit = (1-pace_score)*(1-adj_s) + pace_score*(1-agari_pct)

    if pace_score >= 0.55:
        pace_style_match = 1 if axis_style in ('差し', '追い込み') else 0
    else:
        pace_style_match = 1 if axis_style in ('逃げ', '先行') else 0

    n_nige_adj = sum(1 for s in adj_senko_map.values() if s <= 0.15)

    if axis_style in ('逃げ', '先行'):
        competition_score = max(0.0, 1.0 - same_style_n * 0.25)
    elif axis_style == '差し':
        same_diff = sum(1 for n2, p2 in profiles.items()
                        if n2 != axis and p2 and p2.get('style') == '差し')
        competition_score = max(0.5, 1.0 - same_diff * 0.10)
    else:
        competition_score = 1.0

    return {
        'axis_score':        s1,
        'gap_1_2':           s1 - s2,
        'gap_1_3':           s1 - s3,
        'score_share':       score_share,
        'field_std':         field_std,
        'pace_score':        pace_score,
        'axis_pace_fit':     axis_pace_fit,
        'same_style_n':      same_style_n,
        'n_nige_adj':        n_nige_adj,
        'pace_style_match':  pace_style_match,
        'competition_score': competition_score,
        'n_field':           info['n_field'],
    }


def confidence_label(p_place):
    if p_place >= 0.55: return '★★★ 高信頼'
    if p_place >= 0.45: return '★★  中信頼'
    if p_place >= 0.35: return '★   低信頼'
    return '  (参考)'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',       required=True, help='予測日 (例: 20260412)')
    parser.add_argument('--track_cond', default='良',
                        choices=['良', '稍重', '重', '不良'], help='馬場状態')
    parser.add_argument('--min_prob',   type=float, default=0.0,
                        help='P(3着内)がこの値以上のレースのみ表示 (例: 0.40)')
    parser.add_argument('--data_dir',   default='data')
    args = parser.parse_args()

    print('=' * 65)
    print(f'◎確率予測  {args.date[:4]}/{args.date[4:6]}/{args.date[6:]}  馬場:{args.track_cond}')
    print('=' * 65)

    # ── 1. 全履歴データ + horse_db構築 ──
    print('\n[1/3] 履歴データ読み込み中...')
    races = load_all_csv_races(args.data_dir)
    today_ym = args.date[:6]

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=today_ym)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'  全レース: {len(races):,}R  horse_db: {len(horse_db):,}頭')

    jockey_style_db = build_jockey_style_db(horse_db)
    course_pace_db  = build_course_pace_db(races, today_ym)
    cond_pace_db    = build_cond_pace_db(races, today_ym)
    print(f'  騎手スタイルDB: {len(jockey_style_db):,}人  '
          f'コースペースDB: {len(course_pace_db):,}件  '
          f'馬場ペースDB: {len(cond_pace_db):,}件')

    # ── 2. 確率モデル学習 (チューニング期間) ──
    print('\n[2/3] 確率モデル学習中 (202301-202412)...')
    feat_tune = collect(races, TUNE_START, TUNE_END, walkforward=False)
    w_p, b_p, mu_p, sd_p = logistic_fit(feat_tune, 'hit_place')
    w_w, b_w, mu_w, sd_w = logistic_fit(feat_tune, 'hit_win')
    p3_base = sum(r['hit_place'] for r in feat_tune) / len(feat_tune) * 100
    p1_base = sum(r['hit_win']   for r in feat_tune) / len(feat_tune) * 100
    print(f'  学習レース数: {len(feat_tune):,}  '
          f'チューニング3着内率: {p3_base:.1f}%  1着率: {p1_base:.1f}%')

    # ── 3. 本日レース予測 ──
    print(f'\n[3/3] {args.date} のレース予測...')
    try:
        today_races = load_races_json(args.date, args.data_dir)
    except FileNotFoundError as e:
        print(f'  エラー: {e}')
        return

    # 追い切りデータ読み込み
    oikiri_all = load_oikiri(args.date, args.data_dir)
    if oikiri_all:
        print(f'  追い切りデータあり: {len(oikiri_all)}レース分')
    else:
        print(f'  追い切りデータなし (oikiri_{args.date}.json が見つかりません)')
    print(f'  {len(today_races)}レース found\n')

    results = []
    for race_json in today_races:
        info = build_race_info(race_json, args.track_cond)
        rid  = info['race_id']
        venue_name = VENUE_NAME.get(info['venue_code'], info['venue_code'])
        rnum = race_json.get('rnum', '?')

        # 追い切りスコア計算
        oikiri_race  = oikiri_all.get(rid, {})
        oikiri_scores = compute_oikiri_scores(oikiri_race) if oikiri_race else {}

        profiles, scores, adj_senko_map, pace_label, pace_score = score_race(
            info, horse_db, jockey_style_db, course_pace_db, cond_pace_db,
            oikiri_scores=oikiri_scores)

        if scores is None:
            print(f'  {venue_name}{rnum:>2}R  [{rid}]  → 有効プロファイル不足のためスキップ')
            continue

        top_names = sorted(scores, key=lambda n: scores[n], reverse=True)
        axis  = top_names[0]
        feats = compute_features_live(
            info, profiles, scores, adj_senko_map, pace_score, pace_label)

        if feats is None:
            continue

        p_place = predict_prob(feats, w_p, b_p, mu_p, sd_p)
        p_win   = predict_prob(feats, w_w, b_w, mu_w, sd_w)

        # 人気
        pop_map = {h['name']: h['pop'] for h in info['horse_list']}
        axis_pop = pop_map.get(axis, 99)

        # 上位5頭の追い切り情報
        top5_oikiri = []
        for n in top_names[:5]:
            ok = oikiri_scores.get(n, {})
            top5_oikiri.append({
                'name':   n,
                'score':  scores[n],
                'pop':    pop_map.get(n, 99),
                'style':  (profiles[n] or {}).get('style', '?'),
                'ok_eval':  ok.get('eval', '-'),
                'ok_style': ok.get('style', '-'),
                'ok_t1f':   ok.get('time_1f'),
                'ok_text':  ok.get('eval_text', ''),
                'ok_score': ok.get('score'),
            })

        results.append({
            'race_id':    rid,
            'venue':      venue_name,
            'rnum':       rnum,
            'race_name':  info['race_name'],
            'course':     info['course'],
            'dist':       info['dist'],
            'n_field':    info['n_field'],
            'axis':       axis,
            'axis_pop':   axis_pop,
            'axis_style': (profiles[axis] or {}).get('style', '不明'),
            'axis_score': feats['axis_score'],
            'pace_label': pace_label,
            'pace_style_match': feats['pace_style_match'],
            'p_win':      p_win,
            'p_place':    p_place,
            'top5':       top5_oikiri,
            'has_oikiri': bool(oikiri_scores),
        })

    if not results:
        print('予測できるレースがありませんでした。')
        return

    # ── 出力 ──
    # P(3着内)の高い順にソート
    results.sort(key=lambda x: x['p_place'], reverse=True)

    shown = [r for r in results if r['p_place'] >= args.min_prob]
    skipped = len(results) - len(shown)

    print(f'{"=" * 65}')
    print(f'予測結果 ({len(shown)}レース'
          + (f' / {skipped}件は閾値({args.min_prob:.2f})未満で非表示' if skipped else '') + ')')
    print(f'{"=" * 65}\n')

    for r in shown:
        conf = confidence_label(r['p_place'])
        match_mark = '✓適合' if r['pace_style_match'] else '✗不一致'
        print(f'┌─ {r["venue"]}{r["rnum"]:>2}R  {r["race_name"]}')
        print(f'│  {r["course"]} {r["dist"]}m  {r["n_field"]}頭  '
              f'展開予測: {r["pace_label"]}  {match_mark}')
        print(f'│')
        print(f'│  ◎ {r["axis"]} ({r["axis_pop"]}番人気 / {r["axis_style"]})')
        print(f'│     P(1着)  :  {r["p_win"]*100:>5.1f}%')
        print(f'│     P(3着内):  {r["p_place"]*100:>5.1f}%   {conf}')
        print(f'│')
        print(f'│  スコア上位5頭:')
        for i, h in enumerate(r['top5']):
            mark = ['◎','○','▲','△','×'][i]
            ok_str = ''
            if h['ok_score'] is not None:
                t_str = f"{h['ok_t1f']:.1f}秒" if h['ok_t1f'] else '-'
                ok_str = (f"  [追切: {h['ok_eval'] or '-'}/{h['ok_style'] or '-'}"
                          f"/{t_str}  {h['ok_text']}]")
            print(f'│    {mark} {h["name"]} ({h["pop"]}番人気/{h["style"]})  '
                  f'score={h["score"]:.4f}{ok_str}')
        print(f'└{"─"*55}\n')

    # サマリー
    high = [r for r in shown if r['p_place'] >= 0.45]
    mid  = [r for r in shown if 0.35 <= r['p_place'] < 0.45]
    low  = [r for r in shown if r['p_place'] < 0.35]
    print(f'{"=" * 65}')
    print(f'サマリー: ★★★高信頼(P>=0.45): {len(high)}R  '
          f'★★中(0.35-0.45): {len(mid)}R  ★低(<0.35): {len(low)}R')
    if high:
        print(f'\n注目レース (P(3着内)>=0.45):')
        for r in high:
            print(f'  {r["venue"]}{r["rnum"]:>2}R  ◎{r["axis"]}({r["axis_pop"]}番人気)  '
                  f'P(3着内)={r["p_place"]*100:.1f}%  P(1着)={r["p_win"]*100:.1f}%')
    print(f'{"=" * 65}')


if __name__ == '__main__':
    main()

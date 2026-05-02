"""
backtest_agent.py  スコアエージェント バックテスト
======================================================
過去データ (raceresults_YYYYMM.csv) を使って予測精度・回収率を検証する。

評価指標:
  - 1着的中率   : スコア1位 = 実際の1着
  - 2着以内的中率: スコア1位 or 2位 が実際の1着
  - 3連複的中率 : スコア1〜3位の3頭が全て3着以内
  - 単勝回収率  : スコア1位に100円ベット時の回収率
  - 三連複回収率: スコア1〜3位の三連複に100円ベット時の回収率

使い方:
  python backtest_agent.py                    # 全データで検証
  python backtest_agent.py --year 2025        # 2025年のみ
  python backtest_agent.py --month 202510     # 1ヶ月のみ
  python backtest_agent.py --rnum 8 9 10 11   # R8〜R11指定
  python backtest_agent.py --model win        # WINモデル (デフォルト: sanpuku)

注意: 検証対象レースには「新馬・未勝利・1勝クラス・500万下」を除外しない
     (CSVにはレース名がないため過去データ全体を使用)
     → R番号 (8〜11) のみでフィルタリング
"""

import os, sys, io, glob, csv, re, argparse
from collections import defaultdict

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
except AttributeError:
    pass

from score_agent_core import (
    load_models, should_exclude,
    score_recent_rank, score_agari_rank,
    score_running_style, score_weight_change,
    score_jockey, score_course_fit,
    score_dist_fit, score_track_fit, score_last_margin,
    score_rest_interval, score_venue_fit, score_win_rate,
    WEIGHTS, WEIGHT_PRESETS, parse_avg_pos, parse_bw_change, parse_margin,
    _float, _int,
)

# ─── 定数 ─────────────────────────────────────────────
VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
}

# ─── データ読み込み ─────────────────────────────────────
def load_all_races(data_dir: str = 'data') -> dict:
    """
    全CSVを race_id 単位で集約し、各レースごとに以下を構築:
      - 全馬の行データ
      - 各馬の上がり3F順位
    戻り値: {race_id: {'horses': [...], 'agari_rank_map': {...}}}
    """
    race_db  = defaultdict(list)
    race_ym_map: dict[str, str] = {}   # race_id → YYYYMM (ファイル名由来)

    for fpath in sorted(glob.glob(os.path.join(data_dir, 'raceresults_*.csv'))):
        # ファイル名から YYYYMM を取得
        m = re.search(r'raceresults_(\d{6})\.csv', os.path.basename(fpath))
        file_ym = m.group(1) if m else None

        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                race_db[rid].append(row)
                if file_ym and rid not in race_ym_map:
                    race_ym_map[rid] = file_ym

    races = {}
    for race_id, rows in race_db.items():
        # 上がり3F順位
        agari_list = []
        for h in rows:
            a = _float(h.get('上がり3F'))
            if a is not None:
                agari_list.append((h['馬名'], a))
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i + 1 for i, (name, _) in enumerate(agari_list)}

        # 払戻データと馬番マップ (1行目から取得)
        first = rows[0] if rows else {}
        name_to_umaban = {h.get('馬名', ''): h.get('馬番', '').strip() for h in rows}

        # 距離・コース・馬場状態（列がある場合のみ）
        dist_val   = first.get('距離', '').strip()
        course_val = first.get('コース', '').strip()
        track_val  = first.get('馬場状態', '').strip()

        races[race_id] = {
            'horses':           rows,
            'agari_rank_map':   agari_rank_map,
            'n_agari':          len(agari_list),
            'n_field':          len(rows),
            'name_to_umaban':   name_to_umaban,
            'tansho_raw':       first.get('単勝払戻', ''),
            'sanrenpuku_raw':   first.get('三連複払戻', ''),
            'ym':               race_ym_map.get(race_id, race_id[:6]),
            'dist':             int(dist_val)   if dist_val.isdigit()  else None,
            'course':           course_val      if course_val          else None,
            'track_cond':       track_val       if track_val           else None,
            'race_name':        first.get('race_name', '').strip(),
        }
    return races


def parse_tansho(s: str) -> dict:
    """'7:140' → {'7': 140}"""
    result = {}
    for part in str(s).split('|'):
        part = part.strip()
        if ':' in part:
            k, v = part.rsplit(':', 1)
            try:
                result[k.strip()] = int(v.strip())
            except ValueError:
                pass
    return result


def parse_sanrenpuku(s: str) -> dict:
    """'6 - 7 - 13:4240' → {(6,7,13): 4240}  (複数候補があれば最初の1件)"""
    result = {}
    for part in str(s).split('|'):
        part = part.strip()
        if ':' not in part:
            continue
        combo_str, pay_str = part.rsplit(':', 1)
        nums = re.findall(r'\d+', combo_str)
        try:
            key = tuple(sorted(int(n) for n in nums))
            result[key] = int(pay_str.strip())
        except ValueError:
            pass
    return result


def build_horse_db_upto(races: dict, target_race_id: str) -> dict:
    """
    target_race_id より古いレースだけを使って horse_db を構築
    (リークを防ぐため)
    """
    horse_db = defaultdict(list)
    for race_id, info in races.items():
        if race_id >= target_race_id:
            continue   # 対象レース以降のデータは使わない
        for h in info['horses']:
            rank = _int(h.get('着順'))
            if rank is None:
                continue
            margin_raw = h.get('着差', '').strip()
            record = {
                'race_id':     race_id,
                'race_ym':     race_id[:6],
                'venue_code':  race_id[4:6],
                'race_num':    _int(race_id[10:12], 0),
                'rank':        rank,
                'field_size':  info['n_field'],
                'jockey':      h.get('騎手', ''),
                'odds':        _float(h.get('単勝オッズ')),
                'pop':         _int(h.get('人気')),
                'agari':       _float(h.get('上がり3F')),
                'agari_rank':  info['agari_rank_map'].get(h['馬名'], -1),
                'agari_field': info['n_agari'],
                'avg_pos':     parse_avg_pos(h.get('通過順', '')),
                'bw_chg':      parse_bw_change(h.get('馬体重', '')),
                'dist':        _int(h.get('距離')),
                'track_cond':  h.get('馬場状態', '').strip(),
                'margin':      parse_margin(margin_raw, rank),
            }
            horse_db[h['馬名']].append(record)

    for name in horse_db:
        horse_db[name].sort(key=lambda r: r['race_ym'], reverse=True)

    return dict(horse_db)


# ─── スコア計算 (バックテスト版) ──────────────────────
def score_horse_bt(
    horse_name:  str,
    jockey:      str,
    course:      str,
    dist:        int,
    history:     list,
    jstats,
    dc_db,
    weights:     dict = None,
    track_cond:  str  = '',
    venue_code:  str  = '',
    race_ym:     str  = '',
) -> float:
    w  = weights if weights is not None else WEIGHTS
    rr = score_recent_rank(history)
    ar = score_agari_rank(history)
    rs = score_running_style(history, course, dist)
    wc = score_weight_change(history)
    js = score_jockey(jockey, jstats)
    cf = score_course_fit(horse_name, course, dist, dc_db)
    df = score_dist_fit(history, dist)
    tf = score_track_fit(history, track_cond)
    lm = score_last_margin(history)
    ri = score_rest_interval(history, race_ym)
    vf = score_venue_fit(history, venue_code)
    wr = score_win_rate(history)

    return (
        rr * w['recent_rank']           +
        ar * w['agari_rank']            +
        js * w['jockey']                +
        cf * w['course_fit']            +
        rs * w['running_style']         +
        wc * w['weight_change']         +
        df * w.get('dist_fit',      0.0) +
        tf * w.get('track_fit',     0.0) +
        lm * w.get('last_margin',   0.0) +
        ri * w.get('rest_interval', 0.0) +
        vf * w.get('venue_fit',     0.0) +
        wr * w.get('win_rate',      0.0)
    )


# ─── バックテスト本体 ──────────────────────────────────
def run_backtest(
    data_dir:    str  = 'data',
    year_filter: str  = None,    # 例 '2025'
    month_filter: str = None,    # 例 '202510'
    rnum_filter: list = None,    # 例 [8, 9, 10, 11]
    verbose:     bool = False,
    model:       str  = 'sanpuku',  # 'win' or 'sanpuku'
):
    print('=' * 72)
    print('  スコアエージェント バックテスト')
    print('=' * 72)

    # モデル読み込み
    print('\n[1/3] モデル読み込み...')
    jstats, dc_db = load_models(data_dir)

    # 全レースデータ読み込み
    print('[2/3] レースデータ読み込み...')
    all_races = load_all_races(data_dir)
    print(f'  合計 {len(all_races)} レース')

    # フィルタ
    target_ids = sorted(all_races.keys())
    if year_filter:
        target_ids = [r for r in target_ids if r.startswith(year_filter)]
    if month_filter:
        ym = month_filter.replace('/', '').replace('-', '')
        target_ids = [r for r in target_ids if r.startswith(ym)]
    def _rnum(r):
        try:    return int(r[10:12])
        except: return -1

    if rnum_filter:
        target_ids = [r for r in target_ids if _rnum(r) in rnum_filter]
    else:
        # デフォルト: R8〜R11
        target_ids = [r for r in target_ids if 8 <= _rnum(r) <= 11]

    weights     = WEIGHT_PRESETS.get(model, WEIGHT_PRESETS['sanpuku'])
    weights_win = WEIGHT_PRESETS['win']
    weights_san = WEIGHT_PRESETS['sanpuku']
    print(f'  使用モデル: {model}  重み: {weights}')
    print(f'  検証対象: {len(target_ids)} レース')
    print('[3/3] バックテスト実行中...\n')

    # ─── 集計変数 ───
    total       = 0
    hit_1st     = 0
    hit_top2    = 0
    hit_top3    = 0
    hit_sanpuku = 0
    hit_niren   = 0

    # 回収率用
    tan_invest  = 0;  tan_return  = 0
    san_invest  = 0;  san_return  = 0

    # 両モデル一致レースのみ集計
    agree_total   = 0
    agree_sanpuku = 0
    agree_tan_inv = 0; agree_tan_ret = 0
    agree_san_inv = 0; agree_san_ret = 0

    def _make_monthly():
        return {'total': 0, 'hit1': 0, 'hit3': 0, 'sanpuku': 0,
                'tan_inv': 0, 'tan_ret': 0, 'san_inv': 0, 'san_ret': 0}
    monthly = defaultdict(_make_monthly)
    miss_examples = []

    for race_id in target_ids:
        info       = all_races[race_id]
        horses_raw = info['horses']

        # 新馬・未勝利・1勝クラス・障害 除外
        race_name = info.get('race_name', '')
        if race_name and should_exclude(race_name):
            continue

        # 過去データ (対象レース直前まで)
        horse_db = build_horse_db_upto(all_races, race_id)

        # 実際の結果と馬番マップ
        actual_ranks: dict[str, int] = {}
        name_to_umaban = info.get('name_to_umaban', {})
        for h in horses_raw:
            r = _int(h.get('着順'))
            if r is not None:
                actual_ranks[h['馬名']] = r

        if not actual_ranks:
            continue

        # コース・距離: CSV列があれば使用、なければフォールバック
        course = info.get('course') or 'ダート'
        dist   = info.get('dist')   or 1800

        # スコア計算 (主モデル + win/sanpuku 両方)
        scored_w = []; scored_s = []
        for h in horses_raw:
            name    = h['馬名']
            jockey  = h.get('騎手', '')
            history = horse_db.get(name, [])
            scored_w.append((name, score_horse_bt(name, jockey, course, dist, history, jstats, dc_db, weights_win)))
            scored_s.append((name, score_horse_bt(name, jockey, course, dist, history, jstats, dc_db, weights_san)))

        scored_w.sort(key=lambda x: x[1], reverse=True)
        scored_s.sort(key=lambda x: x[1], reverse=True)

        # 主モデルのスコア (model引数に従う)
        scored = scored_w if model == 'win' else scored_s

        if len(scored) < 3:
            continue

        pred1 = scored[0][0]
        pred2 = scored[1][0]
        pred3 = scored[2][0]

        rank_pred1 = actual_ranks.get(pred1, 99)
        rank_pred2 = actual_ranks.get(pred2, 99)
        rank_pred3 = actual_ranks.get(pred3, 99)

        # 両モデルの上位3頭セット
        top3_win = {scored_w[i][0] for i in range(3)}
        top3_san = {scored_s[i][0] for i in range(3)}
        models_agree = (top3_win == top3_san)

        winner = min(actual_ranks, key=actual_ranks.get)
        winner_rank_in_pred = next(
            (i + 1 for i, (name, _) in enumerate(scored) if name == winner), 99
        )

        total += 1
        ym = info.get('ym', race_id[:6])
        monthly[ym]['total'] += 1

        is_hit1    = (pred1 == winner)
        is_hit2    = (winner in [pred1, pred2])
        is_hit3    = (winner in [pred1, pred2, pred3])
        is_sanpuku = (rank_pred1 <= 3 and rank_pred2 <= 3 and rank_pred3 <= 3)
        is_niren   = (rank_pred1 <= 2 and rank_pred2 <= 2)

        if is_hit1:
            hit_1st += 1
            monthly[ym]['hit1'] += 1
        if is_hit2:
            hit_top2 += 1
        if is_hit3:
            hit_top3 += 1
            monthly[ym]['hit3'] += 1
        if is_sanpuku:
            hit_sanpuku += 1
            monthly[ym]['sanpuku'] += 1
        if is_niren:
            hit_niren += 1

        # ── 単勝回収率 ──
        tan_invest += 100
        monthly[ym]['tan_inv'] += 100
        if is_hit1:
            tansho = parse_tansho(info.get('tansho_raw', ''))
            ub_winner = name_to_umaban.get(pred1, '')
            pay = tansho.get(ub_winner, 0)
            tan_return += pay
            monthly[ym]['tan_ret'] += pay

        # ── 三連複回収率 ──
        san_invest += 100
        monthly[ym]['san_inv'] += 100
        if is_sanpuku:
            sanpuku_pays = parse_sanrenpuku(info.get('sanrenpuku_raw', ''))
            ub1 = _int(name_to_umaban.get(pred1, ''), 0)
            ub2 = _int(name_to_umaban.get(pred2, ''), 0)
            ub3 = _int(name_to_umaban.get(pred3, ''), 0)
            if ub1 and ub2 and ub3:
                key = tuple(sorted([ub1, ub2, ub3]))
                pay = sanpuku_pays.get(key, 0)
                san_return += pay
                monthly[ym]['san_ret'] += pay

        # ── 両モデル一致レースのみ集計 ──
        if models_agree:
            # 一致時は sanpuku の top3 を使う
            a1, a2, a3 = scored_s[0][0], scored_s[1][0], scored_s[2][0]
            ra1 = actual_ranks.get(a1, 99)
            ra2 = actual_ranks.get(a2, 99)
            ra3 = actual_ranks.get(a3, 99)
            agree_total += 1
            a_sanpuku = (ra1 <= 3 and ra2 <= 3 and ra3 <= 3)
            agree_sanpuku += a_sanpuku

            agree_tan_inv += 100
            if a1 == winner:
                tansho = parse_tansho(info.get('tansho_raw', ''))
                agree_tan_ret += tansho.get(name_to_umaban.get(a1, ''), 0)

            agree_san_inv += 100
            if a_sanpuku:
                sanpuku_pays = parse_sanrenpuku(info.get('sanrenpuku_raw', ''))
                i1 = _int(name_to_umaban.get(a1, ''), 0)
                i2 = _int(name_to_umaban.get(a2, ''), 0)
                i3 = _int(name_to_umaban.get(a3, ''), 0)
                if i1 and i2 and i3:
                    agree_san_ret += sanpuku_pays.get(tuple(sorted([i1, i2, i3])), 0)

        if verbose and not is_sanpuku and total <= 20:
            venue = VENUE_MAP.get(race_id[4:6], '?')
            rnum  = int(race_id[10:12])
            miss_examples.append(
                f'  {race_id[:6]} {venue}{rnum}R  '
                f'予測:{pred1}({rank_pred1}着)/{pred2}({rank_pred2}着)/{pred3}({rank_pred3}着)  '
                f'実際1着:{winner}(予測{winner_rank_in_pred}位)'
            )

    # ─── 結果表示 ───
    if total == 0:
        print('検証できるレースがありませんでした。')
        return

    print(f'\n{"="*72}')
    print(f'  バックテスト結果  [{model}モデル]  (R8-R11  {total}レース)')
    print(f'{"="*72}')
    print(f'  ■ 1着的中率    (スコア1位=実際1着):     {hit_1st:4d}/{total} = {hit_1st/total*100:5.1f}%')
    print(f'  ■ 2頭内的中率  (スコア1-2位に1着含む):  {hit_top2:4d}/{total} = {hit_top2/total*100:5.1f}%')
    print(f'  ■ 3頭内的中率  (スコア1-3位に1着含む):  {hit_top3:4d}/{total} = {hit_top3/total*100:5.1f}%')
    print(f'  ■ 三連複的中率 (1-3位が全て3着以内):    {hit_sanpuku:4d}/{total} = {hit_sanpuku/total*100:5.1f}%')
    print(f'  ■ 馬連的中率   (1-2位が1・2着):          {hit_niren:4d}/{total} = {hit_niren/total*100:5.1f}%')
    print()
    t_roi = tan_return / tan_invest * 100 if tan_invest else 0
    s_roi = san_return / san_invest * 100 if san_invest else 0
    print(f'  ■ 単勝回収率   (pred1に毎回100円):      '
          f'投資¥{tan_invest:,} 払戻¥{tan_return:,} = {t_roi:.1f}%')
    print(f'  ■ 三連複回収率 (pred1-2-3に毎回100円):  '
          f'投資¥{san_invest:,} 払戻¥{san_return:,} = {s_roi:.1f}%')

    # ── 両モデル一致レース ──
    print(f'\n  ── 両モデル一致レース（WIN上位3頭 = SANPUKU上位3頭）──')
    if agree_total == 0:
        print('  一致レースなし')
    else:
        agree_rate   = agree_total / total * 100
        a_sp_rate    = agree_sanpuku / agree_total * 100
        a_t_roi      = agree_tan_ret / agree_tan_inv * 100 if agree_tan_inv else 0
        a_s_roi      = agree_san_ret / agree_san_inv * 100 if agree_san_inv else 0
        print(f'  一致レース数: {agree_total}/{total} ({agree_rate:.1f}%)')
        print(f'  ■ 三連複的中率: {agree_sanpuku}/{agree_total} = {a_sp_rate:.1f}%  '
              f'(全体比 {a_sp_rate - hit_sanpuku/total*100:+.1f}%pt)')
        print(f'  ■ 単勝回収率:   投資¥{agree_tan_inv:,} 払戻¥{agree_tan_ret:,} = {a_t_roi:.1f}%')
        print(f'  ■ 三連複回収率: 投資¥{agree_san_inv:,} 払戻¥{agree_san_ret:,} = {a_s_roi:.1f}%')

    # 月別サマリー
    print(f'\n  ── 月別サマリー ──')
    print(f'  {"年月":>6}  {"件数":>5}  {"1着%":>7}  {"3頭内%":>7}  {"三連複%":>7}  '
          f'{"単勝ROI":>8}  {"三連複ROI":>10}')
    print(f'  {"-"*68}')
    for ym in sorted(monthly.keys()):
        m = monthly[ym]
        t = m['total']
        if t == 0:
            continue
        t_r = m['tan_ret'] / m['tan_inv'] * 100 if m['tan_inv'] else 0
        s_r = m['san_ret'] / m['san_inv'] * 100 if m['san_inv'] else 0
        print(f'  {ym:>6}  {t:>5}  '
              f'{m["hit1"]/t*100:>6.1f}%  '
              f'{m["hit3"]/t*100:>6.1f}%  '
              f'{m["sanpuku"]/t*100:>6.1f}%  '
              f'{t_r:>7.1f}%  {s_r:>9.1f}%')

    # 合計行
    print(f'  {"-"*68}')
    ta = hit_1st/total*100; tb = hit_top3/total*100; tc = hit_sanpuku/total*100
    print(f'  {"合計":>6}  {total:>5}  {ta:>6.1f}%  {tb:>6.1f}%  {tc:>6.1f}%  '
          f'{t_roi:>7.1f}%  {s_roi:>9.1f}%')

    if verbose and miss_examples:
        print(f'\n  ── 外れ例 (最大20件) ──')
        for ex in miss_examples:
            print(ex)

    print(f'\n  ※ コース・距離はCSVデータを使用（未取得分のみダート/1800mにフォールバック）')
    print(f'  ※ race_name がある場合のみ 新馬/未勝利/1勝クラス/障害 を除外')
    print('=' * 72)


# ─── エントリポイント ──────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='スコアエージェント バックテスト')
    parser.add_argument('--data',    default='data', help='データディレクトリ')
    parser.add_argument('--year',    help='年でフィルタ (例: 2025)')
    parser.add_argument('--month',   help='年月でフィルタ (例: 202510)')
    parser.add_argument('--rnum',    type=int, nargs='+', help='レース番号 (例: 8 9 10 11)')
    parser.add_argument('--verbose', action='store_true', help='外れ例を表示')
    parser.add_argument('--model',   default='sanpuku', choices=['win','sanpuku','default'],
                        help='使用モデル (default: sanpuku)')
    args = parser.parse_args()

    run_backtest(
        data_dir     = args.data,
        year_filter  = args.year,
        month_filter = args.month,
        rnum_filter  = args.rnum,
        verbose      = args.verbose,
        model        = args.model,
    )

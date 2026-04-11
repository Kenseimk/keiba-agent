"""
uscore_backtest.py  U score ウォークフォワード・バックテスト
==============================================================
テスト期間: 202501〜202603 (デフォルト)
学習期間: テスト月より前の全データ (拡張ウィンドウ)
  → 各月テスト時は前月末時点の horse_db を使用
  → テスト完了後、その月の実績を horse_db に追加

実行例:
  python uscore_backtest.py
  python uscore_backtest.py --test_start 202501 --test_end 202603
  python uscore_backtest.py --verbose
  python uscore_backtest.py --verbose --rnum 8 9 10 11
"""

import os, sys, io, glob, csv, re, argparse
from collections import defaultdict

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
except AttributeError:
    pass

from uscore import (
    analyze_race_uscore, check_bet_uscore,
    should_exclude_uscore, _float, _int, _infer_grade_from_name,
    _parse_bw, _parse_avg_pos, _parse_margin,
)


def _parse_time(s: str):
    """タイム文字列を秒数に変換: '1:12.3' → 72.3"""
    if not s: return None
    s = s.strip()
    if ':' in s:
        parts = s.split(':')
        try: return int(parts[0]) * 60 + float(parts[1])
        except: return None
    try: return float(s)
    except: return None


def _parse_first_pos(s: str):
    """通過順の最初の位置: '3-4-5' → 3.0"""
    if not s: return None
    try: return float(s.split('-')[0])
    except: return None

DATA_DIR         = 'data'
DEFAULT_START    = '202501'
DEFAULT_END      = '202603'

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
}


# ══════════════════════════════════════════════════════
# CSV 読み込み (バックテスト専用)
# ══════════════════════════════════════════════════════

def load_all_csv_races(data_dir: str = DATA_DIR) -> dict:
    """
    全 raceresults_YYYYMM.csv を読み込み、
    race_id 単位でレース情報 + 馬リストを構築して返す。

    戻り値:
      {race_id: {
          'race_id', 'race_name', 'file_ym', 'venue_code',
          'dist', 'course', 'track_cond', 'grade', 'n_field',
          'horse_list': [{name, jockey, pop, gate_num, umaban, odds, rank}, ...],
          'rows': [raw CSV rows],
      }}
    """
    race_db    = defaultdict(list)
    race_ym_map: dict[str, str] = {}

    for fpath in sorted(glob.glob(os.path.join(data_dir, 'raceresults_*.csv'))):
        m = re.search(r'raceresults_(\d{6})\.csv', os.path.basename(fpath))
        file_ym = m.group(1) if m else ''
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                row['_file_ym'] = file_ym
                rid = row['race_id']
                race_db[rid].append(row)
                if file_ym and rid not in race_ym_map:
                    race_ym_map[rid] = file_ym

    races = {}
    for race_id, rows in race_db.items():
        n_field  = len(rows)
        file_ym  = race_ym_map.get(race_id, '')
        first    = rows[0]

        # 上がり順位
        agari_list = [(h['馬名'], _float(h.get('上がり3F')))
                      for h in rows if _float(h.get('上がり3F')) is not None]
        agari_list.sort(key=lambda x: x[1])
        agari_rank_map = {name: i + 1 for i, (name, _) in enumerate(agari_list)}
        n_agari = len(agari_list)

        # venue_code
        venue_raw  = (first.get('場コード') or '').strip()
        venue_code = venue_raw.zfill(2) if venue_raw else (
            race_id[4:6] if len(race_id) >= 6 else '')

        # 馬ごとの情報リスト
        horse_list = []
        for h in rows:
            # 馬体重: "480(+4)" → 480 のみ取り出す
            bw_raw = h.get('馬体重', '')
            bw_val = _float(re.sub(r'\(.*\)', '', bw_raw).strip()) if bw_raw else None
            horse_list.append({
                'name':        h.get('馬名', ''),
                'jockey':      h.get('騎手', ''),
                'pop':         _int(h.get('人気'), 99),
                'gate_num':    _int(h.get('枠番'), 0),
                'umaban':      h.get('馬番', '').strip(),
                'odds':        _float(h.get('単勝オッズ')) or 0.0,
                'rank':        _int(h.get('着順'), 99),
                'agari_rank':  agari_rank_map.get(h.get('馬名', ''), -1),
                'agari_field': n_agari,
                'bw':          bw_val,
                'kg':          _float(h.get('斤量')),
            })

        races[race_id] = {
            'race_id':    race_id,
            'race_name':  first.get('race_name', '').strip(),
            'file_ym':    file_ym,
            'venue_code': venue_code,
            'dist':       _int(first.get('距離'), 1800),
            'course':     (first.get('コース') or '').strip(),
            'track_cond': (first.get('馬場状態') or '').strip(),
            'grade':      (first.get('grade') or '').strip(),
            'n_field':    n_field,
            'horse_list': horse_list,
            'rows':       rows,
        }

    return races


def build_horse_db_from_races(races: dict, upto_ym: str) -> dict:
    """
    file_ym < upto_ym のレースのみを使って horse_db を構築。
    (テスト開始月より前の全データで初期化)
    """
    horse_db: dict[str, list] = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=upto_ym)
    # 最新順にソート
    for name in horse_db:
        horse_db[name].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    return dict(horse_db)


def _build_race_records(race_id: str, info: dict) -> dict[str, dict]:
    """race_id の各馬のレコードを {馬名: record} で返す"""
    rows    = info['rows']
    n_field = info['n_field']
    file_ym = info['file_ym']

    agari_list = [(h['馬名'], _float(h.get('上がり3F')))
                  for h in rows if _float(h.get('上がり3F')) is not None]
    agari_list.sort(key=lambda x: x[1])
    agari_rank_map = {name: i + 1 for i, (name, _) in enumerate(agari_list)}
    n_agari = len(agari_list)

    # タイム順位 (1=最速)
    dist_val = _int(rows[0].get('距離'), 1800) if rows else 1800
    time_list = [(h['馬名'], _parse_time(h.get('タイム')))
                 for h in rows if _parse_time(h.get('タイム')) is not None]
    time_list.sort(key=lambda x: x[1])
    time_rank_map = {name: i + 1 for i, (name, _) in enumerate(time_list)}
    n_timed = len(time_list)

    # 区間速度の計算: 前半 / 後半(上がり3F=600m) ランク
    def _calc_pace(t_sec, agari_sec, dist):
        """前半速度・後半速度・ペース差を返す"""
        if t_sec is None or agari_sec is None or t_sec <= 0 or agari_sec <= 0:
            return None, None, None
        front_dist = max(dist - 600, 0)
        front_sec  = t_sec - agari_sec
        if front_sec <= 0 or front_dist <= 0:
            return None, None, None
        front_spd = front_dist / front_sec   # m/s 前半
        late_spd  = 600.0 / agari_sec        # m/s 後半
        pace_gap  = late_spd - front_spd     # 正=末脚型、負=先行型
        return front_spd, late_spd, pace_gap

    # 各馬の前半/後半速度を事前計算してランク付け
    pace_data = {}
    for h in rows:
        name_h = h.get('馬名', '')
        t   = _parse_time(h.get('タイム'))
        ag  = _float(h.get('上がり3F'))
        fs, ls, pg = _calc_pace(t, ag, dist_val)
        pace_data[name_h] = (fs, ls, pg)

    front_speeds = [(n, v[0]) for n, v in pace_data.items() if v[0] is not None]
    late_speeds  = [(n, v[1]) for n, v in pace_data.items() if v[1] is not None]
    front_speeds.sort(key=lambda x: -x[1])  # 大=速い
    late_speeds.sort(key=lambda x: -x[1])
    front_rank_map = {n: i + 1 for i, (n, _) in enumerate(front_speeds)}
    late_rank_map  = {n: i + 1 for i, (n, _) in enumerate(late_speeds)}
    n_pace = len(front_speeds)

    records = {}
    for h in rows:
        rank = _int(h.get('着順'))
        if not rank:
            continue
        name_h = h.get('馬名', '')
        venue_raw  = (h.get('場コード') or '').strip()
        venue_code = venue_raw.zfill(2) if venue_raw else (
            race_id[4:6] if len(race_id) >= 6 else '00')
        bw_raw = h.get('馬体重', '')
        bw_val = _float(re.sub(r'\(.*\)', '', bw_raw).strip()) if bw_raw else None
        t_sec  = _parse_time(h.get('タイム'))
        fs, ls, pg = pace_data.get(name_h, (None, None, None))
        records[name_h] = {
            'race_id':        race_id,
            'race_ym':        file_ym,
            'venue_code':     venue_code,
            'rank':           rank,
            'field_size':     n_field,
            'jockey':         h.get('騎手', ''),
            'odds':           _float(h.get('単勝オッズ')),
            'pop':            _int(h.get('人気'), 0),
            'agari':          _float(h.get('上がり3F')),
            'agari_rank':     agari_rank_map.get(name_h, -1),
            'agari_field':    n_agari,
            'avg_pos':        _parse_avg_pos(h.get('通過順', '')),
            'first_pos':      _parse_first_pos(h.get('通過順', '')),
            'bw_chg':         _parse_bw(h.get('馬体重', '')),
            'bw':             bw_val,
            'weight_carried': _float(h.get('斤量')),
            'margin':         _parse_margin(h.get('着差', ''), rank),
            'dist':           _int(h.get('距離')),
            'course':         (h.get('コース') or '').strip(),
            'track_cond':     (h.get('馬場状態') or '').strip(),
            'gate_num':       _int(h.get('枠番'), 0),
            'grade':          ((h.get('grade') or '').strip()
                              or _infer_grade_from_name(h.get('race_name', ''))),
            'race_time':      t_sec,
            'speed_mps':      (dist_val / t_sec) if t_sec and t_sec > 0 else None,
            'time_rank':      time_rank_map.get(name_h, -1),
            'n_timed':        n_timed,
            # ── 区間速度 ──
            'front_speed':    fs,
            'late_speed':     ls,
            'pace_gap':       pg,
            'front_rank':     front_rank_map.get(name_h, -1),
            'late_rank':      late_rank_map.get(name_h, -1),
            'n_pace':         n_pace,
        }
    return records


def _add_races_to_horse_db(horse_db: dict, races: dict,
                           upto_ym: str = None, exact_ym: str = None):
    """
    races のうち条件に合うものを horse_db に追加。
      upto_ym   : file_ym < upto_ym のレースのみ
      exact_ym  : file_ym == exact_ym のレースのみ
    ソートは呼び出し元が行う。
    """
    for race_id, info in races.items():
        ym = info['file_ym']
        if upto_ym  is not None and ym >= upto_ym:
            continue
        if exact_ym is not None and ym != exact_ym:
            continue
        for name, rec in _build_race_records(race_id, info).items():
            horse_db.setdefault(name, []).append(rec)


# ══════════════════════════════════════════════════════
# race_info 構築 (analyze_race_uscore 向け)
# ══════════════════════════════════════════════════════

def make_race_info(info: dict) -> dict:
    """
    load_all_csv_races の1レース情報 → analyze_race_uscore の race_info 形式に変換
    """
    hl = info['horse_list']
    return {
        'race_id':    info['race_id'],
        'race_name':  info['race_name'],
        'venue_code': info['venue_code'],
        '_file_ym':   info['file_ym'],
        'race_ym':    info['file_ym'],
        'dist':       info['dist'],
        'course':     info['course'],
        'track_cond': info['track_cond'],
        # horses: name, jockey, gate_num, pop, bw, kg
        'horses': [
            {'name': h['name'], 'jockey': h['jockey'],
             'gate_num': h['gate_num'], 'pop': h['pop'],
             'bw': h.get('bw'), 'kg': h.get('kg')}
            for h in hl
        ],
        # odds: name, odds
        'odds': [
            {'name': h['name'], 'odds': h['odds']}
            for h in hl
        ],
    }


# ══════════════════════════════════════════════════════
# 実績照合
# ══════════════════════════════════════════════════════

def get_winner(info: dict) -> tuple[str, float]:
    """実際の1着馬名と単勝オッズを返す"""
    for h in info['horse_list']:
        if h['rank'] == 1:
            return h['name'], h['odds']
    return '', 0.0


# ══════════════════════════════════════════════════════
# ウォークフォワード本体
# ══════════════════════════════════════════════════════

def run_walkforward(
    test_start:   str = DEFAULT_START,
    test_end:     str = DEFAULT_END,
    verbose:      bool = False,
    rnum_filter:  list[int] = None,
    uscore_threshold: float = 100.0,   # ベット閾値
    top_n_bet:    int = 1,             # 1レースで最大何頭にベット
) -> None:
    print(f'=== U score ウォークフォワード・バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'ベット条件: U_score >= {uscore_threshold:.0f}  上位{top_n_bet}頭')
    print(f'学習: 拡張ウィンドウ (テスト月より前の全データ)\n')

    # ── 全レース読み込み ──────────────────────────────
    print('全 CSV 読み込み中 ...', end=' ', flush=True)
    races = load_all_csv_races(DATA_DIR)
    print(f'{len(races):,} レース')

    # ── テスト対象月リスト ────────────────────────────
    test_months = sorted(set(
        info['file_ym'] for info in races.values()
        if info['file_ym'] and test_start <= info['file_ym'] <= test_end
    ))
    if not test_months:
        print('対象レースが見つかりません')
        return

    # ── 初期 horse_db (テスト開始前の全データ) ────────
    print(f'初期 horse_db 構築 (〜{test_start} 前) ...', end=' ', flush=True)
    horse_db: dict = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=test_start)
    for name in horse_db:
        horse_db[name].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'{len(horse_db):,} 頭')

    # ── 月別集計バッファ ──────────────────────────────
    monthly_stats: list[dict] = []

    for ym in test_months:
        month_races = {
            rid: info for rid, info in races.items()
            if info['file_ym'] == ym
        }

        bet_list   = []   # {'race_id', 'name', 'u_score', 'odds', 'hit': bool}
        skipped_cnt = 0

        for race_id in sorted(month_races.keys()):
            info = month_races[race_id]
            race_name = info['race_name']

            # クラスフィルタ
            if should_exclude_uscore(race_name):
                skipped_cnt += 1
                continue

            # R番号フィルタ
            if rnum_filter:
                try:
                    rnum = int(race_id[-2:])
                    if rnum not in rnum_filter:
                        continue
                except ValueError:
                    pass

            # オッズが入っていないレースはスキップ
            if all(h['odds'] == 0.0 for h in info['horse_list']):
                skipped_cnt += 1
                continue

            # race_info 構築 & スコアリング
            race_info_obj = make_race_info(info)
            try:
                results = analyze_race_uscore(
                    race_info_obj, horse_db,
                    jstats=None, dc_db=None,
                )
            except Exception as e:
                if verbose:
                    print(f'  [WARN] {race_id} エラー: {e}')
                continue

            if not results:
                continue

            # 実際の着順マップ
            rank_map = {h['name']: h['rank'] for h in info['horse_list']}
            odds_map = {h['name']: h['odds'] for h in info['horse_list']}

            # ベット対象: U_score >= 閾値 の上位 top_n_bet 頭
            bets_this_race = 0
            for r in results:
                if r['u_score'] < uscore_threshold:
                    break
                if bets_this_race >= top_n_bet:
                    break
                name     = r['name']
                bet_odds = odds_map.get(name, r['odds'])
                hit      = (rank_map.get(name, 99) == 1)
                bet_list.append({
                    'race_id':  race_id,
                    'race_name': race_name,
                    'name':     name,
                    'u_score':  r['u_score'],
                    'odds':     bet_odds,
                    'pop':      r['pop'],
                    'hit':      hit,
                })
                bets_this_race += 1

                if verbose:
                    mark = '◎' if hit else '×'
                    print(f"  {mark} {race_id} {race_name} | {name} U={r['u_score']:.1f} {bet_odds:.1f}倍 pop={r['pop']}")

        # 月次集計
        n_bets  = len(bet_list)
        n_hits  = sum(1 for b in bet_list if b['hit'])
        paid    = sum(b['odds'] for b in bet_list if b['hit'])
        cost    = float(n_bets)
        roi     = paid / cost * 100 if cost > 0 else 0.0
        win_rate = n_hits / n_bets * 100 if n_bets > 0 else 0.0

        monthly_stats.append({
            'ym':       ym,
            'n_bets':   n_bets,
            'n_hits':   n_hits,
            'paid':     paid,
            'cost':     cost,
            'roi':      roi,
            'win_rate': win_rate,
            'bets':     bet_list,
        })

        print(f"  {ym}: ベット{n_bets:3d}R  的中{n_hits:3d}  勝率{win_rate:5.1f}%  "
              f"回収{paid:7.1f}円/{cost:.0f}円  ROI={roi:6.1f}%")

        # ── この月のデータを horse_db に追加 ──────────
        for race_id, info in month_races.items():
            for name, rec in _build_race_records(race_id, info).items():
                horse_db.setdefault(name, []).insert(0, rec)

    # ══════════════════════════════════════════════════
    # 全体集計
    # ══════════════════════════════════════════════════
    total_bets = sum(s['n_bets'] for s in monthly_stats)
    total_hits = sum(s['n_hits'] for s in monthly_stats)
    total_paid = sum(s['paid']   for s in monthly_stats)
    total_cost = sum(s['cost']   for s in monthly_stats)
    total_roi  = total_paid / total_cost * 100 if total_cost > 0 else 0.0
    total_wr   = total_hits / total_bets * 100  if total_bets > 0 else 0.0

    print()
    print('=' * 72)
    print(f"{'月':>8}  {'ベット':>6}  {'的中':>5}  {'勝率':>7}  {'回収':>10}  {'ROI':>8}")
    print('-' * 72)
    for s in monthly_stats:
        print(f"  {s['ym']}  {s['n_bets']:6d}  {s['n_hits']:5d}  {s['win_rate']:6.1f}%"
              f"  {s['paid']:8.1f}円  {s['roi']:7.1f}%")
    print('-' * 72)
    print(f"  {'合計':>8}  {total_bets:6d}  {total_hits:5d}  {total_wr:6.1f}%"
          f"  {total_paid:8.1f}円  {total_roi:7.1f}%")
    print('=' * 72)
    print()
    print(f'※ 1単位=100円換算で投資{total_cost*100:.0f}円 → 回収{total_paid*100:.0f}円')
    print(f'   損益: {(total_paid - total_cost)*100:+.0f}円')

    # ── 閾値別サマリ（参考）─────────────────────────
    all_bets = [b for s in monthly_stats for b in s['bets']]
    if all_bets:
        print()
        print('── 閾値別回収率 ──')
        print(f"{'閾値':>6}  {'ベット':>6}  {'的中':>5}  {'ROI':>8}")
        for thr in [80, 90, 100, 110, 120, 150]:
            tb = [b for b in all_bets if b['u_score'] >= thr]
            if not tb:
                continue
            th = sum(1 for b in tb if b['hit'])
            tp = sum(b['odds'] for b in tb if b['hit'])
            tr = tp / len(tb) * 100
            print(f"  {thr:4d}  {len(tb):6d}  {th:5d}  {tr:7.1f}%")


# ══════════════════════════════════════════════════════
# CLI エントリ
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='U score ウォークフォワード・バックテスト')
    parser.add_argument('--test_start', default=DEFAULT_START,
                        help=f'テスト開始月 YYYYMM (デフォルト: {DEFAULT_START})')
    parser.add_argument('--test_end',   default=DEFAULT_END,
                        help=f'テスト終了月 YYYYMM (デフォルト: {DEFAULT_END})')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='各ベット詳細を表示')
    parser.add_argument('--rnum', nargs='*', type=int, default=None,
                        help='R番号フィルタ (例: --rnum 8 9 10 11)')
    parser.add_argument('--threshold', type=float, default=100.0,
                        help='ベット閾値 U_score (デフォルト: 100)')
    parser.add_argument('--top_n', type=int, default=1,
                        help='1レースで最大ベット頭数 (デフォルト: 1)')
    args = parser.parse_args()

    run_walkforward(
        test_start=args.test_start,
        test_end=args.test_end,
        verbose=args.verbose,
        rnum_filter=args.rnum,
        uscore_threshold=args.threshold,
        top_n_bet=args.top_n,
    )


if __name__ == '__main__':
    main()

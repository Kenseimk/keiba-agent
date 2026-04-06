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

    records = {}
    for h in rows:
        rank = _int(h.get('着順'))
        if not rank:
            continue
        venue_raw  = (h.get('場コード') or '').strip()
        venue_code = venue_raw.zfill(2) if venue_raw else (
            race_id[4:6] if len(race_id) >= 6 else '00')
        records[h['馬名']] = {
            'race_id':     race_id,
            'race_ym':     file_ym,
            'venue_code':  venue_code,
            'rank':        rank,
            'field_size':  n_field,
            'jockey':      h.get('騎手', ''),
            'odds':        _float(h.get('単勝オッズ')),
            'pop':         _int(h.get('人気'), 0),
            'agari':       _float(h.get('上がり3F')),
            'agari_rank':  agari_rank_map.get(h.get('馬名', ''), -1),
            'agari_field': n_agari,
            'avg_pos':     _parse_avg_pos(h.get('通過順', '')),
            'bw_chg':      _parse_bw(h.get('馬体重', '')),
            'margin':      _parse_margin(h.get('着差', ''), rank),
            'dist':        _int(h.get('距離')),
            'course':      (h.get('コース') or '').strip(),
            'track_cond':  (h.get('馬場状態') or '').strip(),
            'gate_num':    _int(h.get('枠番'), 0),
            'grade':       ((h.get('grade') or '').strip()
                           or _infer_grade_from_name(h.get('race_name', ''))),
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
        # horses: name, jockey, gate_num, pop
        'horses': [
            {'name': h['name'], 'jockey': h['jockey'],
             'gate_num': h['gate_num'], 'pop': h['pop']}
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
    test_start: str = DEFAULT_START,
    test_end:   str = DEFAULT_END,
    verbose:    bool = False,
    rnum_filter: list[int] = None,
) -> None:
    print(f'=== U score ウォークフォワード・バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
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

        cover_races = []   # {'race_id', 'covered': bool, 'top5_names', 'place3_names'}
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

            # 上位5頭が3着内を全カバーしているか
            top5_names   = {r['name'] for r in results[:5]}
            place3_names = {h['name'] for h in info['horse_list'] if h['rank'] in (1, 2, 3)}
            covered      = place3_names.issubset(top5_names)

            cover_races.append({
                'race_id':      race_id,
                'race_name':    race_name,
                'covered':      covered,
                'top5_names':   top5_names,
                'place3_names': place3_names,
            })

            if verbose:
                mark = '◯' if covered else '×'
                top5_str = ', '.join(list(top5_names)[:5])
                p3_str   = ', '.join(place3_names)
                print(f"  {mark} {race_id} {race_name} | 上位5: {top5_str} | 3着内: {p3_str}")

        # 月次集計
        n_races   = len(cover_races)
        n_covered = sum(1 for c in cover_races if c['covered'])
        cover_rate = n_covered / n_races * 100 if n_races > 0 else 0.0

        monthly_stats.append({
            'ym':         ym,
            'n_races':    n_races,
            'n_covered':  n_covered,
            'cover_rate': cover_rate,
            'cover_races': cover_races,
        })

        print(f"  {ym}: 対象{n_races:3d}R / カバー{n_covered:3d}R ({cover_rate:5.1f}%)")

        # ── この月のデータを horse_db に追加 ──────────
        for race_id, info in month_races.items():
            for name, rec in _build_race_records(race_id, info).items():
                horse_db.setdefault(name, []).insert(0, rec)  # 最新を先頭に

    # ══════════════════════════════════════════════════
    # 全体集計
    # ══════════════════════════════════════════════════
    total_races   = sum(s['n_races']   for s in monthly_stats)
    total_covered = sum(s['n_covered'] for s in monthly_stats)
    total_rate    = total_covered / total_races * 100 if total_races > 0 else 0.0

    print()
    print('=' * 55)
    print(f"{'月':>8}  {'対象R':>5}  {'カバー':>6}  {'カバー率':>8}")
    print('-' * 55)
    for s in monthly_stats:
        print(f"  {s['ym']}  {s['n_races']:5d}  {s['n_covered']:6d}  {s['cover_rate']:7.1f}%")
    print('-' * 55)
    print(f"  {'合計':>8}  {total_races:5d}  {total_covered:6d}  {total_rate:7.1f}%")
    print('=' * 55)
    print()
    print(f'※ カバー = U_score 上位5頭に3着内馬が全員含まれているレース')


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
    args = parser.parse_args()

    run_walkforward(
        test_start=args.test_start,
        test_end=args.test_end,
        verbose=args.verbose,
        rnum_filter=args.rnum,
    )


if __name__ == '__main__':
    main()

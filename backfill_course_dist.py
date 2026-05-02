"""
backfill_course_dist.py  R8-R11 レースの距離・コース・馬場状態をバックフィル
======================================================
距離/コース列がない古いCSVに対して、R8-R11 の race_id だけを
db.netkeiba.com から取得して更新する（全レースより10倍高速）。

使い方:
  python backfill_course_dist.py            # 全未取得ファイルを処理
  python backfill_course_dist.py --year 2024
  python backfill_course_dist.py --dry-run  # 取得せず件数確認のみ
"""

import os, sys, re, csv, time, random, glob, argparse, requests
from bs4 import BeautifulSoup
from collections import defaultdict

DATA_DIR   = 'data'
SLEEP_MIN  = 2.0
SLEEP_MAX  = 4.0
BURST_N    = 20      # N件ごとに長め休憩
BURST_WAIT = (15, 25)

BASE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://db.netkeiba.com/',
}

NEW_COLUMNS = [
    'race_id', 'race_name', 'grade', '距離', 'コース', '馬場状態',
    '着順', '枠番', '馬番', '馬名', '性齢', '斤量', '騎手',
    'タイム', '着差', '通過順', '上がり3F', '単勝オッズ', '人気', '馬体重',
    '年', '場コード', '回次', '日次', 'レース番号',
    '単勝払戻', '複勝払戻', '馬連払戻', '馬単払戻', 'ワイド払戻', '三連複払戻', '三連単払戻',
]


def fetch_meta(session, race_id: str) -> tuple:
    """(race_name, grade, dist, course, track_cond) を返す。失敗時は空文字列。"""
    url = f'https://db.netkeiba.com/race/{race_id}/'
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 404:
            return '', '', '', '', ''
        if r.status_code == 429:
            print(f'  [429] レート制限 → 60秒待機')
            time.sleep(60)
            return fetch_meta(session, race_id)
        r.encoding = 'EUC-JP'
        soup = BeautifulSoup(r.text, 'html.parser')

        title_text = (soup.select_one('title') or type('', (), {'get_text': lambda *a, **k: ''})()).get_text()

        race_name = ''
        for sel in ['.RaceName', '.race_name', 'h1.RaceMainTitle', '.RaceMainTitle']:
            el = soup.select_one(sel)
            if el:
                race_name = el.get_text(strip=True)
                break
        if not race_name:
            m = re.match(r'^([^\d｜|（(]+?)(?:\s*[\(（]G[123][\)）])?\s*(?:\d|｜|\||結果|出走)', title_text.strip())
            if m:
                race_name = m.group(1).strip()

        grade = ''
        gm = re.search(r'[\(（](G[123])[\)）]', title_text)
        if gm:
            grade = gm.group(1)

        dist_text = course = track_cond = ''
        snap = soup.select_one('.mainrace_data') or soup.select_one('.diary_snap_cut')
        if snap:
            snap_str = snap.get_text(' ', strip=True)
            dm = re.search(r'(芝|ダ|障).*?(\d{3,4})m', snap_str)
            if dm:
                surface = dm.group(1)
                dist_text = dm.group(2)
                course = '芝' if surface == '芝' else ('ダート' if surface == 'ダ' else '障害')
            tc = re.search(r'(?:芝|ダート)\s*:\s*(良|稍重|重|不良)', snap_str)
            if tc:
                track_cond = tc.group(1)

        return race_name, grade, dist_text, course, track_cond

    except Exception as e:
        print(f'  [ERROR] {race_id}: {e}')
        return '', '', '', '', ''


def process_file(fpath: str, session, dry_run: bool = False):
    print(f'\n--- {os.path.basename(fpath)} ---')

    with open(fpath, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print('  空ファイル スキップ')
        return

    fieldnames = list(rows[0].keys())
    has_dist   = '距離' in fieldnames

    # 全レース番号の未取得 race_id を収集
    existing_meta = {}
    if has_dist:
        for row in rows:
            rid = row['race_id']
            if rid not in existing_meta and row.get('距離', '').strip():
                existing_meta[rid] = (
                    row['race_name'], row.get('grade', ''),
                    row['距離'], row['コース'], row['馬場状態']
                )

    target_ids = sorted({
        row['race_id'] for row in rows
        if row['race_id'] not in existing_meta
    })

    print(f'  未取得: {len(target_ids)} race_id')
    if dry_run or not target_ids:
        return

    # 取得
    meta_map = dict(existing_meta)
    for i, rid in enumerate(target_ids, 1):
        if i > 1 and (i - 1) % BURST_N == 0:
            wait = random.uniform(*BURST_WAIT)
            print(f'  [{i}/{len(target_ids)}] {BURST_N}件完了 → {wait:.0f}秒待機')
            time.sleep(wait)

        name, grade, dist, course, track = fetch_meta(session, rid)
        meta_map[rid] = (name, grade, dist, course, track)

        if i <= 3 or i % 50 == 0:
            print(f'  [{i}/{len(target_ids)}] {rid} → {dist}m {course} {track}')

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    # CSV 書き直し
    out_cols = NEW_COLUMNS
    # 既存CSVに新列がない場合は追加
    with open(fpath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=out_cols, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            rid = row['race_id']
            name, grade, dist, course, track = meta_map.get(rid, ('', '', '', '', ''))
            row['race_name'] = row.get('race_name', '') or name
            row['grade']     = row.get('grade', '')     or grade
            row['距離']      = row.get('距離', '')      or dist
            row['コース']    = row.get('コース', '')    or course
            row['馬場状態']  = row.get('馬場状態', '')  or track
            writer.writerow(row)

    print(f'  完了: {len(rows)} 行更新')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',    type=int, help='処理する年 (例: 2024)')
    parser.add_argument('--dry-run', action='store_true', help='件数確認のみ')
    args = parser.parse_args()

    pattern = f'{DATA_DIR}/raceresults_{args.year}*.csv' if args.year \
              else f'{DATA_DIR}/raceresults_*.csv'
    files = sorted(glob.glob(pattern))

    # 距離列がない、または空行が残っているファイルを対象にする
    targets = []
    for fpath in files:
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows_check = list(reader)
        has_col = '距離' in (rows_check[0].keys() if rows_check else [])
        if not has_col:
            targets.append(fpath)
            continue
        # 距離列があっても空行が残っているファイルも対象
        empty = sum(1 for r in rows_check if not r.get('距離', '').strip())
        if empty > 0:
            targets.append(fpath)

    print(f'補完対象ファイル: {len(targets)} 件')
    if not targets:
        print('全ファイル対応済みです。')
        return

    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    session.get('https://db.netkeiba.com/', timeout=10)
    time.sleep(2)

    for fpath in targets:
        process_file(fpath, session, dry_run=args.dry_run)
        if not args.dry_run:
            time.sleep(random.uniform(5, 10))

    print('\n全処理完了')


if __name__ == '__main__':
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

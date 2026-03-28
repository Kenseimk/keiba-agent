"""
backfill_race_names.py - 既存raceresults CSVにrace_name・grade列を追加

既存の raceresults_YYYYMM.csv を読み込み、
db.netkeiba.com から各レースのレース名・グレードを取得して更新する。

使い方:
  python backfill_race_names.py                    # data/ 以下の全CSVを処理
  python backfill_race_names.py --year 2024        # 2024年分のみ
  python backfill_race_names.py --file raceresults_202412.csv
"""
import os, re, csv, time, random, glob, argparse, requests
from bs4 import BeautifulSoup

DATA_DIR = 'data'
SLEEP_MIN, SLEEP_MAX = 2.0, 4.0
BURST_INTERVAL = 20

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
    '単勝払戻', '複勝払戻', '馬連払戻', '馬単払戻', 'ワイド払戻', '三連複払戻', '三連単払戻'
]


def get_race_meta(session, race_id):
    """db.netkeiba.com からレース名・グレード・距離・コース・馬場状態を取得"""
    url = f'https://db.netkeiba.com/race/{race_id}/'
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 404:
            return '', '', '', '', ''
        if r.status_code == 429:
            print(f'  [429] レート制限 → 60秒待機')
            time.sleep(60)
            return get_race_meta(session, race_id)
        r.encoding = 'EUC-JP'
        soup = BeautifulSoup(r.text, 'html.parser')

        # タイトル
        title_tag = soup.select_one('title')
        title_text = title_tag.get_text() if title_tag else ''

        # レース名
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

        # グレード
        grade = ''
        gm = re.search(r'[\(（](G[123])[\)）]', title_text)
        if gm:
            grade = gm.group(1)

        # 距離・コース・馬場状態
        dist_text = course = track_cond = ''
        snap = soup.select_one('.mainrace_data') or soup.select_one('.diary_snap_cut')
        if snap:
            snap_str = snap.get_text(' ', strip=True)
            dm = re.search(r'(芝|ダ|障).*?(\d{3,4})m', snap_str)
            if dm:
                surface_char = dm.group(1)
                dist_text = dm.group(2)
                course = '芝' if surface_char == '芝' else ('ダート' if surface_char == 'ダ' else '障害')
            tc = re.search(r'(?:芝|ダート)\s*:\s*(良|稍重|重|不良)', snap_str)
            if tc:
                track_cond = tc.group(1)

        return race_name, grade, dist_text, course, track_cond

    except Exception as e:
        print(f'  [ERROR] {race_id}: {e}')
        return '', '', '', '', ''


def process_file(fpath, session):
    print(f'\n処理中: {fpath}')

    # 既存データ読み込み
    rows = []
    with open(fpath, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print('  空ファイルスキップ')
        return

    # race_name列が既にあるか確認
    has_race_name = 'race_name' in rows[0]

    # 未取得のrace_idを収集
    race_ids = sorted(set(r['race_id'] for r in rows))
    race_name_map = {}

    if has_race_name:
        # 既存の値を引き継ぐ（距離・コース・馬場も）
        for r in rows:
            rid = r['race_id']
            if rid not in race_name_map and r.get('race_name', '').strip():
                race_name_map[rid] = (
                    r['race_name'], r.get('grade', ''),
                    r.get('距離', ''), r.get('コース', ''), r.get('馬場状態', '')
                )

    # 未取得分のみフェッチ
    to_fetch = [rid for rid in race_ids if rid not in race_name_map]
    print(f'  {len(race_ids)}件中 {len(to_fetch)}件が未取得')

    for i, rid in enumerate(to_fetch, 1):
        if i > 1 and (i - 1) % BURST_INTERVAL == 0:
            wait = random.uniform(15, 25)
            print(f'  [{i}/{len(to_fetch)}] {BURST_INTERVAL}件完了 → {wait:.1f}秒待機')
            time.sleep(wait)

        name, grade, dist, course, track = get_race_meta(session, rid)
        race_name_map[rid] = (name, grade, dist, course, track)

        if i <= 3 or i % 50 == 0:
            print(f'  [{i}/{len(to_fetch)}] {rid} → "{name}" {grade} {dist}m {course} {track}')

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    # 更新データを書き込み
    with open(fpath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=NEW_COLUMNS, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            rid = row['race_id']
            name, grade, dist, course, track = race_name_map.get(rid, ('', '', '', '', ''))
            row['race_name'] = name
            row['grade']     = grade
            row['距離']      = dist
            row['コース']    = course
            row['馬場状態']  = track
            writer.writerow(row)

    print(f'  ✅ 完了: {len(rows)}行 更新')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, help='処理する年（指定なしで全年）')
    parser.add_argument('--file', help='特定のファイルのみ処理')
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    session.get('https://db.netkeiba.com/', timeout=10)
    time.sleep(2)

    if args.file:
        fpath = os.path.join(DATA_DIR, args.file) if not os.path.isabs(args.file) else args.file
        process_file(fpath, session)
    else:
        pattern = f'{DATA_DIR}/raceresults_{args.year}*.csv' if args.year else f'{DATA_DIR}/raceresults_*.csv'
        files = sorted(glob.glob(pattern))
        print(f'対象ファイル: {len(files)}件')
        for fpath in files:
            process_file(fpath, session)
            time.sleep(random.uniform(5, 10))

    print('\n全処理完了')


if __name__ == '__main__':
    import sys, io
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

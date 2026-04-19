"""
fetch_nar_history.py - 地方競馬(NAR) nar.netkeiba.com スクレイパー CSV出力版

出力: data/raceresults_nar_YYYYMM.csv (JRA版と同一スキーマ)

使い方:
  python fetch_nar_history.py --year 2023 --month 1
  python fetch_nar_history.py --year 2023 --month 1 --race-min 1 --race-max 12
"""
import os, re, csv, time, random, argparse, calendar, requests
from bs4 import BeautifulSoup

DATA_DIR   = 'data'
BASE       = 'https://nar.netkeiba.com'
ENC        = 'euc-jp'

SLEEP_MIN      = 2.0
SLEEP_MAX      = 4.5
SLEEP_LONG_MIN = 10.0
SLEEP_LONG_MAX = 18.0
BURST_INTERVAL = 20
MAX_RETRIES    = 3
SESSION_RESET_INTERVAL = 60

NAR_VENUE = {
    '30':'門別','31':'帯広','32':'盛岡','33':'水沢',
    '34':'浦和','35':'船橋','36':'大井','37':'川崎',
    '38':'金沢','39':'笠松','40':'名古屋','41':'園田',
    '42':'姫路','43':'高知','44':'佐賀',
}

CSV_COLUMNS = [
    'race_id','race_name','grade','距離','コース','馬場状態',
    '着順','枠番','馬番','馬名','性齢','斤量','騎手',
    'タイム','着差','通過順','上がり3F','単勝オッズ','人気','馬体重',
    '調教師','調教タイム',
    '年','場コード','回次','日次','レース番号',
    '単勝払戻','複勝払戻','馬連払戻','馬単払戻','ワイド払戻','三連複払戻','三連単払戻',
]

BASE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    'Referer': BASE + '/',
}

_session = None
_session_count = 0


def _get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_RESET_INTERVAL:
        s = requests.Session()
        s.headers.update(BASE_HEADERS)
        try:
            s.get(BASE + '/', timeout=10)
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as e:
            print(f'[session] init失敗（続行）: {e}')
        _session = s
        _session_count = 0
        print('[session] セッション初期化')
    return _session


def _sleep(long=False):
    t = (random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
         if long else random.uniform(SLEEP_MIN, SLEEP_MAX))
    time.sleep(t)


def _get(url):
    global _session_count
    s = _get_session()
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(url, timeout=25)
            r.raise_for_status()
            _session_count += 1
            return r
        except Exception as e:
            print(f'  [WARN] {url[:70]} attempt={attempt+1} err={e}')
            time.sleep(6 * (attempt + 1))
    return None


def decode(r):
    return r.content.decode(ENC, errors='replace')


# ── レースID取得 ──────────────────────────────────────────────
def fetch_race_ids_for_date(date_str, race_min=1, race_max=12):
    url = f'{BASE}/top/race_list_sub.html?kaisai_date={date_str}'
    r = _get(url)
    if not r:
        return []
    text = decode(r)
    ids = []
    seen = set()
    for rid in re.findall(r'race_id=(\d{12})', text):
        venue = rid[4:6]
        rnum  = int(rid[10:12])
        if venue in NAR_VENUE and race_min <= rnum <= race_max and rid not in seen:
            seen.add(rid)
            ids.append(rid)
    return sorted(ids)


# ── 払戻パース ────────────────────────────────────────────────
def _parse_payout_row(txt):
    amounts = [int(m.replace(',', '')) for m in re.findall(r'[\d,]+(?=円)', txt)]
    nums = re.findall(r'\b(\d{1,2})\b', txt.split('円')[0])
    return nums, amounts


def _fmt_tansho(nums, amounts):
    return f'{nums[0]}:{amounts[0]}' if nums and amounts else ''


def _fmt_fukusho(nums, amounts):
    parts = [f'{nums[i]}:{amounts[i]}' for i in range(min(3, len(nums), len(amounts)))]
    return '|'.join(parts)


def _fmt_umaren(nums, amounts):
    return (f'{nums[0]}-{nums[1]}:{amounts[0]}'
            if len(nums) >= 2 and amounts else '')


def _fmt_umatan(nums, amounts):
    return (f'{nums[0]} → {nums[1]}:{amounts[0]}'
            if len(nums) >= 2 and amounts else '')


def _fmt_wide(nums, amounts):
    parts = []
    for i in range(min(3, len(amounts))):
        if i * 2 + 1 < len(nums):
            parts.append(f'{nums[i*2]}-{nums[i*2+1]}:{amounts[i]}')
    return '|'.join(parts)


def _fmt_sanrenpuku(nums, amounts):
    return (f'{nums[0]}-{nums[1]}-{nums[2]}:{amounts[0]}'
            if len(nums) >= 3 and amounts else '')


def _fmt_sanrentan(nums, amounts):
    return (f'{nums[0]} → {nums[1]} → {nums[2]}:{amounts[0]}'
            if len(nums) >= 3 and amounts else '')


# ── レース結果取得 ────────────────────────────────────────────
def fetch_race_result(race_id):
    url = f'{BASE}/race/result.html?race_id={race_id}'
    r = _get(url)
    if not r:
        return []

    text = decode(r)
    soup = BeautifulSoup(text, 'html.parser')

    # ── レース情報 ──
    race_name = ''
    for sel in ['.RaceName', 'h1.RaceName', 'h1']:
        el = soup.select_one(sel)
        if el:
            race_name = el.get_text(strip=True)
            if race_name:
                break
    if not race_name:
        title_el = soup.find('title')
        if title_el:
            race_name = re.sub(r'の結果.*', '', title_el.get_text(strip=True)).strip()

    course_type = 'ダート'
    dist_val    = 0
    track_cond  = ''
    for el in soup.find_all(class_=re.compile(r'RaceData')):
        rd = el.get_text(' ', strip=True)
        dm = re.search(r'([芝ダ障])(\d{3,4})m', rd)
        if dm:
            course_type = {'芝': '芝', '障': '障害'}.get(dm.group(1), 'ダート')
            dist_val = int(dm.group(2))
        for cond in ['不良', '重', '稍重', '良']:
            if cond in rd:
                track_cond = cond
                break
        if dist_val:
            break

    venue_code = race_id[4:6]
    kai        = race_id[6:8]
    nichi      = race_id[8:10]
    race_num   = int(race_id[10:12])
    year_str   = race_id[:4]

    # ── 払戻 ──
    pay = {
        '単勝払戻': '', '複勝払戻': '', '馬連払戻': '',
        '馬単払戻': '', 'ワイド払戻': '', '三連複払戻': '', '三連単払戻': '',
    }
    for t in soup.find_all('table'):
        for row in t.find_all('tr'):
            txt = row.get_text(' ', strip=True)
            if not txt:
                continue
            if txt.startswith('単勝'):
                nums, amounts = _parse_payout_row(txt[2:])
                pay['単勝払戻'] = _fmt_tansho(nums, amounts)
            elif txt.startswith('複勝'):
                nums, amounts = _parse_payout_row(txt[2:])
                pay['複勝払戻'] = _fmt_fukusho(nums, amounts)
            elif txt.startswith('馬連'):
                nums, amounts = _parse_payout_row(txt[2:])
                pay['馬連払戻'] = _fmt_umaren(nums, amounts)
            elif txt.startswith('馬単'):
                nums, amounts = _parse_payout_row(txt[2:])
                pay['馬単払戻'] = _fmt_umatan(nums, amounts)
            elif txt.startswith('ワイド'):
                nums, amounts = _parse_payout_row(txt[3:])
                pay['ワイド払戻'] = _fmt_wide(nums, amounts)
            elif txt.startswith('3連複') or txt.startswith('三連複'):
                label = 3
                nums, amounts = _parse_payout_row(txt[label:])
                pay['三連複払戻'] = _fmt_sanrenpuku(nums, amounts)
            elif txt.startswith('3連単') or txt.startswith('三連単'):
                label = 3
                nums, amounts = _parse_payout_row(txt[label:])
                pay['三連単払戻'] = _fmt_sanrentan(nums, amounts)

    # ── 着順テーブル ──
    result_table = None
    for t in soup.find_all('table'):
        trs = t.find_all('tr')
        if len(trs) < 3:
            continue
        hdr = trs[0].get_text(' ', strip=True) if trs else ''
        if '着' in hdr and '馬名' in hdr:
            result_table = t
            break
    if result_table is None:
        tables = soup.find_all('table')
        if tables:
            result_table = max(tables, key=lambda t: len(t.find_all('tr')))

    rows_out = []
    if result_table:
        for tr in result_table.find_all('tr')[1:]:
            tds  = tr.find_all('td')
            if len(tds) < 6:
                continue
            texts = [td.get_text(' ', strip=True) for td in tds]

            # 着順
            m = re.match(r'^\d+', texts[0]) if texts else None
            if not m:
                continue
            chakujun = m.group()

            waku   = re.search(r'\d+', texts[1]).group() if len(tds) > 1 and re.search(r'\d+', texts[1]) else ''
            umaban = re.search(r'\d+', texts[2]).group() if len(tds) > 2 and re.search(r'\d+', texts[2]) else ''

            horse_name = texts[3] if len(texts) > 3 else ''
            seire      = texts[4] if len(texts) > 4 else ''
            kinryo     = texts[5] if len(texts) > 5 else ''
            jockey     = texts[6] if len(texts) > 6 else ''
            time_str   = texts[7] if len(texts) > 7 else ''
            chakusa    = texts[8] if len(texts) > 8 else ''
            ninki      = texts[9] if len(texts) > 9 else ''
            tan_odds   = texts[10] if len(texts) > 10 else ''
            agari3f    = texts[11] if len(texts) > 11 else ''
            trainer    = ''
            bataijyu   = ''

            # 馬体重を後ろから探す
            for tx in reversed(texts[11:]):
                if re.match(r'\d{3}\([+-]?\d+\)', tx):
                    bataijyu = tx
                    break
            # 調教師 (馬体重の直前)
            for i in range(len(texts) - 1, 10, -1):
                if re.match(r'\d{3}\([+-]?\d+\)', texts[i]):
                    if i > 0:
                        trainer = texts[i - 1]
                    break

            row = {
                'race_id':    race_id,
                'race_name':  race_name,
                'grade':      '',
                '距離':        dist_val,
                'コース':      course_type,
                '馬場状態':    track_cond,
                '着順':        chakujun,
                '枠番':        waku,
                '馬番':        umaban,
                '馬名':        horse_name,
                '性齢':        seire,
                '斤量':        kinryo,
                '騎手':        jockey,
                'タイム':      time_str,
                '着差':        chakusa,
                '通過順':      '',
                '上がり3F':    agari3f,
                '単勝オッズ':  tan_odds,
                '人気':        ninki,
                '馬体重':      bataijyu,
                '調教師':      trainer,
                '調教タイム':  '',
                '年':          year_str,
                '場コード':    venue_code,
                '回次':        kai,
                '日次':        nichi,
                'レース番号':  race_num,
            }
            row.update(pay)
            rows_out.append(row)

    return rows_out


# ── メイン ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',      type=int, required=True)
    parser.add_argument('--month',     type=int, required=True)
    parser.add_argument('--race-min',  type=int, default=1,  dest='race_min')
    parser.add_argument('--race-max',  type=int, default=12, dest='race_max')
    parser.add_argument('--no-skip',   action='store_true')
    args = parser.parse_args()

    year, month = args.year, args.month
    csv_path = os.path.join(DATA_DIR, f'raceresults_nar_{year}{month:02d}.csv')
    os.makedirs(DATA_DIR, exist_ok=True)

    # 既存データ読み込み (再開用)
    existing_ids = set()
    if not args.no_skip and os.path.exists(csv_path):
        with open(csv_path, encoding='utf-8') as f:
            for row in csv.DictReader(f):
                existing_ids.add(row.get('race_id', ''))
        print(f'既存: {len(existing_ids)}レース スキップ')

    _, days = calendar.monthrange(year, month)
    print(f'\n{year}年{month:02d}月 地方競馬 取得開始 ({days}日分)')

    all_rows  = []
    req_count = 0

    for day in range(1, days + 1):
        date_str = f'{year}{month:02d}{day:02d}'
        race_ids = fetch_race_ids_for_date(date_str, args.race_min, args.race_max)
        new_ids  = [r for r in race_ids if r not in existing_ids]
        if not new_ids:
            _sleep()
            continue

        print(f'  {date_str}: {len(new_ids)}レース')
        _sleep()

        for i, race_id in enumerate(new_ids):
            venue_name = NAR_VENUE.get(race_id[4:6], race_id[4:6])
            print(f'    [{race_id}] {venue_name} {race_id[10:12]}R', end=' ... ', flush=True)

            rows = fetch_race_result(race_id)
            if rows:
                all_rows.extend(rows)
                san = rows[0].get('三連複払戻', '')
                print(f'{len(rows)}頭  3連複:{san[:20]}')
            else:
                print('スキップ（データなし）')

            req_count += 1
            if req_count % BURST_INTERVAL == 0:
                wait = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
                print(f'  [クールダウン {wait:.0f}s]')
                time.sleep(wait)
            else:
                _sleep()

    # CSV書き出し (追記 or 新規)
    if all_rows:
        mode = 'a' if existing_ids else 'w'
        with open(csv_path, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
            if mode == 'w':
                writer.writeheader()
            writer.writerows(all_rows)
        print(f'\n✅ {csv_path} に {len(all_rows)}行 書き込み完了')
    else:
        # 空ファイルを作成 (スキップ防止)
        if not os.path.exists(csv_path):
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
        print(f'\n⚠️  新規データなし（{year}年{month}月は地方競馬なし）')


if __name__ == '__main__':
    main()

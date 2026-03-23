"""
fetch_history.py - netkeiba から直接 race_id を取得してスクレイピング
CSVファイル不要。netkeibaの開催カレンダーから race_id を生成する。
"""

import os, re, json, time, random, glob, argparse, subprocess, requests
from bs4 import BeautifulSoup

# ============================================================
# 定数
# ============================================================
VENUE_CODE = {
    '札幌':'01','函館':'02','福島':'03','新潟':'04',
    '東京':'05','中山':'06','中京':'07','京都':'08',
    '阪神':'09','小倉':'10',
}
JRA_VENUES = set(VENUE_CODE.keys())

DATA_DIR   = 'data'
SLEEP_MIN  = 2.0
SLEEP_MAX  = 5.0
SLEEP_LONG_MIN = 10.0
SLEEP_LONG_MAX = 20.0
BURST_INTERVAL = 15
MAX_RETRIES    = 3
SESSION_RESET_INTERVAL = 50

BASE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language':           'ja,en-US;q=0.9,en;q=0.8',
    'Accept-Encoding':           'gzip, deflate, br',
    'Connection':                'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest':            'document',
    'Sec-Fetch-Mode':            'navigate',
    'Sec-Fetch-Site':            'same-origin',
    'Sec-Fetch-User':            '?1',
    'Cache-Control':             'max-age=0',
    'DNT':                       '1',
}

_session = None
_session_count = 0

def _get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_RESET_INTERVAL:
        s = requests.Session()
        s.headers.update(BASE_HEADERS)
        try:
            s.get('https://www.netkeiba.com/', timeout=10)
            time.sleep(random.uniform(1.5, 3.0))
            s.headers.update({'Referer': 'https://race.netkeiba.com/top/'})
        except Exception as e:
            print(f'[session] トップページ取得失敗（続行）: {e}')
        _session = s
        _session_count = 0
        print('[session] セッション初期化完了')
    return _session

def _sleep(long=False):
    if long:
        t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
    else:
        t = random.uniform(SLEEP_MIN, SLEEP_MAX)
    time.sleep(max(1.0, t + random.uniform(-0.3, 0.3)))

# ============================================================
# race_id 生成（カレンダー不要・総当たり方式）
# ============================================================
def generate_race_ids(year: int, month: int) -> list:
    """
    指定年月のJRA全レースのrace_idを総当たりで生成する。
    netkeibaのカレンダーページから開催情報を取得して絞り込む。
    """
    # netkeibaの開催スケジュールページからrace_idを収集
    url = f'https://race.netkeiba.com/top/calendar.html?year={year}&month={month}'
    session = _get_session()
    race_ids = []

    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'EUC-JP'
        soup = BeautifulSoup(resp.text, 'html.parser')
        _sleep()

        # カレンダーページの開催リンクから race_id を抽出
        for a in soup.select('a[href*="race_id="]'):
            href = a.get('href', '')
            m = re.search(r'race_id=(\d{12})', href)
            if m:
                race_id = m.group(1)
                # JRA のみ（場コード 01〜10）
                venue = race_id[4:6]
                if venue in VENUE_CODE.values():
                    race_ids.append(race_id)

        race_ids = list(set(race_ids))
        print(f'[calendar] {year}年{month}月: カレンダーから{len(race_ids)}件のrace_idを取得')

    except Exception as e:
        print(f'[calendar] カレンダー取得失敗: {e}')
        # フォールバック: 総当たりでrace_idを生成
        race_ids = _generate_by_brute_force(year, month)

    return sorted(set(race_ids))


def _generate_by_brute_force(year: int, month: int) -> list:
    """
    カレンダー取得失敗時のフォールバック。
    JRA全10場・最大6回・最大12日・最大12Rの組み合わせを生成。
    （実際に存在するものだけ後でフィルタリング）
    """
    race_ids = []
    for venue_code in VENUE_CODE.values():
        for kai in range(1, 7):       # 最大6回開催
            for nichi in range(1, 13): # 最大12日
                for race_num in range(1, 13): # 最大12R
                    rid = f'{year}{venue_code}{str(kai).zfill(2)}{str(nichi).zfill(2)}{str(race_num).zfill(2)}'
                    race_ids.append(rid)
    print(f'[brute_force] {year}年{month}月: {len(race_ids)}件の候補を生成')
    return race_ids


# ============================================================
# レース結果取得
# ============================================================
def fetch_race_result(race_id: str) -> dict:
    global _session_count
    url     = f'https://race.netkeiba.com/race/result.html?race_id={race_id}'
    session = _get_session()

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=15)

            if resp.status_code == 404:
                return None  # レースが存在しない

            if resp.status_code == 429:
                wait = (BURST_INTERVAL ** (attempt + 2)) * 10
                print(f'[scraper] 429 → {wait:.0f}秒待機')
                time.sleep(wait)
                _session = None
                session = _get_session()
                continue

            if resp.status_code in (403, 503):
                wait = (2 ** (attempt + 1)) * 5
                print(f'[scraper] {resp.status_code} → {wait:.0f}秒待機')
                if resp.status_code == 403:
                    _session = None
                    time.sleep(random.uniform(30, 60))
                    session = _get_session()
                else:
                    time.sleep(wait)
                continue

            resp.raise_for_status()
            resp.encoding = 'EUC-JP'
            soup = BeautifulSoup(resp.text, 'html.parser')
            _session_count += 1

            # レースが存在しない場合（タイトルやテーブルなし）
            table = soup.select_one('.race_table_01') or soup.select_one('#race_result_tbl')
            if not table:
                return None

            # 新馬戦を除外
            race_class_el = soup.select_one('.RaceData02 span') or soup.select_one('.race_class')
            if race_class_el and '新馬' in race_class_el.text:
                return None

            session.headers.update({'Referer': url})
            horses  = _parse_horses(soup)
            payouts = _parse_payouts(soup)

            if not horses:
                return None

            return {'race_id': race_id, 'horses': horses, 'payouts': payouts}

        except requests.exceptions.Timeout:
            print(f'[scraper] タイムアウト ({attempt+1}/{MAX_RETRIES})')
            time.sleep(2 ** attempt * 3)
        except requests.exceptions.RequestException as e:
            print(f'[scraper] エラー: {e} ({attempt+1}/{MAX_RETRIES})')
            time.sleep(2 ** attempt * 2)

    return None


def _parse_horses(soup):
    horses = []
    table  = soup.select_one('.race_table_01') or soup.select_one('#race_result_tbl')
    if not table: return horses

    for row in table.select('tr')[1:]:
        cols = row.select('td')
        if len(cols) < 12: continue
        try:
            finish_rank = int(cols[0].text.strip())
        except ValueError:
            continue

        name = (cols[3].select_one('a') or cols[3]).text.strip()

        agari = None
        for idx in [11, 12]:
            if idx < len(cols):
                try: agari = float(cols[idx].text.strip()); break
                except ValueError: pass

        corner_text = ''
        for idx in [10, 11]:
            if idx < len(cols):
                t = cols[idx].text.strip()
                if re.search(r'\d+-\d+', t):
                    corner_text = t; break

        corner_orders = [int(c) for c in re.split(r'[-\s]', corner_text) if c.isdigit()]
        last_corner   = corner_orders[-1] if corner_orders else None

        horses.append({
            'name': name, 'finish_rank': finish_rank,
            'agari_3f': agari, 'corner_orders': corner_orders,
            'last_corner': last_corner,
        })
    return horses


def _parse_payouts(soup):
    payouts = {}
    payout_types = ('単勝','複勝','枠連','馬連','ワイド','馬単','3連複','3連単')

    for block in (soup.select('.payout_block table') or soup.select('.pay_block')):
        current_type = None
        for row in block.select('tr'):
            for cell in row.select('th,.tan,td.tansyo,td.fukusyo'):
                t = cell.text.strip()
                if t in payout_types:
                    current_type = t
                    if t not in payouts: payouts[t] = []
                    break
            if not current_type: continue
            hc = row.select_one('.num,.horse_num')
            pc = row.select_one('.pay,.yen')
            if hc and pc:
                hs = hc.text.strip().replace('\n','-').replace('\u2192','→')
                pn = re.sub(r'[^\d]', '', pc.text)
                try:
                    payouts[current_type].append({'horses': hs, 'payout': int(pn)})
                except ValueError:
                    pass
    return payouts


# ============================================================
# 月単位取得
# ============================================================
def fetch_month(year: int, month: int, skip_existing: bool = True):
    output_path = os.path.join(DATA_DIR, f'netkeiba_{year}{str(month).zfill(2)}.json')
    os.makedirs(DATA_DIR, exist_ok=True)

    existing = {}
    if skip_existing and os.path.exists(output_path):
        with open(output_path) as f:
            existing = json.load(f)

    race_ids = generate_race_ids(year, month)
    print(f'[fetch_month] {year}年{month}月: {len(race_ids)}件のrace_idを処理')

    new_data = dict(existing)
    success = skip = error = 0

    for i, race_id in enumerate(race_ids, 1):
        if skip_existing and race_id in existing:
            skip += 1
            continue

        # バースト後の休憩
        if i > 1 and (i - 1) % BURST_INTERVAL == 0:
            t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
            print(f'[fetch_month] {BURST_INTERVAL}件完了 → {t:.1f}秒休憩')
            time.sleep(t)

        try:
            result = fetch_race_result(race_id)
            if result:
                new_data[race_id] = result
                success += 1
                print(f'  [{i}/{len(race_ids)}] {race_id} → 取得成功 ({len(result["horses"])}頭)')
            # 存在しないレースは静かにスキップ
        except Exception as e:
            print(f'  [{i}/{len(race_ids)}] {race_id} → エラー: {e}')
            error += 1
            time.sleep(random.uniform(5, 10))

        _sleep()

        # 10件ごとに中間保存
        if success > 0 and success % 10 == 0:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, ensure_ascii=False, indent=2)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(new_data, f, ensure_ascii=False, indent=2)

    print(f'[fetch_month] 完了: 取得={success}, スキップ={skip}, エラー={error}')
    print(f'[fetch_month] 保存: {output_path} ({len(new_data)}件)')
    return new_data


# ============================================================
# CLI
# ============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',  type=int, required=True)
    parser.add_argument('--month', type=int, required=True)
    parser.add_argument('--no-skip', action='store_true')
    args = parser.parse_args()
    fetch_month(args.year, args.month, skip_existing=not args.no_skip)

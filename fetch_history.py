"""
fetch_history.py - netkeiba スクレイパー（race_id直接取得版）

netkeibaの race_list.html ページの DOM から
myhorse_{race_id} 要素を取得してrace_idを収集する。
CSVファイル不要。
"""

import os, re, json, time, random, argparse, requests
from bs4 import BeautifulSoup

# ============================================================
# 定数
# ============================================================
VENUE_CODE = {
    '札幌':'01','函館':'02','福島':'03','新潟':'04',
    '東京':'05','中山':'06','中京':'07','京都':'08',
    '阪神':'09','小倉':'10',
}
JRA_VENUE_CODES = set(VENUE_CODE.values())

DATA_DIR        = 'data'
SLEEP_MIN       = 2.0
SLEEP_MAX       = 4.5
SLEEP_LONG_MIN  = 10.0
SLEEP_LONG_MAX  = 18.0
BURST_INTERVAL  = 15
MAX_RETRIES     = 3
SESSION_RESET_INTERVAL = 50

BASE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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

_session       = None
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
    t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX) if long else random.uniform(SLEEP_MIN, SLEEP_MAX)
    time.sleep(max(1.0, t + random.uniform(-0.3, 0.3)))

# ============================================================
# ステップ1: カレンダーから開催日リストを取得
# ============================================================
def get_kaisai_dates(year: int, month: int) -> list:
    """カレンダーページから開催日付リスト（YYYYMMDD）を取得"""
    url     = f'https://race.netkeiba.com/top/calendar.html?year={year}&month={month}'
    session = _get_session()
    dates   = []
    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'EUC-JP'
        soup = BeautifulSoup(resp.text, 'html.parser')
        session.headers.update({'Referer': url})
        _sleep()

        for a in soup.select('a[href*="kaisai_date="]'):
            href = a.get('href', '')
            m = re.search(r'kaisai_date=(\d{8})', href)
            if m:
                dates.append(m.group(1))

        dates = sorted(set(dates))
        print(f'[calendar] {year}年{month}月: {len(dates)}開催日 → {dates}')
    except Exception as e:
        print(f'[calendar] 取得失敗: {e}')
    return dates

# ============================================================
# ステップ2: race_list.html から race_id を取得
# ============================================================
def get_race_ids_for_date(kaisai_date: str) -> list:
    """
    race_list.html?kaisai_date=YYYYMMDD のDOMから
    myhorse_{race_id} 要素のIDを収集してrace_idリストを返す
    """
    url     = f'https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}'
    session = _get_session()
    race_ids = []
    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'EUC-JP'
        soup = BeautifulSoup(resp.text, 'html.parser')
        session.headers.update({'Referer': url})
        _sleep()

        # myhorse_{race_id} の要素からrace_idを抽出
        for el in soup.select('[id^="myhorse_"]'):
            el_id = el.get('id', '')
            m = re.match(r'myhorse_(\d{12})', el_id)
            if m:
                race_id = m.group(1)
                # JRAのみ（場コード 01〜10）、新馬戦は後でフィルタ
                venue = race_id[4:6]
                if venue in JRA_VENUE_CODES:
                    race_ids.append(race_id)

        race_ids = sorted(set(race_ids))
        print(f'  [{kaisai_date}] {len(race_ids)}レース取得')
    except Exception as e:
        print(f'  [{kaisai_date}] race_id取得失敗: {e}')
    return race_ids

# ============================================================
# ステップ3: レース結果を取得
# ============================================================
def fetch_race_result(race_id: str):
    global _session_count
    url     = f'https://race.netkeiba.com/race/result.html?race_id={race_id}'
    session = _get_session()

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 404: return None
            if resp.status_code == 429:
                wait = 2 ** (attempt + 3) * 5
                print(f'[scraper] 429 → {wait}秒待機')
                time.sleep(wait)
                _session = None; session = _get_session()
                continue
            if resp.status_code == 403:
                print('[scraper] 403 → セッションリセット')
                _session = None
                time.sleep(random.uniform(30, 60))
                session = _get_session()
                continue
            if resp.status_code == 503:
                time.sleep(2 ** (attempt + 1) * 5)
                continue

            resp.raise_for_status()
            resp.encoding = 'EUC-JP'
            soup = BeautifulSoup(resp.text, 'html.parser')
            _session_count += 1

            table = soup.select_one('.race_table_01') or soup.select_one('#race_result_tbl')
            if not table: return None

            # 新馬戦を除外（ページタイトルや race_class から判定）
            page_text = soup.get_text()
            if '新馬' in page_text[:2000]:
                return None

            session.headers.update({'Referer': url})
            horses  = _parse_horses(soup)
            payouts = _parse_payouts(soup)
            if not horses: return None

            return {'race_id': race_id, 'horses': horses, 'payouts': payouts}

        except requests.exceptions.Timeout:
            print(f'[scraper] タイムアウト ({attempt+1}/{MAX_RETRIES})')
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f'[scraper] エラー: {e}')
            time.sleep(2 * (attempt + 1))
    return None

def _parse_horses(soup):
    horses = []
    table  = soup.select_one('.race_table_01') or soup.select_one('#race_result_tbl')
    if not table: return horses
    for row in table.select('tr')[1:]:
        cols = row.select('td')
        if len(cols) < 12: continue
        try: finish_rank = int(cols[0].text.strip())
        except ValueError: continue
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
                if re.search(r'\d+-\d+', t): corner_text = t; break
        corner_orders = [int(c) for c in re.split(r'[-\s]', corner_text) if c.isdigit()]
        horses.append({
            'name': name, 'finish_rank': finish_rank,
            'agari_3f': agari,
            'corner_orders': corner_orders,
            'last_corner': corner_orders[-1] if corner_orders else None,
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
                hs = hc.text.strip().replace('\n', '-').replace('\u2192', '→')
                pn = re.sub(r'[^\d]', '', pc.text)
                try: payouts[current_type].append({'horses': hs, 'payout': int(pn)})
                except ValueError: pass
    return payouts

# ============================================================
# 月単位取得メイン
# ============================================================
def fetch_month(year: int, month: int, skip_existing: bool = True):
    output_path = os.path.join(DATA_DIR, f'netkeiba_{year}{str(month).zfill(2)}.json')
    os.makedirs(DATA_DIR, exist_ok=True)

    existing = {}
    if skip_existing and os.path.exists(output_path):
        try:
            with open(output_path) as f:
                existing = json.load(f)
            if existing:
                print(f'[fetch_month] 取得済み: {len(existing)}件 → スキップ対象あり')
        except Exception:
            pass

    # 開催日リストを取得
    kaisai_dates = get_kaisai_dates(year, month)
    if not kaisai_dates:
        print(f'[fetch_month] {year}年{month}月: 開催日なし')
        return {}

    # 各開催日からrace_idを収集
    all_race_ids = []
    for kd in kaisai_dates:
        ids = get_race_ids_for_date(kd)
        all_race_ids.extend(ids)
        _sleep()

    all_race_ids = sorted(set(all_race_ids))
    print(f'[fetch_month] {year}年{month}月: 合計{len(all_race_ids)}レース')

    # 各レースの結果を取得
    new_data = dict(existing)
    success = skip = error = 0

    for i, race_id in enumerate(all_race_ids, 1):
        if skip_existing and race_id in existing:
            skip += 1
            continue

        if i > 1 and (i - 1) % BURST_INTERVAL == 0:
            t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
            print(f'[fetch_month] {BURST_INTERVAL}件完了 → {t:.1f}秒休憩')
            time.sleep(t)

        try:
            result = fetch_race_result(race_id)
            if result:
                new_data[race_id] = result
                success += 1
                if success <= 3 or success % 20 == 0:
                    print(f'  [{i}/{len(all_race_ids)}] {race_id} ✓ ({len(result["horses"])}頭)')
        except Exception as e:
            print(f'  [{i}/{len(all_race_ids)}] {race_id} エラー: {e}')
            error += 1
            time.sleep(random.uniform(5, 10))

        _sleep()

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
    parser.add_argument('--year',    type=int, required=True)
    parser.add_argument('--month',   type=int, required=True)
    parser.add_argument('--no-skip', action='store_true')
    args = parser.parse_args()
    fetch_month(args.year, args.month, skip_existing=not args.no_skip)

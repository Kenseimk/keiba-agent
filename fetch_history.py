"""
fetch_history.py - netkeiba スクレイパー（db.netkeiba.com 完全版 v5）
"""

import os, re, json, time, random, argparse, requests, calendar
from bs4 import BeautifulSoup

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
            s.get('https://db.netkeiba.com/', timeout=10)
            time.sleep(random.uniform(1.5, 2.5))
            s.headers.update({'Referer': 'https://db.netkeiba.com/'})
        except Exception as e:
            print(f'[session] init失敗（続行）: {e}')
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
            m = re.search(r'kaisai_date=(\d{8})', a.get('href', ''))
            if m:
                dates.append(m.group(1))
        dates = sorted(set(dates))
        print(f'[calendar] {year}年{month}月: {len(dates)}開催日 → {dates}')
    except Exception as e:
        print(f'[calendar] 取得失敗: {e}')
        _, last_day = calendar.monthrange(year, month)
        for d in range(1, last_day + 1):
            dates.append(f'{year}{str(month).zfill(2)}{str(d).zfill(2)}')
    return dates

# ============================================================
# ステップ2: db.netkeiba.com/race/list/ から race_id を取得
# ============================================================
def get_race_ids_for_date(date_str: str) -> list:
    url     = f'https://db.netkeiba.com/race/list/{date_str}/'
    session = _get_session()
    race_ids = []
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        resp.encoding = 'EUC-JP'
        soup = BeautifulSoup(resp.text, 'html.parser')
        session.headers.update({'Referer': url})
        _sleep()
        for a in soup.select('a[href*="/race/"]'):
            m = re.search(r'/race/(\d{12})/?', a.get('href', ''))
            if m:
                race_id    = m.group(1)
                venue_code = race_id[4:6]
                # JRA場コードのみ（01〜10）
                if venue_code in JRA_VENUE_CODES:
                    race_ids.append(race_id)
        race_ids = sorted(set(race_ids))
        print(f'  [{date_str}] {len(race_ids)}レース取得')
    except Exception as e:
        print(f'  [{date_str}] 取得失敗: {e}')
    return race_ids

# ============================================================
# ステップ3: db.netkeiba.com/race/{race_id}/ からレース結果を取得
# ============================================================
def fetch_race_result(race_id: str):
    global _session_count
    url     = f'https://db.netkeiba.com/race/{race_id}/'
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

            table = soup.select_one('.race_table_01')
            if not table: return None

            # 新馬戦を除外
            title = soup.select_one('title')
            if title and '新馬' in title.text:
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
    """
    db.netkeiba.com の race_table_01 をパース
    [0]=着順 [3]=馬名 [14]=通過順 [15]=上がり
    """
    horses = []
    table  = soup.select_one('.race_table_01')
    if not table: return horses

    for row in table.select('tr')[1:]:
        cols = row.select('td')
        if len(cols) < 16: continue
        try:
            finish_rank = int(cols[0].text.strip())
        except ValueError:
            continue

        name  = cols[3].text.strip()
        agari = None
        try:
            agari = float(cols[15].text.strip())
        except (ValueError, IndexError):
            pass

        corner_text   = cols[14].text.strip() if len(cols) > 14 else ''
        corner_orders = [int(c) for c in re.split(r'[-\s]', corner_text) if c.isdigit()]

        horses.append({
            'name':          name,
            'finish_rank':   finish_rank,
            'agari_3f':      agari,
            'corner_orders': corner_orders,
            'last_corner':   corner_orders[-1] if corner_orders else None,
        })
    return horses


def _parse_payouts(soup):
    """
    db.netkeiba.com の pay_table_01 をパース

    払戻テーブルのセル構造:
      td[0] = 馬券種（単勝/複勝/...）
      td[1] = 馬番（複数頭は <br> 区切り）
      td[2] = 払戻金額（複数は <br> 区切り）
      td[3] = 人気

    複勝などは1つのセルに複数の値が <br> で区切られている。
    """
    payouts      = {}
    payout_types = ('単勝','複勝','枠連','馬連','ワイド','馬単','3連複','3連単','三連複','三連単')

    for table in soup.select('.pay_table_01'):
        current_type = None
        for row in table.select('tr'):
            cols = row.select('td, th')
            if not cols: continue

            type_text = cols[0].get_text(strip=True)
            if type_text in payout_types:
                current_type = type_text
                if current_type not in payouts:
                    payouts[current_type] = []

            if not current_type or len(cols) < 3:
                continue

            # <br> で区切られた複数値を個別に取得
            horses_list  = [s.strip() for s in cols[1].get_text(separator='\n').split('\n') if s.strip()]
            payouts_list = [s.strip() for s in cols[2].get_text(separator='\n').split('\n') if s.strip()]

            for h_str, p_str in zip(horses_list, payouts_list):
                p_num = re.sub(r'[^\d]', '', p_str)
                if h_str and p_num:
                    try:
                        payouts[current_type].append({
                            'horses': h_str,
                            'payout': int(p_num),
                        })
                    except ValueError:
                        pass

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
                d = json.load(f)
            if d:
                existing = d
                print(f'[fetch_month] 取得済み: {len(existing)}件')
        except Exception:
            pass

    kaisai_dates = get_kaisai_dates(year, month)
    if not kaisai_dates:
        print(f'[fetch_month] {year}年{month}月: 開催日なし')
        return {}

    all_race_ids = []
    for kd in kaisai_dates:
        ids = get_race_ids_for_date(kd)
        all_race_ids.extend(ids)
        _sleep()

    all_race_ids = sorted(set(all_race_ids))
    print(f'[fetch_month] {year}年{month}月: 合計{len(all_race_ids)}レース')

    if not all_race_ids:
        print(f'[fetch_month] race_idが0件 → スキップ')
        return existing

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
                if success <= 3 or success % 30 == 0:
                    print(f'  [{i}/{len(all_race_ids)}] {race_id} ✓ ({len(result["horses"])}頭) 上がり:{result["horses"][0].get("agari_3f")}')
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',    type=int, required=True)
    parser.add_argument('--month',   type=int, required=True)
    parser.add_argument('--no-skip', action='store_true')
    args = parser.parse_args()
    fetch_month(args.year, args.month, skip_existing=not args.no_skip)

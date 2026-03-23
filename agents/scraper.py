"""
agents/scraper.py - netkeiba スクレイパー（bot検知対策済み）

bot検知対策:
  - 本物のChromeに近い10項目以上のHeaders
  - requests.Session でCookie自動管理・コネクション再利用
  - トップページ → 結果ページの遷移を模倣（Referer設定）
  - ランダムな待機時間（固定間隔を避ける）
  - BURST_INTERVAL件ごとに長めの休憩（10〜20秒）
  - 429/503/403時のバックオフ付きリトライ
  - SESSION_RESET_INTERVAL件ごとにセッションをリセット
"""

import re, time, random, requests
from bs4 import BeautifulSoup

# ============================================================
# 定数
# ============================================================
BASE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
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

BASE_URL          = 'https://race.netkeiba.com/race/result.html'
NETKEIBA_TOP      = 'https://www.netkeiba.com/'
RACE_TOP          = 'https://race.netkeiba.com/top/'

SLEEP_MIN         = 2.0    # 通常待機の最小秒
SLEEP_MAX         = 5.0    # 通常待機の最大秒
SLEEP_LONG_MIN    = 10.0   # バースト後の長め休憩（最小）
SLEEP_LONG_MAX    = 20.0   # バースト後の長め休憩（最大）
BURST_INTERVAL    = 15     # N件ごとに長め休憩
MAX_RETRIES       = 3
RETRY_BACKOFF     = 2.0
SESSION_RESET_INTERVAL = 50

VENUE_CODE = {
    '札幌':'01','函館':'02','福島':'03','新潟':'04',
    '東京':'05','中山':'06','中京':'07','京都':'08',
    '阪神':'09','小倉':'10',
}

# ============================================================
# Session管理
# ============================================================
_session = None
_session_count = 0

def _create_session():
    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    try:
        resp = s.get(NETKEIBA_TOP, timeout=10)
        resp.raise_for_status()
        _random_sleep()
        s.headers.update({'Referer': RACE_TOP})
        print('[scraper] セッション初期化完了（Cookie取得済み）')
    except Exception as e:
        print(f'[scraper] トップページ取得失敗（続行）: {e}')
    return s

def _get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_RESET_INTERVAL:
        _session = _create_session()
        _session_count = 0
    return _session

def _random_sleep(long=False):
    if long:
        t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
    else:
        t = random.uniform(SLEEP_MIN, SLEEP_MAX)
    t += random.uniform(-0.3, 0.3)
    time.sleep(max(1.0, t))

# ============================================================
# race_id 生成
# ============================================================
def build_race_id(date_str, session_str, race_num_str):
    """CSVフィールドから netkeiba の race_id を生成する"""
    year_m = re.match(r'(\d{4})年', date_str)
    if not year_m: return None
    year = year_m.group(1)

    venue_code = next((c for v, c in VENUE_CODE.items() if v in session_str), None)
    if not venue_code: return None

    kai_m = re.search(r'^(\d+)回', session_str)
    kai   = kai_m.group(1).zfill(2) if kai_m else '01'

    nichi_m = re.search(r'(\d+)日目', session_str)
    nichi   = nichi_m.group(1).zfill(2) if nichi_m else '01'

    race_m  = re.search(r'(\d+)', race_num_str)
    race_n  = race_m.group(1).zfill(2) if race_m else '01'

    return f'{year}{venue_code}{kai}{nichi}{race_n}'

# ============================================================
# メイン取得関数
# ============================================================
def fetch_race_result(race_id):
    """
    netkeibaからレース結果を取得する。

    Returns: {
        'race_id': str,
        'horses': [
            {'name', 'finish_rank', 'agari_3f', 'corner_orders', 'last_corner'}
        ],
        'payouts': {
            '単勝':  [{'horses':'1', 'payout':1200}],
            '複勝':  [{'horses':'1', 'payout':230}, ...],
            '馬連':  [...], 'ワイド': [...],
            '馬単':  [...], '3連複': [...], '3連単': [...]
        }
    }
    """
    global _session_count
    url     = f'{BASE_URL}?race_id={race_id}'
    session = _get_session()

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=15)

            # bot検知レスポンス別対応
            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** (attempt + 2) * 10
                print(f'[scraper] 429 Too Many Requests → {wait:.0f}秒待機')
                time.sleep(wait)
                _session = None
                session = _get_session()
                continue

            if resp.status_code == 503:
                wait = RETRY_BACKOFF ** (attempt + 1) * 5
                print(f'[scraper] 503 → {wait:.0f}秒待機')
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                print('[scraper] 403 Forbidden: bot検知の可能性 → セッションリセット＋長め待機')
                _session = None
                time.sleep(random.uniform(30, 60))
                session = _get_session()
                continue

            resp.raise_for_status()
            resp.encoding = 'EUC-JP'
            soup = BeautifulSoup(resp.text, 'html.parser')
            _session_count += 1

            # 次のリクエスト用にRefererを更新
            session.headers.update({'Referer': url})

            return {
                'race_id': race_id,
                'horses':  _parse_horses(soup),
                'payouts': _parse_payouts(soup),
            }

        except requests.exceptions.Timeout:
            print(f'[scraper] タイムアウト ({attempt+1}/{MAX_RETRIES})')
            time.sleep(RETRY_BACKOFF ** attempt * 3)
        except requests.exceptions.RequestException as e:
            print(f'[scraper] エラー: {e} ({attempt+1}/{MAX_RETRIES})')
            time.sleep(RETRY_BACKOFF ** attempt * 2)

    raise RuntimeError(f'[scraper] {race_id} の取得に{MAX_RETRIES}回失敗')

# ============================================================
# HTMLパース
# ============================================================
def _parse_horses(soup):
    horses = []
    table = soup.select_one('.race_table_01') or soup.select_one('#race_result_tbl')
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
# バッチ取得
# ============================================================
def fetch_multiple(race_ids):
    """複数レースを bot検知対策込みで一括取得"""
    results = []
    total   = len(race_ids)

    for i, race_id in enumerate(race_ids, 1):
        print(f'[scraper] {i}/{total} {race_id}')

        # バースト後の長め休憩
        if i > 1 and (i - 1) % BURST_INTERVAL == 0:
            t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
            print(f'[scraper] {BURST_INTERVAL}件完了 → {t:.1f}秒休憩')
            time.sleep(t)

        try:
            result = fetch_race_result(race_id)
            results.append(result)
            print(f'  → 馬:{len(result["horses"])}頭 払戻:{len(result["payouts"])}種')
        except Exception as e:
            print(f'  → 失敗: {e}')

        if i < total:
            _random_sleep()

    return results

# ============================================================
# スコア計算ヘルパー
# ============================================================
def calc_agari_pt(agari_3f, race_fastest):
    if agari_3f is None or race_fastest is None: return 0.0
    return round(max(0.0, 10.0 - (agari_3f - race_fastest) * 10.0), 1)

def calc_last_corner_pt(last_corner, field_size):
    if last_corner is None or field_size is None: return 0.0
    third = field_size / 3
    if last_corner <= 2:           return 5.0
    if last_corner <= 4:           return 4.0
    if last_corner <= third:       return 3.0
    if last_corner <= third * 2:   return 2.0
    return 1.0

# ============================================================
# CLI
# ============================================================
if __name__ == '__main__':
    import json, sys
    test_id = sys.argv[1] if len(sys.argv) >= 2 else build_race_id('2026年03月21日', '1回阪神8日目', '8 R')
    print(f'テスト: {test_id}')
    print(json.dumps(fetch_race_result(test_id), ensure_ascii=False, indent=2))

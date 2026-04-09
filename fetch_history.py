"""
fetch_history.py - netkeiba スクレイパー CSV出力版

race_list_sub.html から race_id を正確に生成し、
db.netkeiba.com/race/{race_id}/ から結果を取得して
CSVファイルとして保存する。

CSVフォーマット:
  race_id, 着順, 枠番, 馬番, 馬名, 性齢, 斤量, 騎手,
  タイム, 着差, 通過順, 上がり3F, 単勝オッズ, 人気, 馬体重,
  年, 場コード, 回次, 日次, レース番号

使い方:
  python fetch_history.py --year 2024 --month 1
  python fetch_history.py --year 2024 --month 1 --no-skip
"""

import os, re, csv, json, time, random, argparse, requests, calendar
from bs4 import BeautifulSoup

VENUE_CODE = {
    '札幌':'01','函館':'02','福島':'03','新潟':'04',
    '東京':'05','中山':'06','中京':'07','京都':'08',
    '阪神':'09','小倉':'10',
}

DATA_DIR        = 'data'
SLEEP_MIN       = 2.0
SLEEP_MAX       = 4.5
SLEEP_LONG_MIN  = 10.0
SLEEP_LONG_MAX  = 18.0
BURST_INTERVAL  = 15
MAX_RETRIES     = 3
SESSION_RESET_INTERVAL = 50

CSV_COLUMNS = [
    'race_id','race_name','grade','距離','コース','馬場状態',
    '着順','枠番','馬番','馬名','性齢','斤量','騎手',
    'タイム','着差','通過順','上がり3F','単勝オッズ','人気','馬体重',
    '調教師','調教タイム',
    '年','場コード','回次','日次','レース番号',
    '単勝払戻','複勝払戻','馬連払戻','馬単払戻','ワイド払戻','三連複払戻','三連単払戻'
]

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
            s.get('https://race.netkeiba.com/', timeout=10)
            time.sleep(random.uniform(1.5, 2.5))
            s.headers.update({'Referer': 'https://race.netkeiba.com/'})
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
# ステップ2: race_list_sub.html から race_id を正確に生成
# ============================================================
def get_race_ids_for_date(date_str: str) -> list:
    url     = f'https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={date_str}'
    session = _get_session()
    race_ids = []

    try:
        resp = session.get(url, timeout=15)
        resp.encoding = 'UTF-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        session.headers.update({'Referer': url})
        _sleep()

        year      = date_str[:4]
        full_text = soup.get_text()

        # 「N回 場所 N日目」でブロック分割
        blocks = re.split(r'(?=\d+回\s*\S+\s*\d+日目)', full_text)

        total_kaisai = 0
        for block in blocks:
            if not block.strip():
                continue
            header = re.match(r'(\d+)回\s*(\S+)\s*(\d+)日目', block.strip())
            if not header:
                continue

            kai_str   = header.group(1)
            venue_str = header.group(2)
            nichi_str = header.group(3)

            venue_code = next((c for v, c in VENUE_CODE.items() if v in venue_str), None)
            if not venue_code:
                continue  # 地方競馬はスキップ

            kai   = kai_str.zfill(2)
            nichi = nichi_str.zfill(2)

            race_nums = re.findall(r'(\d+)R', block)
            max_race  = max(int(r) for r in race_nums) if race_nums else 12

            for race_n in range(1, max_race + 1):
                race_ids.append(f'{year}{venue_code}{kai}{nichi}{str(race_n).zfill(2)}')

            total_kaisai += 1

        race_ids = sorted(set(race_ids))
        print(f'  [{date_str}] {total_kaisai}開催 / {len(race_ids)}race_id生成')

    except Exception as e:
        print(f'  [{date_str}] 取得失敗: {e}')

    return race_ids

# ============================================================
# ステップ3: db.netkeiba.com からレース結果をCSV行として取得
# ============================================================
def fetch_race_rows(race_id: str) -> list:
    """
    レース結果を辞書のリスト（1馬=1行）として返す。
    新馬戦・存在しないrace_idはNoneを返す。
    """
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

            # タイトルからレース名・グレード取得
            title = soup.select_one('title')
            title_text = title.get_text() if title else ''

            # 新馬戦を除外
            if '新馬' in title_text:
                return None

            # レース名抽出: "有馬記念(G1) 2024年..." → "有馬記念"
            race_name = ''
            grade     = ''
            # まずCSSセレクタで試みる
            for sel in ['.RaceName', '.race_name', 'h1.RaceMainTitle', '.RaceMainTitle']:
                el = soup.select_one(sel)
                if el:
                    race_name = el.get_text(strip=True)
                    break
            # CSSで取れなければタイトルから抽出
            if not race_name:
                m = re.match(r'^([^\d｜|（(]+?)(?:\s*[\(（]G[123][\)）])?\s*(?:\d|｜|\||結果|出走)', title_text.strip())
                if m:
                    race_name = m.group(1).strip()
            # グレード抽出
            gm = re.search(r'[\(（](G[123])[\)）]', title_text)
            if gm:
                grade = gm.group(1)

            # 距離・コース・馬場状態の抽出
            # "diary_snap_cut" に "ダ左1400m / 天候 : 晴 / ダート : 良" などが入る
            dist_text = ''
            course    = ''
            track_cond = ''
            snap = soup.select_one('.mainrace_data') or soup.select_one('.diary_snap_cut')
            if snap:
                snap_str = snap.get_text(' ', strip=True)
                # 距離: "芝右外2000m" "ダ左1400m" "障2900m" など
                dm = re.search(r'(芝|ダ|障).*?(\d{3,4})m', snap_str)
                if dm:
                    surface_char = dm.group(1)
                    dist_text = dm.group(2)  # "1400"
                    if surface_char == '芝':
                        course = '芝'
                    elif surface_char == 'ダ':
                        course = 'ダート'
                    else:
                        course = '障害'
                # 馬場状態: "芝 : 良" "ダート : 稍重" など
                tc = re.search(r'(?:芝|ダート)\s*:\s*(良|稍重|重|不良)', snap_str)
                if tc:
                    track_cond = tc.group(1)

            session.headers.update({'Referer': url})

            # 払戻情報を取得
            payouts = _parse_payouts(soup)

            # race_id の内訳
            year     = race_id[:4]
            venue_c  = race_id[4:6]
            kai      = race_id[6:8]
            nichi    = race_id[8:10]
            race_n   = race_id[10:12]

            rows = []
            for row in table.select('tr')[1:]:
                cols = row.select('td')
                if len(cols) < 16: continue
                try:
                    finish_rank = int(cols[0].text.strip())
                except ValueError:
                    continue  # 中止・除外などはスキップ

                def get(i, default=''):
                    try: return cols[i].text.strip()
                    except: return default

                rows.append({
                    'race_id':    race_id,
                    'race_name':  race_name,
                    'grade':      grade,
                    '距離':       dist_text,
                    'コース':     course,
                    '馬場状態':   track_cond,
                    '着順':       finish_rank,
                    '枠番':       get(1),
                    '馬番':       get(2),
                    '馬名':       get(3),
                    '性齢':       get(4),
                    '斤量':       get(5),
                    '騎手':       get(6),
                    'タイム':     get(7),
                    '着差':       get(8),
                    '通過順':     get(14),
                    '上がり3F':   get(15),
                    '単勝オッズ': get(16),
                    '人気':       get(17),
                    '馬体重':     get(18),
                    '調教師':     re.sub(r'^\[[東西]\]', '', get(22)).strip(),
                    '調教タイム': get(19),
                    '年':         year,
                    '場コード':   venue_c,
                    '回次':       kai,
                    '日次':       nichi,
                    'レース番号': race_n,
                    # 払戻（全馬共通・1着の場合の払戻を記録）
                    '単勝払戻':   _payout_str(payouts, '単勝'),
                    '複勝払戻':   _payout_str(payouts, '複勝'),
                    '馬連払戻':   _payout_str(payouts, '馬連'),
                    '馬単払戻':   _payout_str(payouts, '馬単'),
                    'ワイド払戻': _payout_str(payouts, 'ワイド'),
                    '三連複払戻': _payout_str(payouts, '三連複'),
                    '三連単払戻': _payout_str(payouts, '三連単'),
                })

            return rows if rows else None

        except requests.exceptions.Timeout:
            print(f'[scraper] タイムアウト ({attempt+1}/{MAX_RETRIES})')
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f'[scraper] エラー: {e}')
            time.sleep(2 * (attempt + 1))
    return None


def _payout_str(payouts: dict, ptype: str) -> str:
    """払戻を '馬番:金額|馬番:金額' 形式で返す"""
    entries = payouts.get(ptype, [])
    if not entries: return ''
    return '|'.join(f'{e["horses"]}:{e["payout"]}' for e in entries)


def _parse_payouts(soup) -> dict:
    payouts      = {}
    payout_types = ('単勝','複勝','枠連','馬連','ワイド','馬単','三連複','三連単','3連複','3連単')
    for table in soup.select('.pay_table_01'):
        current_type = None
        for row in table.select('tr'):
            cols = row.select('td, th')
            if not cols: continue
            type_text = cols[0].get_text(strip=True)
            if type_text in payout_types:
                canonical = type_text.replace('3連複','三連複').replace('3連単','三連単')
                current_type = canonical
                if current_type not in payouts:
                    payouts[current_type] = []
            if not current_type or len(cols) < 3:
                continue
            horses_list  = [s.strip() for s in cols[1].get_text(separator='\n').split('\n') if s.strip()]
            payouts_list = [s.strip() for s in cols[2].get_text(separator='\n').split('\n') if s.strip()]
            for h_str, p_str in zip(horses_list, payouts_list):
                p_num = re.sub(r'[^\d]', '', p_str)
                if h_str and p_num:
                    try:
                        payouts[current_type].append({'horses': h_str, 'payout': int(p_num)})
                    except ValueError:
                        pass
    return payouts

# ============================================================
# 月単位取得メイン
# ============================================================
def fetch_month(year: int, month: int, skip_existing: bool = True):
    ym          = f'{year}{str(month).zfill(2)}'
    output_path = os.path.join(DATA_DIR, f'raceresults_{ym}.csv')
    os.makedirs(DATA_DIR, exist_ok=True)

    # --no-skip 時は既存ファイルを削除して全件再取得
    if not skip_existing and os.path.exists(output_path):
        os.remove(output_path)
        print(f'[fetch_month] 既存ファイル削除: {output_path}')

    # 取得済みrace_idを確認
    existing_ids = set()
    if skip_existing and os.path.exists(output_path):
        try:
            with open(output_path, encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_ids.add(row.get('race_id',''))
            print(f'[fetch_month] 取得済み: {len(existing_ids)}件')
        except Exception:
            pass

    # ステップ1: 開催日リスト
    kaisai_dates = get_kaisai_dates(year, month)
    if not kaisai_dates:
        print(f'[fetch_month] {year}年{month}月: 開催日なし')
        return

    # ステップ2: race_idリスト
    all_race_ids = []
    for kd in kaisai_dates:
        ids = get_race_ids_for_date(kd)
        all_race_ids.extend(ids)
        _sleep()

    all_race_ids = sorted(set(all_race_ids))
    print(f'[fetch_month] {year}年{month}月: 合計{len(all_race_ids)}race_id候補')

    if not all_race_ids:
        print('[fetch_month] race_idが0件 → スキップ')
        return

    # ステップ3: CSVに追記
    write_header = not os.path.exists(output_path) or len(existing_ids) == 0
    success = skip = not_found = error = 0

    with open(output_path, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()

        for i, race_id in enumerate(all_race_ids, 1):
            if skip_existing and race_id in existing_ids:
                skip += 1
                continue

            if i > 1 and (i - 1) % BURST_INTERVAL == 0:
                t = random.uniform(SLEEP_LONG_MIN, SLEEP_LONG_MAX)
                print(f'[fetch_month] {BURST_INTERVAL}件完了 → {t:.1f}秒休憩')
                time.sleep(t)

            try:
                rows = fetch_race_rows(race_id)
                if rows:
                    writer.writerows(rows)
                    f.flush()
                    success += 1
                    if success <= 3 or success % 30 == 0:
                        print(f'  [{i}/{len(all_race_ids)}] {race_id} ✓ ({len(rows)}頭)')
                else:
                    not_found += 1
            except Exception as e:
                print(f'  [{i}/{len(all_race_ids)}] {race_id} エラー: {e}')
                error += 1
                time.sleep(random.uniform(5, 10))

            _sleep()

    print(f'[fetch_month] 完了: 取得={success}, スキップ={skip}, 存在なし={not_found}, エラー={error}')
    print(f'[fetch_month] 保存: {output_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',    type=int, required=True)
    parser.add_argument('--month',   type=int, required=True)
    parser.add_argument('--no-skip', action='store_true')
    args = parser.parse_args()
    fetch_month(args.year, args.month, skip_existing=not args.no_skip)

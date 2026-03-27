"""
scrape_entries.py - 当日/翌日のJRA出馬表をスクレイピングして predict_input.csv を生成

使い方:
  python scrape_entries.py              # 本日の出馬表
  python scrape_entries.py --date 20260329  # 指定日
  python scrape_entries.py --next-weekend   # 次の土日
"""
import sys, io, os, re, time, random, csv, argparse, datetime, requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://race.netkeiba.com/',
}

JRA_VENUE_CODES = {'01','02','03','04','05','06','07','08','09','10'}

def sleep(lo=1.5, hi=3.0):
    time.sleep(random.uniform(lo, hi))

def get(session, url, **kw):
    for attempt in range(3):
        try:
            r = session.get(url, timeout=20, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f'  [WARN] {url} attempt={attempt+1} err={e}')
            time.sleep(5 * (attempt + 1))
    return None

# ── 開催日一覧取得 ─────────────────────────────────────────
def fetch_kaisai_dates(session, year, month):
    """指定年月の開催日リストを取得"""
    url = f'https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={year}{month:02d}01'
    r = get(session, url)
    if not r: return []
    soup = BeautifulSoup(r.content, 'html.parser')
    dates = set()
    for a in soup.find_all('a', href=True):
        m = re.search(r'kaisai_date=(\d{8})', a['href'])
        if m and m.group(1)[:6] == f'{year}{month:02d}':
            dates.add(m.group(1))
    return sorted(dates)

# ── レースID一覧取得 ──────────────────────────────────────
def fetch_race_ids(session, date_str):
    """指定日のJRAレースID一覧を取得"""
    url = f'https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}'
    r = get(session, url)
    if not r: return []
    soup = BeautifulSoup(r.content, 'html.parser')
    race_ids = []
    seen = set()
    for a in soup.find_all('a', href=True):
        m = re.search(r'race_id=(\d{12})', a['href'])
        if m:
            rid = m.group(1)
            venue_code = rid[4:6]
            if venue_code in JRA_VENUE_CODES and rid not in seen:
                seen.add(rid)
                race_ids.append(rid)
    return sorted(race_ids)

# ── 出馬表取得 ────────────────────────────────────────────
def fetch_shutuba(session, race_id):
    """出馬表（馬名・騎手・馬番）を取得"""
    url = f'https://race.netkeiba.com/race/shutuba.html?race_id={race_id}'
    r = get(session, url)
    if not r: return [], ''
    soup = BeautifulSoup(r.content, 'html.parser')

    # レース名
    race_name = ''
    for sel in ['.RaceName', 'h2.RaceName', '[class*="RaceName"]']:
        el = soup.select_one(sel)
        if el:
            race_name = el.get_text(strip=True)
            break
    if not race_name:
        race_name = f'{race_id[10:12]}R'

    horses = []
    # メインテーブルから馬情報取得
    table = soup.find('table', class_=re.compile(r'Shutuba|ShutubaTable'))
    if table is None:
        table = soup.find('table')

    if table:
        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) < 5: continue
            # 馬番取得
            umaban = ''
            for td in tds[:3]:
                t = td.get_text(strip=True)
                if re.match(r'^\d{1,2}$', t):
                    umaban = t
                    break
            # 馬名取得
            name_a = row.find('a', href=re.compile(r'/horse/'))
            if not name_a: continue
            name = name_a.get_text(strip=True)
            # 騎手取得
            jockey = ''
            jockey_a = row.find('a', href=re.compile(r'/jockey/'))
            if jockey_a:
                jockey = jockey_a.get_text(strip=True)
            # 馬体重（当日公表）
            weight = ''
            for td in reversed(tds):
                t = td.get_text(strip=True)
                if re.match(r'\d{3}\([+-]?\d+\)', t):
                    weight = t
                    break
            if name:
                horses.append({
                    'umaban': umaban,
                    'name': name,
                    'jockey': jockey,
                    'weight': weight,
                })
    return horses, race_name

# ── オッズ取得 ────────────────────────────────────────────
def fetch_odds(session, race_id):
    """単勝オッズ・人気を取得 → {馬名: (odds, pop)}"""
    url = f'https://race.netkeiba.com/odds/index.html?race_id={race_id}&type=b1'
    r = get(session, url)
    if not r: return {}
    soup = BeautifulSoup(r.content, 'html.parser')

    odds_map = {}
    # 人気順テーブルからパース
    for row in soup.find_all('tr'):
        tds = row.find_all('td')
        if len(tds) < 3: continue
        # 馬名リンク
        name_a = row.find('a', href=re.compile(r'/horse/|shutuba'))
        if not name_a: continue
        name = name_a.get_text(strip=True)
        # オッズ（数値XX.X形式）
        odds_val = None
        pop_val  = None
        for i, td in enumerate(tds):
            t = td.get_text(strip=True)
            if re.match(r'^\d+\.\d$', t) and odds_val is None:
                try: odds_val = float(t)
                except: pass
            if re.match(r'^\d{1,2}$', t) and pop_val is None and i == 0:
                try: pop_val = int(t)
                except: pass
        if name and odds_val:
            odds_map[name] = (odds_val, pop_val or 99)

    # フォールバック: 人気順に並んでいる場合
    if not odds_map:
        pop = 1
        for row in soup.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) < 2: continue
            name_a = row.find('a', href=re.compile(r'/horse/'))
            if not name_a: continue
            name = name_a.get_text(strip=True)
            for td in tds:
                t = td.get_text(strip=True)
                if re.match(r'^\d+\.\d$', t):
                    try:
                        odds_map[name] = (float(t), pop)
                        pop += 1
                    except: pass
                    break

    return odds_map

# ── メイン ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default='', help='取得日 YYYYMMDD (省略=今日)')
    parser.add_argument('--next-weekend', action='store_true', help='次の土日を取得')
    parser.add_argument('--output', default='predict_input.csv', help='出力CSVパス')
    args = parser.parse_args()

    today = datetime.date.today()

    if args.next_weekend:
        # 次の土曜または日曜を探す
        d = today
        while d.weekday() not in (5, 6):  # 5=土, 6=日
            d += datetime.timedelta(days=1)
        target_dates = [d.strftime('%Y%m%d')]
        if d.weekday() == 5:  # 土曜なら日曜も
            target_dates.append((d + datetime.timedelta(days=1)).strftime('%Y%m%d'))
    elif args.date:
        target_dates = [args.date]
    else:
        target_dates = [today.strftime('%Y%m%d')]

    print(f'取得対象日: {target_dates}')

    session = requests.Session()
    session.headers.update(HEADERS)
    # Cookieセット用に一度アクセス
    session.get('https://race.netkeiba.com/', timeout=10)
    sleep(2, 3)

    all_rows = []
    race_meta = {}  # race_id -> race_name

    for date_str in target_dates:
        print(f'\n=== {date_str} のレース取得中 ===')
        race_ids = fetch_race_ids(session, date_str)
        print(f'  {len(race_ids)}レース発見: {race_ids[:5]}...')

        for race_id in race_ids:
            print(f'  [{race_id}] 出馬表取得中...', end=' ')
            horses, race_name = fetch_shutuba(session, race_id)
            sleep()
            if not horses:
                print('スキップ（馬なし）')
                continue
            print(f'{len(horses)}頭', end=' ')

            odds_map = fetch_odds(session, race_id)
            sleep()
            print(f'オッズ{len(odds_map)}件')

            race_label = f'{date_str}_{race_id[10:12]}R'
            if race_name:
                race_label = f'{date_str}_{race_id[10:12]}R_{race_name}'
            race_meta[race_id] = race_label

            for h in horses:
                name = h['name']
                odds_info = odds_map.get(name, (None, None))
                all_rows.append({
                    'race_id':    race_label,
                    '馬名':        name,
                    '単勝オッズ':  odds_info[0] or '',
                    '人気':        odds_info[1] or '',
                    '騎手':        h['jockey'],
                    '馬体重':      h['weight'],
                    '馬番':        h['umaban'],
                })

    if not all_rows:
        print('\n対象レースなし（本日はJRA開催なし）')
        # 空ファイルを出力して workflow が続けられるようにする
        with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['race_id','馬名','単勝オッズ','人気','騎手','馬体重','馬番'])
            w.writeheader()
        sys.exit(0)

    # CSV書き出し
    with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['race_id','馬名','単勝オッズ','人気','騎手','馬体重','馬番'])
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f'\n✅ {args.output} に {len(all_rows)}行出力 ({len(race_meta)}レース)')

if __name__ == '__main__':
    main()

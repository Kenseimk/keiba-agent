"""
scrape_nar_results.py - 地方競馬(NAR) 2026年レース結果収集
出力: data/raceresults_nar_202601.csv ... (既存JRA形式に合わせる)

使い方:
  python scrape_nar_results.py                  # 2026年1月〜当月
  python scrape_nar_results.py --months 202601  # 指定月のみ
"""
import sys, re, time, random, argparse, datetime, csv, os
import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8')

BASE = 'https://nar.netkeiba.com'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Referer': BASE + '/',
}
ENC = 'euc-jp'

NAR_VENUE_CODES = {
    '30':'門別','31':'帯広','32':'盛岡','33':'水沢',
    '34':'浦和','35':'船橋','36':'大井','37':'川崎',
    '38':'金沢','39':'笠松','40':'名古屋','41':'園田',
    '42':'姫路','43':'高知','44':'佐賀',
}

CSV_COLS = [
    'race_id','race_name','grade','距離','コース','馬場状態',
    '着順','枠番','馬番','馬名','性齢','斤量','騎手','タイム','着差',
    '通過順','上がり3F','単勝オッズ','人気','馬体重','調教師',
    '年','場コード','回次','日次','レース番号',
    '単勝払戻','複勝払戻','馬連払戻','馬単払戻','ワイド払戻','三連複払戻','三連単払戻',
]


def sleep(lo=1.5, hi=3.0):
    time.sleep(random.uniform(lo, hi))


def get(session, url):
    for attempt in range(3):
        try:
            r = session.get(url, timeout=20, headers=HEADERS)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f'  [WARN] {url[:60]} attempt={attempt+1} err={e}')
            time.sleep(5 * (attempt + 1))
    return None


def decode(r):
    return r.content.decode(ENC, errors='replace')


def fetch_dates_in_month(session, year, month):
    """指定年月の全日付を生成 (地方競馬は毎日開催のため全日チェック)"""
    import calendar
    _, days_in_month = calendar.monthrange(year, month)
    today = datetime.date.today()
    dates = []
    for d in range(1, days_in_month + 1):
        dt = datetime.date(year, month, d)
        if dt > today: break
        dates.append(dt.strftime('%Y%m%d'))
    return dates


def fetch_race_ids(session, date_str):
    """指定日の全地方競馬レースID"""
    url = f'{BASE}/top/race_list_sub.html?kaisai_date={date_str}'
    r = get(session, url)
    if not r: return []
    text = decode(r)
    race_ids = []
    seen = set()
    for rid in re.findall(r'race_id=(\d{12})', text):
        venue = rid[4:6]
        if venue in NAR_VENUE_CODES and rid not in seen:
            seen.add(rid)
            race_ids.append(rid)
    return sorted(race_ids)


def parse_payout_row(row_text):
    """'3連複 11 12 14 3,280円 8人気' → ('3連複', '11-12-14', 3280)"""
    # 金額抽出
    amounts = [int(m.replace(',', '')) for m in re.findall(r'[\d,]+(?=円)', row_text)]
    # 馬番抽出 (人気の前まで)
    nums = re.findall(r'\b(\d{1,2})\b', row_text.split('円')[0])
    # 券種
    bet = row_text.split()[0] if row_text else ''
    return bet, nums, amounts


def fmt_umaren(nums, amounts):
    """馬連: ['12','14'] → '12-14:3610'"""
    if len(nums) >= 2 and amounts:
        return f'{nums[0]}-{nums[1]}:{amounts[0]}'
    return ''


def fmt_wide(nums, amounts):
    """ワイド: 3組分"""
    parts = []
    for i in range(min(3, len(amounts))):
        if i*2+1 < len(nums):
            parts.append(f'{nums[i*2]}-{nums[i*2+1]}:{amounts[i]}')
    return '|'.join(parts)


def fmt_umatan(nums, amounts):
    if len(nums) >= 2 and amounts:
        return f'{nums[0]}-{nums[1]}:{amounts[0]}'
    return ''


def fmt_sanrenpuku(nums, amounts):
    if len(nums) >= 3 and amounts:
        return f'{nums[0]}-{nums[1]}-{nums[2]}:{amounts[0]}'
    return ''


def fmt_sanrentan(nums, amounts):
    if len(nums) >= 3 and amounts:
        return f'{nums[0]} → {nums[1]} → {nums[2]}:{amounts[0]}'
    return ''


def fmt_tansho(nums, amounts):
    if nums and amounts:
        return f'{nums[0]}:{amounts[0]}'
    return ''


def fmt_fukusho(nums, amounts):
    parts = []
    for i, n in enumerate(nums[:3]):
        if i < len(amounts):
            parts.append(f'{n}:{amounts[i]}')
    return '|'.join(parts)


def fetch_race_result(session, race_id):
    """レース結果を取得、(rows, meta) を返す"""
    url = f'{BASE}/race/result.html?race_id={race_id}'
    r = get(session, url)
    if not r: return [], {}

    text = decode(r)
    soup = BeautifulSoup(text, 'html.parser')

    # ── レース情報 ──
    race_name = ''
    for sel in ['.RaceName', '.RaceMainColumn h1', 'h1']:
        el = soup.select_one(sel)
        if el:
            race_name = el.get_text(strip=True)
            break
    if not race_name:
        title_el = soup.find('title')
        if title_el:
            race_name = title_el.get_text(strip=True).split('の結果')[0].strip()

    # コース/距離/馬場状態
    course_type = 'ダート'
    dist_val    = 0
    track_cond  = ''
    race_data_el = soup.find(class_=re.compile(r'RaceData'))
    if race_data_el:
        rd_txt = race_data_el.get_text(' ', strip=True)
        # 距離
        dm = re.search(r'([芝ダ障])(\d{3,4})m', rd_txt)
        if dm:
            course_type = '芝' if dm.group(1) == '芝' else ('障害' if dm.group(1) == '障' else 'ダート')
            dist_val = int(dm.group(2))
        # 馬場状態
        tm = re.search(r'馬場[:\s:]*([良稍重不])', rd_txt)
        if tm: track_cond = tm.group(1)
        # 別パターン
        if not track_cond:
            for cond in ['良', '稍重', '重', '不良']:
                if cond in rd_txt:
                    track_cond = cond
                    break

    # 場コード・回次・日次・レース番号
    venue_code = race_id[4:6]
    kai        = race_id[6:8]
    nichi      = race_id[8:10]
    race_num   = int(race_id[10:12])
    year       = race_id[:4]

    # ── 払戻 ──
    pay_map = {
        '単勝払戻': '', '複勝払戻': '', '馬連払戻': '',
        '馬単払戻': '', 'ワイド払戻': '', '三連複払戻': '', '三連単払戻': '',
        '枠連払戻': '',
    }
    for t in soup.find_all('table'):
        rows = t.find_all('tr')
        for row in rows:
            txt = row.get_text(' ', strip=True)
            if not txt: continue
            # 券種ラベルで判定
            if txt.startswith('単勝') or txt.startswith('単勝'):
                bet, nums, amounts = parse_payout_row(txt[2:])
                pay_map['単勝払戻'] = fmt_tansho(nums, amounts)
            elif txt.startswith('複勝'):
                bet, nums, amounts = parse_payout_row(txt[2:])
                pay_map['複勝払戻'] = fmt_fukusho(nums, amounts)
            elif txt.startswith('枠連'):
                bet, nums, amounts = parse_payout_row(txt[2:])
                pay_map['枠連払戻'] = fmt_umaren(nums, amounts)
            elif txt.startswith('馬連'):
                bet, nums, amounts = parse_payout_row(txt[2:])
                pay_map['馬連払戻'] = fmt_umaren(nums, amounts)
            elif txt.startswith('ワイド'):
                bet, nums, amounts = parse_payout_row(txt[3:])
                pay_map['ワイド払戻'] = fmt_wide(nums, amounts)
            elif txt.startswith('枠単'):
                pass  # 枠単は不使用
            elif txt.startswith('馬単'):
                bet, nums, amounts = parse_payout_row(txt[2:])
                pay_map['馬単払戻'] = fmt_umatan(nums, amounts)
            elif txt.startswith('3連複') or txt.startswith('三連複'):
                label_len = 3 if txt.startswith('3連複') else 3
                bet, nums, amounts = parse_payout_row(txt[label_len:])
                pay_map['三連複払戻'] = fmt_sanrenpuku(nums, amounts)
            elif txt.startswith('3連単') or txt.startswith('三連単'):
                bet, nums, amounts = parse_payout_row(txt[3:])
                pay_map['三連単払戻'] = fmt_sanrentan(nums, amounts)

    # ── 着順テーブル ──
    result_rows = []
    result_table = None
    for t in soup.find_all('table'):
        trs = t.find_all('tr')
        if len(trs) > 3:
            th_txt = trs[0].get_text(' ', strip=True) if trs else ''
            if '着' in th_txt and '馬名' in th_txt:
                result_table = t
                break

    if result_table is None:
        # フォールバック: 最大行数のtable
        tables = soup.find_all('table')
        if tables:
            result_table = max(tables, key=lambda t: len(t.find_all('tr')))

    if result_table:
        trs = result_table.find_all('tr')
        for tr in trs[1:]:  # ヘッダをスキップ
            tds = tr.find_all('td')
            if len(tds) < 6: continue
            texts = [td.get_text(' ', strip=True) for td in tds]

            # 着順判定
            try:
                chakujun = int(re.search(r'\d+', texts[0]).group())
            except:
                continue  # 取消・除外等

            try:
                waku   = re.search(r'\d+', texts[1]).group() if len(tds) > 1 else ''
                umaban = re.search(r'\d+', texts[2]).group() if len(tds) > 2 else ''
            except:
                waku = umaban = ''

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
            tsukajun   = ''

            # 後ろの列から馬体重・調教師を探す
            for i in range(len(texts)-1, 10, -1):
                if re.match(r'\d{3}\([+-]?\d+\)', texts[i]):
                    bataijyu = texts[i]
                    break
            for i in range(len(texts)-1, 10, -1):
                if re.match(r'\d{3}\([+-]?\d+\)', texts[i]):
                    bataijyu = texts[i]
                    if i > 0:
                        trainer = texts[i-1]
                    break

            row = {
                'race_id':   race_id,
                'race_name': race_name,
                'grade':     '',
                '距離':       dist_val,
                'コース':     course_type,
                '馬場状態':   track_cond,
                '着順':       chakujun,
                '枠番':       waku,
                '馬番':       umaban,
                '馬名':       horse_name,
                '性齢':       seire,
                '斤量':       kinryo,
                '騎手':       jockey,
                'タイム':     time_str,
                '着差':       chakusa,
                '通過順':     tsukajun,
                '上がり3F':   agari3f,
                '単勝オッズ': tan_odds,
                '人気':       ninki,
                '馬体重':     bataijyu,
                '調教師':     trainer,
                '年':         year,
                '場コード':   venue_code,
                '回次':       kai,
                '日次':       nichi,
                'レース番号': race_num,
            }
            row.update(pay_map)
            result_rows.append(row)

    return result_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--months', nargs='*', default=[], help='対象月 (例: 202601 202602)')
    parser.add_argument('--outdir', default='data', help='出力ディレクトリ')
    args = parser.parse_args()

    today = datetime.date.today()
    if args.months:
        months = [(int(m[:4]), int(m[4:6])) for m in args.months]
    else:
        # 2026年1月から今月まで
        months = []
        y, mo = 2026, 1
        while (y, mo) <= (today.year, today.month):
            months.append((y, mo))
            mo += 1
            if mo > 12: y += 1; mo = 1

    session = requests.Session()
    session.headers.update(HEADERS)

    for year, month in months:
        out_path = os.path.join(args.outdir, f'raceresults_nar_{year}{month:02d}.csv')
        print(f'\n{"="*60}')
        print(f'  {year}年{month:02d}月 地方競馬 収集開始 → {out_path}')

        # 既存レースIDをロードしてスキップ
        existing_ids = set()
        if os.path.exists(out_path):
            import pandas as pd
            try:
                ex = pd.read_csv(out_path, encoding='utf-8', usecols=['race_id'])
                existing_ids = set(ex['race_id'].astype(str))
                print(f'  既存: {len(existing_ids)}件スキップ')
            except: pass

        dates = fetch_dates_in_month(session, year, month)
        print(f'  開催日: {len(dates)}日')
        sleep(1, 2)

        all_rows = []
        for date_str in dates:
            race_ids = fetch_race_ids(session, date_str)
            # R8-11のみ対象
            race_ids = [r for r in race_ids if 8 <= int(r[10:12]) <= 11]
            new_ids  = [r for r in race_ids if r not in existing_ids]
            if not new_ids:
                continue
            print(f'  {date_str}: {len(new_ids)}レース取得')
            sleep(1, 2)

            for race_id in new_ids:
                venue_name = NAR_VENUE_CODES.get(race_id[4:6], '?')
                print(f'    [{race_id}] {venue_name} {race_id[10:12]}R', end=' ... ', flush=True)
                rows = fetch_race_result(session, race_id)
                if rows:
                    all_rows.extend(rows)
                    print(f'{len(rows)}頭 三連複:{rows[0].get("三連複払戻","")[:20]}')
                else:
                    print('スキップ')
                sleep(2.0, 4.0)

        if all_rows:
            mode = 'a' if existing_ids else 'w'
            with open(out_path, mode, encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction='ignore')
                if mode == 'w': writer.writeheader()
                writer.writerows(all_rows)
            print(f'  → {len(all_rows)}行 保存完了')
        else:
            print(f'  新規データなし')

    print('\n完了')


if __name__ == '__main__':
    main()

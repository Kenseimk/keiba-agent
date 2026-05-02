# -*- coding: utf-8 -*-
"""4月12日・18日の対象レース結果をnetkeibaからスクレイピング"""
import sys, io, re, time, csv, os, random, requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'ja,en-US;q=0.9',
}

TARGET_IDS = [
    '202606030609', '202606030610', '202606030611',
    '202609020609', '202609020610', '202609020611',
    '202603010209', '202603010210', '202603010211',
    '202606030709', '202606030710', '202606030711',
    '202609020709', '202609020710', '202609020711',
    '202603010309', '202603010310', '202603010311',
]

OUT_CSV = 'data/raceresults_202604_extra.csv'
FIELDS  = ['race_id', 'race_name', '着順', '馬番', '馬名', '三連複払戻']


def fetch_result(session, race_id):
    url = f'https://race.netkeiba.com/race/result.html?race_id={race_id}'
    r = session.get(url, timeout=20)
    r.encoding = 'EUC-JP'
    soup = BeautifulSoup(r.text, 'html.parser')

    # レース名
    race_name = ''
    for sel in ['.RaceName', 'h2.RaceName', '[class*="RaceName"]']:
        el = soup.select_one(sel)
        if el:
            race_name = el.get_text(strip=True)
            break

    # 着順テーブル（最も多い行数のtableを選択）
    tables = [t for t in soup.find_all('table') if len(t.find_all('tr')) > 5]
    table = max(tables, key=lambda t: len(t.find_all('tr')), default=None)

    horses = []
    if table:
        for tr in table.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) < 5:
                continue
            texts = [td.get_text(strip=True) for td in tds]
            # texts[0]=着順, texts[1]=枠番, texts[2]=馬番
            if not re.match(r'^\d{1,2}$', texts[0]):
                continue
            rank = texts[0]
            # 馬番: texts[2] が馬番（texts[1]は枠番）
            umaban = texts[2] if re.match(r'^\d{1,2}$', texts[2]) else texts[1]
            name_a = tr.find('a', href=re.compile(r'/horse/'))
            name   = name_a.get_text(strip=True) if name_a else ''
            if name:
                horses.append({'rank': rank, 'umaban': umaban, 'name': name})

    # 三連複払戻: class="Fuku3" の行から取得
    sanfuku_pay = None
    for tbl in soup.find_all('table', class_='Payout_Detail_Table'):
        for tr in tbl.find_all('tr'):
            if 'Fuku3' in (tr.get('class') or []):
                payout_td = tr.find('td', class_='Payout')
                if payout_td:
                    m = re.search(r'([\d,]+)', payout_td.get_text())
                    if m:
                        sanfuku_pay = int(m.group(1).replace(',', ''))
                break
        if sanfuku_pay is not None:
            break

    return race_name, horses, sanfuku_pay


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    # 既存データをクリアして再取得
    with open(OUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()

        for race_id in TARGET_IDS:
            print(f'  [{race_id}] 取得中...', end=' ', flush=True)
            try:
                race_name, horses, sanfuku = fetch_result(session, race_id)
                if not horses:
                    print('データなし')
                    continue
                for h in horses:
                    writer.writerow({
                        'race_id':    race_id,
                        'race_name':  race_name,
                        '着順':        h['rank'],
                        '馬番':        h['umaban'],
                        '馬名':        h['name'],
                        '三連複払戻': sanfuku or '',
                    })
                f.flush()
                top3 = [h['umaban'] for h in horses if int(h['rank']) <= 3]
                print(f'{len(horses)}頭  3着={top3}  三連複={sanfuku}円')
            except Exception as e:
                print(f'エラー: {e}')
            time.sleep(random.uniform(2, 3))

    print(f'\n完了 → {OUT_CSV}')


if __name__ == '__main__':
    main()

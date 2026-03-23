"""
scripts/fetch_history.py - 過去レースデータの一括取得スクリプト

CSVファイルからrace_idを生成し、netkeibaから
上がり3F・コーナー順・払戻オッズを取得してJSONに保存する。

使い方:
    # 2025年3月分を取得
    python scripts/fetch_history.py --year 2025 --month 3

    # 全CSVを対象に取得
    python scripts/fetch_history.py --all

    # 取得済みをスキップして未取得のみ
    python scripts/fetch_history.py --all --skip-existing

出力:
    data/netkeiba_YYYYMM.json  (月ごとのファイル)
    {
        "202509010408": {
            "race_id": "202509010408",
            "horses": [...],
            "payouts": {...}
        },
        ...
    }
"""

import os
import re
import json
import time
import glob
import argparse
import subprocess
from collections import defaultdict

# scraperをインポート（agents/配下に置く場合）
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.scraper import build_race_id, fetch_race_result

JRA = {'札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉'}
DATA_DIR = 'data'
SLEEP_SEC = 2.5  # リクエスト間隔（余裕を持って2.5秒）


def parse_csv(filepath: str) -> list:
    """CSVからJRAの全レースを読み込む（新馬除外）"""
    result = subprocess.run(
        ['iconv', '-f', 'shift_jis', '-t', 'utf-8', filepath],
        capture_output=True
    )
    lines = result.stdout.decode('utf-8', errors='replace').strip().split('\n')

    races = defaultdict(lambda: {'date': '', 'session': '', 'race_num': ''})
    i = 0
    while i + 1 < len(lines):
        l1 = lines[i].rstrip('\r')
        l2 = lines[i + 1].rstrip('\r')
        combined = l1.replace('"', '') + l2.replace('"', '')
        parts = [p.strip() for p in combined.split(',')]

        if len(parts) >= 17 and ':' in parts[0]:
            try:
                session = parts[16]
                if not any(v in session for v in JRA):
                    i += 2
                    continue
                if '新馬' in parts[15]:
                    i += 2
                    continue
                key = f'{parts[14]}_{session}_{parts[17] if len(parts)>17 else ""}'
                races[key] = {
                    'date':     parts[14],
                    'session':  session,
                    'race_num': parts[17] if len(parts) > 17 else '',
                }
            except Exception:
                pass
        i += 2

    return list(races.values())


def load_existing(output_path: str) -> dict:
    """既存の取得済みデータを読み込む"""
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_data(data: dict, output_path: str):
    """データをJSONに保存"""
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'[fetch_history] 保存: {output_path} ({len(data)}件)')


def fetch_month(year: int, month: int, skip_existing: bool = True):
    """指定月のデータを取得"""
    # CSVファイルを探す
    csv_patterns = [
        f'data/{year}_{month}_raceresults.csv',
        f'{year}_{month}_raceresults.csv',
    ]
    csv_path = None
    for p in csv_patterns:
        if os.path.exists(p):
            csv_path = p
            break

    if not csv_path:
        print(f'[fetch_history] CSVが見つかりません: {year}年{month}月')
        return

    output_path = os.path.join(DATA_DIR, f'netkeiba_{year}{str(month).zfill(2)}.json')
    existing = load_existing(output_path) if skip_existing else {}

    races = parse_csv(csv_path)
    print(f'[fetch_history] {year}年{month}月: {len(races)}レース')

    new_data = dict(existing)
    success = 0
    skip = 0
    error = 0

    for race_info in races:
        race_id = build_race_id(
            race_info['date'],
            race_info['session'],
            race_info['race_num']
        )
        if not race_id:
            continue

        if skip_existing and race_id in existing:
            skip += 1
            continue

        try:
            result = fetch_race_result(race_id)
            new_data[race_id] = result
            success += 1

            # 10件ごとに中間保存
            if success % 10 == 0:
                save_data(new_data, output_path)

        except Exception as e:
            print(f'  エラー [{race_id}]: {e}')
            error += 1
            time.sleep(SLEEP_SEC * 2)

        time.sleep(SLEEP_SEC)

    save_data(new_data, output_path)
    print(f'  完了: 取得={success}, スキップ={skip}, エラー={error}')
    return new_data


def fetch_all(skip_existing: bool = True):
    """全CSVを対象に取得"""
    csv_files = sorted(
        glob.glob('data/*_raceresults.csv') +
        glob.glob('*_raceresults.csv')
    )
    # 'all' を含むファイルは除外
    csv_files = [f for f in csv_files if 'all' not in os.path.basename(f)]

    print(f'[fetch_history] 対象CSVファイル数: {len(csv_files)}')

    for csv_path in csv_files:
        # ファイル名から年月を抽出
        match = re.search(r'(\d{4})_(\d+)_raceresults', os.path.basename(csv_path))
        if not match:
            continue
        year  = int(match.group(1))
        month = int(match.group(2))
        print(f'\n=== {year}年{month}月 ({csv_path}) ===')
        fetch_month(year, month, skip_existing=skip_existing)


# ============================================================
# 取得済みデータのロードヘルパー（morning/learnerから使う）
# ============================================================
def load_race_detail(race_id: str, data_dir: str = DATA_DIR) -> dict | None:
    """
    保存済みJSONからrace_idのデータを取得。
    なければNetkeibaからリアルタイム取得。
    """
    year_month = race_id[:6]  # 例: '202503'
    json_path = os.path.join(data_dir, f'netkeiba_{year_month}.json')

    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if race_id in data:
            return data[race_id]

    # キャッシュになければリアルタイム取得
    print(f'[load_race_detail] キャッシュなし → リアルタイム取得: {race_id}')
    try:
        result = fetch_race_result(race_id)
        # 取得後にキャッシュ保存
        existing = load_existing(json_path)
        existing[race_id] = result
        save_data(existing, json_path)
        return result
    except Exception as e:
        print(f'[load_race_detail] 取得失敗: {e}')
        return None


# ============================================================
# CLI
# ============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='netkeiba 過去データ一括取得')
    parser.add_argument('--year',  type=int, help='対象年')
    parser.add_argument('--month', type=int, help='対象月')
    parser.add_argument('--all',   action='store_true', help='全CSV対象')
    parser.add_argument('--no-skip', action='store_true', help='既存データも再取得')
    args = parser.parse_args()

    skip = not args.no_skip

    if args.all:
        fetch_all(skip_existing=skip)
    elif args.year and args.month:
        fetch_month(args.year, args.month, skip_existing=skip)
    else:
        parser.print_help()

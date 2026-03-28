"""
test_fetch.py - fetch_history.py の動作確認（ミニマムテスト）

1レースだけ取得して結果を表示する。
fetch_history.py と同じフォルダに置いて実行する。

使い方:
    python test_fetch.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_history import fetch_race_result, get_race_ids_for_date, get_kaisai_dates

print('=' * 50)
print('ステップ1: カレンダーから開催日を取得')
print('=' * 50)
dates = get_kaisai_dates(2025, 3)
print(f'取得した開催日: {dates}')

print()
print('=' * 50)
print('ステップ2: 最初の開催日のrace_idを取得')
print('=' * 50)
if dates:
    race_ids = get_race_ids_for_date(dates[0])
    print(f'race_ids: {race_ids[:5]}')  # 最初の5件だけ表示

    if race_ids:
        print()
        print('=' * 50)
        print(f'ステップ3: 最初のrace_id {race_ids[0]} の結果を取得')
        print('=' * 50)
        result = fetch_race_result(race_ids[0])

        if result:
            print(f'✅ 取得成功!')
            print(f'  race_id: {result["race_id"]}')
            print(f'  頭数: {len(result["horses"])}頭')
            print(f'  1着馬: {result["horses"][0]["name"]}')
            print(f'  上がり3F: {result["horses"][0]["agari_3f"]}')
            print(f'  コーナー順: {result["horses"][0]["corner_orders"]}')
            print(f'  払戻種: {list(result["payouts"].keys())}')
            if "単勝" in result["payouts"]:
                print(f'  単勝払戻: {result["payouts"]["単勝"]}')
            if "複勝" in result["payouts"]:
                print(f'  複勝払戻: {result["payouts"]["複勝"]}')
        else:
            print('❌ 取得失敗（Noneが返った）')
    else:
        print('❌ race_idが取得できなかった')
else:
    print('❌ 開催日が取得できなかった')

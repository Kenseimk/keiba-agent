# -*- coding: utf-8 -*-
"""
fetch_oikiri_batch.py  複数日付の調教データを一括取得
使い方: python fetch_oikiri_batch.py
"""
import os, sys
os.environ['PYTHONIOENCODING'] = 'utf-8'

from fetch_oikiri import fetch_oikiri_for_date, save_oikiri

DATES = [
    '20260321',
    '20260322',
    '20260323',
    '20260328',
    '20260329',
    '20260330',
]

for date_str in DATES:
    out_path = f'data/oikiri_{date_str}.json'
    if os.path.exists(out_path):
        print(f'{date_str}: スキップ (既存)')
        continue
    print(f'\n=== {date_str} 取得開始 ===')
    try:
        data = fetch_oikiri_for_date(date_str, premium=True)
        save_oikiri(date_str, data)
    except Exception as e:
        print(f'ERROR: {e}')

print('\n完了')

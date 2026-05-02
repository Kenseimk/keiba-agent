"""
fetch_missing_months.py  不足月の過去データを順次取得
2025/12 〜 2026/03 を fetch_history.py を使って取得する
"""
import subprocess, sys, os

months = [
    ('2025', '12'),
    ('2026', '1'),
    ('2026', '2'),
    ('2026', '3'),
]

for year, month in months:
    csv_path = f"data/raceresults_{year}{int(month):02d}.csv"
    if os.path.exists(csv_path):
        print(f"[スキップ] {csv_path} は既に存在します")
        continue

    print(f"\n{'='*50}")
    print(f"取得開始: {year}年{month}月")
    print(f"{'='*50}")

    result = subprocess.run(
        [sys.executable, 'fetch_history.py', '--year', year, '--month', month],
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )

    if result.returncode == 0:
        if os.path.exists(csv_path):
            size = os.path.getsize(csv_path)
            print(f"[完了] {csv_path} ({size:,} bytes)")
        else:
            print(f"[注意] ファイル未生成（レースなし or エラー）")
    else:
        print(f"[エラー] returncode={result.returncode}")

print("\n全月取得完了")

"""
agents/scout.py  情報収集エージェント
fetch_race.pyをエージェントとして包む
- netkeibaから出馬表・オッズ・戦績を取得
- 条件Cフィルタリング
- Notionのデータストアに保存（任意）
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fetch_race import run_fetch
from notion_store import save_predictions
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))


def run_scout(date_str: str = None) -> dict:
    """
    当日のレース情報を取得してNotionに保存する
    Returns: {'candidates': [...], 'date': date_str}
    """
    date_str = date_str or datetime.now(JST).strftime('%Y%m%d')
    print(f"[scout] 情報収集開始: {date_str}")

    races_data = run_fetch(date_str)

    candidates = races_data.get('candidates', [])
    print(f"[scout] 条件C候補: {len(candidates)}レース")

    return races_data


def run_scout_and_save(date_str: str = None) -> dict:
    """取得後にNotionへ保存（morning実行時に使用）"""
    races_data = run_scout(date_str)
    # 候補がある場合のみ保存（予測生成後にselectorが保存するので基本不要）
    return races_data

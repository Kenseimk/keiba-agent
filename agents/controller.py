"""
agents/controller.py  掛け金コントロール専任エージェント
cashierから掛け金計算ロジックを分離
- 軍資金残高から掛け金上限を計算
- Notionの掛け金設定DBを管理
- strategistへ上限を渡す
"""
import os, requests
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

BET_SETTING_DB  = "131a1230-563d-4242-b7e3-0ecb88deb1d7"
CASH_DB         = "2eb1333d21b180389c5e000b44ea9f23"

BET_RATIO = {
    "単勝":  0.06,
    "三連複": 0.02,
    "三連単": 0.01,
}
DEFAULT_LIMITS = {"単勝": 5000, "三連複": 1500, "三連単": 500}


def _headers():
    key = os.environ.get('NOTION_API_KEY', '')
    if not key:
        return None
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def get_limits(year: int = None, month: int = None) -> dict:
    """掛け金設定DBから今月の上限を取得"""
    now = datetime.now(JST)
    year  = year  or now.year
    month = month or now.month
    label = f"{year}年{month}月"

    h = _headers()
    if not h:
        print(f"[controller] NOTION_API_KEY未設定 → デフォルト値使用")
        return DEFAULT_LIMITS

    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{BET_SETTING_DB}/query",
            headers=h,
            json={"filter": {"property": "月", "title": {"equals": label}}},
            timeout=15
        )
        resp.raise_for_status()
        results = resp.json().get('results', [])
        if not results:
            print(f"[controller] 掛け金設定なし: {label} → デフォルト値使用")
            return DEFAULT_LIMITS

        props = results[0].get('properties', {})
        limits = {
            "単勝":  int(props.get("掛け金上限（単勝）",  {}).get("number") or DEFAULT_LIMITS["単勝"]),
            "三連複": int(props.get("掛け金上限（三連複）", {}).get("number") or DEFAULT_LIMITS["三連複"]),
            "三連単": int(props.get("掛け金上限（三連単）", {}).get("number") or DEFAULT_LIMITS["三連単"]),
        }
        print(f"[controller] 掛け金上限取得: {label} 単勝{limits['単勝']:,}円 三連複{limits['三連複']:,}円")
        return limits
    except Exception as e:
        print(f"[controller] 掛け金設定取得エラー: {e} → デフォルト値使用")
        return DEFAULT_LIMITS


def calc_limits_from_balance(balance: int) -> dict:
    """軍資金残高から掛け金上限を計算（100円単位）"""
    limits = {}
    for bet_type, ratio in BET_RATIO.items():
        limits[bet_type] = int(balance * ratio // 100) * 100
    return limits


def save_limits(year: int, month: int, balance: int) -> dict:
    """掛け金設定DBに上限を保存"""
    h = _headers()
    if not h:
        return {}

    limits   = calc_limits_from_balance(balance)
    date_str = f"{year:04d}-{month:02d}-01"
    label    = f"{year}年{month}月"

    payload = {
        "parent": {"database_id": BET_SETTING_DB},
        "properties": {
            "月":              {"title": [{"text": {"content": label}}]},
            "軍資金残高":       {"number": balance},
            "掛け金上限（単勝）":  {"number": limits["単勝"]},
            "掛け金上限（三連複）": {"number": limits["三連複"]},
            "掛け金上限（三連単）": {"number": limits["三連単"]},
            "設定日":           {"date": {"start": date_str}},
            "メモ":             {"rich_text": [{"text": {"content": "軍資金6%/2%/1%"}}]},
        }
    }
    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=h, json=payload, timeout=15
        )
        resp.raise_for_status()
        print(f"[controller] ✅ 掛け金設定保存: {label} 単勝{limits['単勝']:,}円")
        return limits
    except Exception as e:
        print(f"[controller] ❌ 掛け金設定保存エラー: {e}")
        return {}


def format_limits_report(limits: dict, balance: int, year: int, month: int) -> str:
    return (
        f"## 💴 {year}年{month}月 掛け金上限設定\n\n"
        f"**軍資金残高**: {balance:,}円\n\n"
        f"| 馬券 | 上限 | 配分率 |\n"
        f"|------|------|--------|\n"
        f"| 単勝   | **{limits.get('単勝',0):,}円**  | 6% |\n"
        f"| 三連複 | **{limits.get('三連複',0):,}円** | 2% |\n"
        f"| 三連単 | **{limits.get('三連単',0):,}円** | 1% |\n"
    )

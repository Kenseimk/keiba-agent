"""
agents/cashier.py  軍資金管理エージェント（月次精算・月初補充専任）
掛け金計算はcontroller.pyに移譲
"""
import os, requests, datetime, calendar
from datetime import timezone, timedelta

JST             = timezone(timedelta(hours=9))
HORSE_RACING_DB = "2df1333d21b1808293adc2fe02155ce9"
CASH_DB         = "2eb1333d21b180389c5e000b44ea9f23"
MONTHLY_BUDGET  = 20000
PROFIT_RATE     = 0.70


def _headers():
    key = os.environ.get('NOTION_API_KEY', '')
    if not key: return None
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def get_cash_balance() -> int:
    """軍資金DBから現在の残高を合計"""
    h = _headers()
    if not h: return 0
    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{CASH_DB}/query",
            headers=h,
            json={"sorts": [{"property": "日付", "direction": "descending"}], "page_size": 100},
            timeout=15
        )
        resp.raise_for_status()
        total = sum(
            p.get('properties', {}).get('記録', {}).get('number') or 0
            for p in resp.json().get('results', [])
        )
        print(f"[cashier] 軍資金残高: {total:,}円")
        return total
    except Exception as e:
        print(f"[cashier] 残高取得エラー: {e}")
        return 0


def save_cash(label: str, amount: int, date_str: str) -> bool:
    h = _headers()
    if not h: return False
    payload = {
        "parent": {"database_id": CASH_DB},
        "properties": {
            "名前": {"title": [{"text": {"content": label}}]},
            "記録": {"number": amount},
            "日付": {"date": {"start": date_str}},
        }
    }
    try:
        resp = requests.post("https://api.notion.com/v1/pages",
                             headers=h, json=payload, timeout=15)
        resp.raise_for_status()
        print(f"[cashier] ✅ 軍資金記録: {label} {amount:,}円")
        return True
    except Exception as e:
        print(f"[cashier] ❌ 記録エラー: {e}")
        return False


def calc_monthly_profit(year: int, month: int) -> dict:
    """HorseRacingDBからその月の損益を集計"""
    h = _headers()
    if not h: return {"profit": 0, "invest": 0, "payout": 0, "count": 0}

    last_day = calendar.monthrange(year, month)[1]
    start    = f"{year:04d}-{month:02d}-01"
    end      = f"{year:04d}-{month:02d}-{last_day:02d}"

    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{HORSE_RACING_DB}/query",
            headers=h,
            json={"filter": {"and": [
                {"property": "日付", "date": {"on_or_after": start}},
                {"property": "日付", "date": {"on_or_before": end}},
            ]}, "page_size": 100},
            timeout=15
        )
        resp.raise_for_status()
        pages = resp.json().get('results', [])
        invest = payout = 0
        for p in pages:
            props   = p.get('properties', {})
            invest += props.get('掛け金', {}).get('number') or 0
            payout += props.get('払戻金', {}).get('number') or 0
        profit = payout - invest
        print(f"[cashier] {year}/{month}月 損益: 投資{invest:,}円 回収{payout:,}円 損益{profit:,}円")
        return {"profit": profit, "invest": invest, "payout": payout, "count": len(pages)}
    except Exception as e:
        print(f"[cashier] 集計エラー: {e}")
        return {"profit": 0, "invest": 0, "payout": 0, "count": 0}


def run_month_end(year: int, month: int) -> dict:
    """月末精算"""
    last_day = calendar.monthrange(year, month)[1]
    date_str = f"{year:04d}-{month:02d}-{last_day:02d}"
    result   = calc_monthly_profit(year, month)
    profit   = result['profit']
    added    = 0

    if profit > 0:
        added = int(profit * PROFIT_RATE)
        save_cash(f"{year}年{month}月 利益70%追加", added, date_str)
        print(f"[cashier] 月末精算: +{added:,}円追加")
    else:
        print(f"[cashier] 月末精算: マイナスのため追加なし")

    return {**result, "year": year, "month": month, "added": added}


def run_month_start(year: int, month: int) -> dict:
    """月初補充 + controllerに掛け金上限を更新させる"""
    date_str = f"{year:04d}-{month:02d}-01"
    save_cash(f"{year}年{month}月 月次補充", MONTHLY_BUDGET, date_str)

    # controllerに残高を渡して掛け金上限を更新
    balance = get_cash_balance()
    limits  = {}
    try:
        from agents.controller import save_limits
        limits = save_limits(year, month, balance)
    except Exception as e:
        print(f"[cashier] controller呼び出しエラー: {e}")

    return {"year": year, "month": month, "added": MONTHLY_BUDGET,
            "balance": balance, "limits": limits}


# ===== オッズ影響上限チェック =====

ODDS_IMPACT_THRESHOLD = 50000  # この金額を超えたらオッズに影響し始める

def check_odds_impact_threshold(balance: int) -> dict:
    """
    単勝上限がオッズ影響ラインを超えていないかチェック
    毎月初のcashier_start実行時に自動チェック
    """
    from agents.controller import calc_limits_from_balance
    limits  = calc_limits_from_balance(balance)
    tansho  = limits.get('単勝', 0)
    reached = tansho >= ODDS_IMPACT_THRESHOLD

    return {
        'balance':   balance,
        'tansho':    tansho,
        'reached':   reached,
        'threshold': ODDS_IMPACT_THRESHOLD,
    }

def format_odds_impact_alert(check: dict) -> str:
    """オッズ影響ライン到達アラートのDiscordメッセージ"""
    return (
        f"## ⚠️ 余剰資金アラート\n\n"
        f"単勝上限が **{check['tansho']:,}円** に達しました。\n\n"
        f"これはオッズに影響し始める目安の "
        f"**{check['threshold']:,}円** を超えています。\n\n"
        f"現在の軍資金残高: **{check['balance']:,}円**\n\n"
        f"余剰資金を別の投資に回すことを検討してください。\n"
        f"（インデックス投資・米国株・長期投資への移行タイミングです）"
    )

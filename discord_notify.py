"""
discord_notify.py  Discord Webhook 通知
"""

import requests, json, datetime

# 色定数
COLOR_GREEN  = 0x1D9E75
COLOR_ORANGE = 0xEF9F27
COLOR_RED    = 0xE24B4A
COLOR_BLUE   = 0x378ADD
COLOR_GRAY   = 0x888780


def send_webhook(webhook_url: str, content: str = None, embeds: list = None) -> bool:
    """Webhookにメッセージを送信"""
    payload = {}
    if content:
        payload['content'] = content
    if embeds:
        payload['embeds'] = embeds

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[discord] 送信エラー: {e}")
        return False


def notify_morning(
    webhook_url: str,
    selector_text: str,
    verifier_text: str,
    verdict: str,
    date_str: str = None,
):
    """朝の予想通知（エージェント①②の統合結果）"""
    date_str = date_str or datetime.date.today().strftime('%Y年%m月%d日')

    verdict_color = {
        '推奨': COLOR_GREEN,
        '要注意': COLOR_ORANGE,
        '見送り推奨': COLOR_RED,
        'なし': COLOR_GRAY,
    }.get(verdict, COLOR_GRAY)

    # 2000文字制限に注意してtruncate
    sel_short = selector_text[:1800] if len(selector_text) > 1800 else selector_text
    ver_short = verifier_text[:800]  if len(verifier_text) > 800  else verifier_text

    embeds = [
        {
            "title":       f"競馬予想 {date_str}",
            "description": f"エージェント② 最終評価: **{verdict}**",
            "color":       verdict_color,
            "fields": [
                {
                    "name":   "エージェント①（選出）",
                    "value":  sel_short or "参加対象レースなし",
                    "inline": False,
                },
                {
                    "name":   "エージェント②（反証）",
                    "value":  ver_short or "—",
                    "inline": False,
                },
            ],
            "footer": {
                "text": "購入は手動で行ってください / keiba-agent v4.0"
            },
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    ]

    return send_webhook(webhook_url, embeds=embeds)


def notify_evening(
    webhook_url: str,
    learner_text: str,
    date_str: str = None,
):
    """夜の学習レポート通知"""
    date_str = date_str or datetime.date.today().strftime('%Y年%m月%d日')

    embeds = [
        {
            "title":       f"夜間学習レポート {date_str}",
            "description": learner_text[:2000],
            "color":       COLOR_BLUE,
            "footer":      {"text": "keiba-agent v4.0 / 学習エージェント"},
            "timestamp":   datetime.datetime.utcnow().isoformat(),
        }
    ]

    return send_webhook(webhook_url, embeds=embeds)


def notify_error(webhook_url: str, message: str):
    """エラー通知"""
    embeds = [{
        "title":       "エラー発生",
        "description": message[:1000],
        "color":       COLOR_RED,
        "timestamp":   datetime.datetime.utcnow().isoformat(),
    }]
    return send_webhook(webhook_url, embeds=embeds)


def test_webhook(webhook_url: str) -> bool:
    """Webhookの接続テスト"""
    return send_webhook(
        webhook_url,
        content="keiba-agent 接続テスト OK ✅"
    )


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        url = sys.argv[1]
        print("接続テスト送信中...")
        ok = test_webhook(url)
        print("成功" if ok else "失敗")
    else:
        print("使い方: python discord_notify.py <WEBHOOK_URL>")

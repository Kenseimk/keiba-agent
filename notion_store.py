"""
notion_store.py  Notion APIで予測データを保存・読み込み
"""

import json, os, re, requests
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))
NOTION_PAGE_ID = "32a1333d-21b1-813e-a2c8-f18d52f3c7de"

def _headers():
    key = os.environ.get('NOTION_API_KEY', '')
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

def _key_ok():
    key = os.environ.get('NOTION_API_KEY', '')
    if not key:
        print("[notion] ❌ NOTION_API_KEY が未設定")
        return False
    return True

def save_predictions(date_str: str, predictions: list) -> bool:
    if not _key_ok(): return False

    # 既存ページを削除（同日の重複防止）
    try:
        resp = requests.post(
            "https://api.notion.com/v1/search",
            headers=_headers(),
            json={"query": f"selected_{date_str}", "filter": {"value": "page", "property": "object"}, "page_size": 5},
            timeout=15
        )
        for r in resp.json().get('results', []):
            title_list = r.get('properties', {}).get('title', {}).get('title', [])
            title = title_list[0]['plain_text'] if title_list else ''
            if title == f"selected_{date_str}":
                requests.patch(
                    f"https://api.notion.com/v1/pages/{r['id']}",
                    headers=_headers(),
                    json={"archived": True},
                    timeout=10
                )
                print(f"[notion] 既存ページを削除: {title}")
    except Exception as e:
        print(f"[notion] 既存ページ削除エラー（続行）: {e}")

    # データをJSON文字列に変換
    data_str = json.dumps(predictions, ensure_ascii=False)

    # 2000文字ごとにブロック分割
    blocks = []
    for i in range(0, len(data_str), 1999):
        chunk = data_str[i:i+1999]
        blocks.append({
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}],
                "language": "json"
            }
        })

    payload = {
        "parent": {"page_id": NOTION_PAGE_ID},
        "properties": {
            "title": {"title": [{"text": {"content": f"selected_{date_str}"}}]}
        },
        "children": blocks
    }

    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=_headers(),
            json=payload,
            timeout=20
        )
        resp.raise_for_status()
        pid = resp.json()['id']
        print(f"[notion] ✅ 保存完了: selected_{date_str} (id={pid})")
        return True
    except Exception as e:
        print(f"[notion] ❌ 保存失敗: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[notion] レスポンス: {e.response.text[:300]}")
        return False

def load_predictions(date_str: str) -> list:
    if not _key_ok(): return []

    try:
        resp = requests.post(
            "https://api.notion.com/v1/search",
            headers=_headers(),
            json={"query": f"selected_{date_str}", "filter": {"value": "page", "property": "object"}, "page_size": 10},
            timeout=15
        )
        resp.raise_for_status()

        target_id = None
        for r in resp.json().get('results', []):
            title_list = r.get('properties', {}).get('title', {}).get('title', [])
            title = title_list[0]['plain_text'] if title_list else ''
            if title == f"selected_{date_str}":
                target_id = r['id']
                break

        if not target_id:
            print(f"[notion] ページなし: selected_{date_str}")
            return []

        blocks_resp = requests.get(
            f"https://api.notion.com/v1/blocks/{target_id}/children?page_size=100",
            headers=_headers(),
            timeout=15
        )
        blocks_resp.raise_for_status()

        json_str = ''
        for block in blocks_resp.json().get('results', []):
            if block['type'] == 'code':
                for t in block['code']['rich_text']:
                    json_str += t['text']['content']

        if not json_str:
            print(f"[notion] データ空: selected_{date_str}")
            return []

        predictions = json.loads(json_str)
        print(f"[notion] ✅ 読み込み完了: selected_{date_str} ({len(predictions)}レース)")
        return predictions

    except Exception as e:
        print(f"[notion] ❌ 読み込み失敗: {e}")
        return []

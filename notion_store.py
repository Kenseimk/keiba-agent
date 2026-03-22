"""
notion_store.py  Notionをデータストアとして使う
morning実行後 → Notionに予測データを保存
prerace実行時 → Notionから予測データを読み込む
"""

import json, os, re
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

# keiba-agent データストアのページID
NOTION_PAGE_ID = "32a1333d-21b1-813e-a2c8-f18d52f3c7de"
NOTION_API_KEY = os.environ.get('NOTION_API_KEY', '')

import requests

def get_headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

def save_predictions(date_str: str, predictions: list) -> bool:
    """予測データをNotionページの子ページとして保存"""
    if not NOTION_API_KEY:
        print("[notion] NOTION_API_KEY未設定")
        return False

    # 予測データをJSON文字列に変換
    data_json = json.dumps(predictions, ensure_ascii=False)

    # 子ページを作成
    payload = {
        "parent": {"page_id": NOTION_PAGE_ID},
        "properties": {
            "title": {
                "title": [{"text": {"content": f"selected_{date_str}"}}]
            }
        },
        "children": [
            {
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [{"type": "text", "text": {"content": data_json[:2000]}}],
                    "language": "json"
                }
            }
        ]
    }

    # 2000文字を超える場合は複数ブロックに分割
    if len(data_json) > 2000:
        blocks = []
        for i in range(0, len(data_json), 2000):
            blocks.append({
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [{"type": "text", "text": {"content": data_json[i:i+2000]}}],
                    "language": "json"
                }
            })
        payload["children"] = blocks

    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=get_headers(),
            json=payload,
            timeout=15
        )
        resp.raise_for_status()
        page_id = resp.json()['id']
        print(f"[notion] 保存完了: selected_{date_str} (page_id={page_id})")
        return True
    except Exception as e:
        print(f"[notion] 保存エラー: {e}")
        return False

def load_predictions(date_str: str) -> list:
    """Notionから予測データを読み込む"""
    if not NOTION_API_KEY:
        print("[notion] NOTION_API_KEY未設定")
        return []

    # 子ページを検索
    try:
        resp = requests.post(
            "https://api.notion.com/v1/search",
            headers=get_headers(),
            json={
                "query": f"selected_{date_str}",
                "filter": {"value": "page", "property": "object"},
                "page_size": 5
            },
            timeout=15
        )
        resp.raise_for_status()
        results = resp.json().get('results', [])

        # ページタイトルが一致するものを探す
        target_page = None
        for r in results:
            title = r.get('properties', {}).get('title', {}).get('title', [])
            title_text = title[0]['text']['content'] if title else ''
            if title_text == f"selected_{date_str}":
                target_page = r
                break

        if not target_page:
            print(f"[notion] ページなし: selected_{date_str}")
            return []

        # ページのブロックを取得
        page_id = target_page['id']
        blocks_resp = requests.get(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers=get_headers(),
            timeout=15
        )
        blocks_resp.raise_for_status()
        blocks = blocks_resp.json().get('results', [])

        # コードブロックからJSONを結合
        json_str = ''
        for block in blocks:
            if block['type'] == 'code':
                texts = block['code']['rich_text']
                for t in texts:
                    json_str += t['text']['content']

        if not json_str:
            print(f"[notion] データなし: selected_{date_str}")
            return []

        predictions = json.loads(json_str)
        print(f"[notion] 読み込み完了: selected_{date_str} ({len(predictions)}レース)")
        return predictions

    except Exception as e:
        print(f"[notion] 読み込みエラー: {e}")
        return []

def delete_old_predictions(keep_days: int = 7):
    """古い予測データを削除（keep_days日より古いもの）"""
    if not NOTION_API_KEY:
        return
    cutoff = (datetime.now(JST) - timedelta(days=keep_days)).strftime('%Y%m%d')
    try:
        resp = requests.post(
            "https://api.notion.com/v1/search",
            headers=get_headers(),
            json={"query": "selected_", "filter": {"value": "page", "property": "object"}, "page_size": 25},
            timeout=15
        )
        for r in resp.json().get('results', []):
            title = r.get('properties', {}).get('title', {}).get('title', [])
            title_text = title[0]['text']['content'] if title else ''
            m = re.match(r'selected_(\d{8})', title_text)
            if m and m.group(1) < cutoff:
                requests.patch(
                    f"https://api.notion.com/v1/pages/{r['id']}",
                    headers=get_headers(),
                    json={"archived": True},
                    timeout=10
                )
                print(f"[notion] 削除: {title_text}")
    except Exception as e:
        print(f"[notion] 削除エラー: {e}")

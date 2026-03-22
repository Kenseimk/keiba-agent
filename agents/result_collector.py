"""
agents/result_collector.py  全レース結果収集エージェント
evening実行時に当日の全レース結果をnetkeibaから取得して
NotionのレースDB（🏁 レース結果DB）に保存
"""
import re, time, datetime, os, requests
from pathlib import Path
from datetime import timezone, timedelta

JST      = timezone(timedelta(hours=9))
DATA_DIR = Path('data')

RACE_RESULT_DB = "e9a606be-d024-43c6-aa1b-ea1e12acbff3"


def _headers():
    key = os.environ.get('NOTION_API_KEY', '')
    if not key: return None
    return {
        "Authorization":  f"Bearer {key}",
        "Content-Type":   "application/json",
        "Notion-Version": "2022-06-28",
    }


def _save_to_notion(rows: list[dict], date_fmt: str) -> int:
    """レース結果をNotionのレース結果DBに保存"""
    h = _headers()
    if not h:
        print("[result_collector] NOTION_API_KEY未設定")
        return 0

    saved = 0
    for row in rows:
        # 着順が数字でない場合（除外・取消等）はスキップ
        try:
            rank = int(row['rank'])
        except (ValueError, KeyError):
            continue

        # レース名_馬名 をタイトルに
        title = f"{row['race_name']} {row['horse_name']}"

        # 上がり3Fを数値化
        agari = None
        try:
            agari = float(row['agari_3f'])
        except (ValueError, TypeError):
            pass

        # 単勝オッズを数値化
        odds = None
        try:
            odds = float(str(row['odds']).replace('---', ''))
        except (ValueError, TypeError):
            pass

        payload = {
            "parent": {"database_id": RACE_RESULT_DB},
            "properties": {
                "レース名": {"title": [{"text": {"content": title}}]},
                "date:日付:start": date_fmt,
                "date:日付:is_datetime": 0,
                "race_id":    {"rich_text": [{"text": {"content": row.get('race_id', '')}}]},
                "開催場":      {"select": {"name": row['venue']}} if row.get('venue') and row['venue'] in ['札幌','函館','福島','新潟','東京','中山','中京','京都','阪神','小倉'] else None,
                "コース":      {"select": {"name": row['course']}} if row.get('course') and row['course'] in ['芝','ダート','障害'] else None,
                "距離":       {"number": int(row['dist'])} if row.get('dist') else None,
                "頭数":       {"number": int(row['n_horses'])} if row.get('n_horses') else None,
                "着順":       {"number": rank},
                "馬名":       {"rich_text": [{"text": {"content": row.get('horse_name', '')}}]},
                "騎手":       {"rich_text": [{"text": {"content": row.get('jockey', '')}}]},
                "タイム":      {"rich_text": [{"text": {"content": row.get('time', '')}}]},
                "上がり3F":    {"number": agari} if agari else None,
                "単勝オッズ":   {"number": odds} if odds else None,
                "人気":       {"number": int(row['popularity'])} if row.get('popularity') else None,
                "着差":       {"rich_text": [{"text": {"content": row.get('margin', '')}}]},
                "コーナー通過順": {"rich_text": [{"text": {"content": row.get('corner_pass', '')}}]},
                "馬体重":      {"number": int(row['weight'])} if row.get('weight') else None,
                "体重増減":    {"number": int(row['weight_diff'])} if row.get('weight_diff') else None,
                "調教師":      {"rich_text": [{"text": {"content": row.get('trainer', '')}}]},
                "スコア予測対象": {"checkbox": row.get('is_target', False)},
            }
        }

        # Noneのプロパティを除外
        payload["properties"] = {k: v for k, v in payload["properties"].items() if v is not None}

        try:
            resp = requests.post(
                "https://api.notion.com/v1/pages",
                headers=h, json=payload, timeout=15
            )
            resp.raise_for_status()
            saved += 1
        except Exception as e:
            print(f"[result_collector] 保存エラー: {title} - {e}")

        time.sleep(0.3)  # API制限対策

    return saved


def fetch_race_ids_for_date(date_str: str, page) -> list[str]:
    """当日開催の全レースIDを取得"""
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        race_ids = page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="race_id="]'));
            const ids = new Set();
            links.forEach(a => {
                const m = a.href.match(/race_id=(\\d{12})/);
                if (m) ids.add(m[1]);
            });
            return Array.from(ids).sort();
        }""") or []
        print(f"[result_collector] 本日のレース数: {len(race_ids)}R")
        return race_ids
    except Exception as e:
        print(f"[result_collector] レースID取得エラー: {e}")
        return []


def fetch_race_result(race_id: str, page) -> tuple[list[dict], dict]:
    """netkeibaからレース結果を取得"""
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        rows = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('.RaceTable01 tr').forEach(row => {
                const tds = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (tds.length < 8 || !/^\\d+$/.test(tds[0])) return;
                results.push({
                    rank: tds[0], horse_name: tds[3] || '',
                    jockey: tds[6] || '', time: tds[7] || '',
                    margin: tds[8] || '', popularity: tds[9] || '',
                    odds: tds[10] || '', corner_pass: tds[11] || '',
                    agari_3f: tds[12] || '', weight: tds[13] || '',
                    trainer: tds[14] || '',
                });
            });
            return results;
        }""") or []

        race_info = page.evaluate("""() => {
            const title = document.querySelector('.RaceName')?.innerText?.trim() || '';
            const data  = document.querySelector('.RaceData01')?.innerText?.trim() || '';
            return {title, data};
        }""") or {}

        return rows, race_info
    except Exception as e:
        print(f"[result_collector] 結果取得エラー {race_id}: {e}")
        return [], {}


def run_result_collector(date_str: str = None,
                         target_race_ids: list[str] = None) -> dict:
    """
    当日の全レース結果を収集してNotionに保存
    target_race_ids: スコア予測対象のrace_id一覧（スコア予測対象フラグに使用）
    """
    from playwright.sync_api import sync_playwright
    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    target_ids = set(target_race_ids or [])

    print(f"[result_collector] 全レース結果収集開始: {date_str}")

    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        page.set_default_timeout(20000)

        race_ids = fetch_race_ids_for_date(date_str, page)
        if not race_ids:
            print("[result_collector] レースIDが取得できませんでした")
            browser.close()
            return {"saved": 0, "races": 0}

        for i, race_id in enumerate(race_ids):
            print(f"[result_collector] 取得中 ({i+1}/{len(race_ids)}): {race_id}")
            rows, race_info = fetch_race_result(race_id, page)

            race_name = race_info.get('title', '')
            data_text = race_info.get('data', '')

            dist   = 0
            course = 'ダート'
            venue  = ''

            m = re.search(r'(\d{3,4})m', data_text)
            if m: dist = int(m.group(1))
            if '芝' in data_text: course = '芝'
            m = re.search(r'(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)',
                          race_name + data_text)
            if m: venue = m.group(1)

            for row in rows:
                w_str = row.get('weight', '')
                w_m   = re.search(r'(\d+)\(([+-]?\d+)\)', w_str)
                all_rows.append({
                    **row,
                    'race_id':   race_id,
                    'race_name': race_name,
                    'venue':     venue,
                    'course':    course,
                    'dist':      dist,
                    'n_horses':  len(rows),
                    'weight':      w_m.group(1) if w_m else '',
                    'weight_diff': w_m.group(2) if w_m else '',
                    'is_target': race_id in target_ids,
                })

            time.sleep(0.5)

        browser.close()

    if not all_rows:
        print("[result_collector] データが取得できませんでした")
        return {"saved": 0, "races": len(race_ids)}

    saved = _save_to_notion(all_rows, date_fmt)
    print(f"[result_collector] ✅ 完了: {saved}件保存 / {len(race_ids)}レース")
    return {"saved": saved, "races": len(race_ids), "rows": len(all_rows)}

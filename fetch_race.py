"""
fetch_race.py
netkeibaから当日の開催情報・オッズ・horse_idを取得する
ブラウザ（Playwright）を使ってJSを実行して取得
"""

import json, re, time, datetime
from playwright.sync_api import sync_playwright

BASE_URL = "https://race.netkeiba.com"
DB_URL   = "https://db.netkeiba.com"

TARGET_VENUES = ['東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌']
MIN_DIST = 1800   # 1800m以上
MAX_HORSES = 14   # 14頭以下

def get_race_date(date: datetime.date = None) -> str:
    d = date or datetime.date.today()
    return d.strftime("%Y%m%d")

def fetch_race_list(page, date_str: str) -> list[dict]:
    """当日の8〜11Rレース一覧を取得"""
    url = f"{BASE_URL}/top/race_list.html?kaisai_date={date_str}"
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    races = page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="race_id="]'));
        const results = {};
        links.forEach(a => {
            const m = a.href.match(/race_id=(\\d{12})/);
            if (!m) return;
            const rid = m[1];
            const rnum = parseInt(rid.slice(10, 12));
            if (rnum < 8 || rnum > 11) return;
            const txt = a.closest('li,div,td')?.textContent || a.textContent;
            if (!results[rid]) results[rid] = {
                race_id: rid,
                rnum: rnum,
                text: txt.replace(/\\s+/g,' ').trim().slice(0,100)
            };
        });
        return Object.values(results);
    }""")
    return races or []

def fetch_odds_and_shutuba(page, race_id: str) -> dict:
    """出馬表からレース情報・全馬オッズ・horse_idを取得"""
    # 出馬表
    shutuba_url = f"{BASE_URL}/race/shutuba.html?race_id={race_id}"
    page.goto(shutuba_url, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    horses = page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="/horse/"]'));
        const horses = {};
        links.forEach(a => {
            const m = a.href.match(/\\/horse\\/(\\d{10})/);
            const name = a.textContent.trim().replace(/^\\d+\\s*\\n?/,'').trim();
            if (m && name.length > 1 && !horses[m[1]]) {
                // 騎手リンクを同じ行から取得
                const row = a.closest('tr');
                const jLink = row?.querySelector('a[href*="/jockey/"]');
                horses[m[1]] = {name, horse_id: m[1], jockey: jLink?.textContent.trim() || ''};
            }
        });
        return Object.values(horses);
    }""")

    # レース情報（距離・頭数・コース・クラス）
    race_meta = page.evaluate("""() => {
        const meta = document.querySelector('.RaceData01, .RaceData')?.textContent || '';
        const title = document.querySelector('h2.RaceName, .RaceName')?.textContent?.trim() || '';
        return {meta, title};
    }""")

    # オッズ取得
    odds_url = f"{BASE_URL}/odds/index.html?race_id={race_id}"
    page.goto(odds_url, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    odds_data = page.evaluate("""() => {
        const rows = Array.from(document.querySelectorAll('tr')).filter(r => r.querySelectorAll('td').length >= 4);
        const data = [];
        rows.forEach(r => {
            const tds = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
            const link = r.querySelector('a[href*="/horse/"]');
            if (link && /^\\d+$/.test(tds[0])) {
                const m = link.href.match(/\\/horse\\/(\\d{10})/);
                data.push({
                    pop: parseInt(tds[0]),
                    name: link.textContent.trim(),
                    odds: parseFloat(tds[tds.length-1]),
                    horse_id: m ? m[1] : null
                });
            }
        });
        return data.filter(d => d.pop && d.odds);
    }""")

    # 距離・頭数を parse
    meta_text = race_meta.get('meta','') + race_meta.get('title','')
    dist_m = re.search(r'(\d{3,4})m', meta_text)
    horses_m = re.search(r'(\d+)頭', meta_text)
    course_m = '芝' if '芝' in meta_text and 'ダート' not in meta_text else 'ダート'
    dist = int(dist_m.group(1)) if dist_m else 0
    n_horses = int(horses_m.group(1)) if horses_m else len(odds_data)
    race_name = race_meta.get('title','').strip()

    return {
        'race_id': race_id,
        'race_name': race_name,
        'dist': dist,
        'n_horses': n_horses,
        'course': course_m,
        'horses': horses,    # horse_id -> {name, jockey}
        'odds': odds_data,   # [{pop, name, odds, horse_id}]
    }

def fetch_horse_histories(page, horse_ids: list[str]) -> dict:
    """db.netkeibaから各馬の過去成績（上がり3F・passage）を取得"""
    # db.netkeiba.comに移動（同ドメインfetch用）
    page.goto(f"{DB_URL}/horse/result/{horse_ids[0]}/", wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    # 全馬を一括fetchするJSを実行
    horses_json = json.dumps({hid: hid for hid in horse_ids})

    script = f"""
    async () => {{
        const ids = {horses_json};
        const results = {{}};
        for (const [id] of Object.entries(ids)) {{
            try {{
                const res = await fetch('/horse/result/' + id + '/');
                const text = await res.text();
                const doc = new DOMParser().parseFromString(text, 'text/html');
                const rows = Array.from(doc.querySelectorAll('table tr'))
                    .filter(r => r.querySelectorAll('td').length > 20);
                results[id] = rows.slice(0, 6).map(r => {{
                    const t = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
                    return {{date:t[0], rank:t[11], dist:t[14], time:t[18],
                             margin:t[19], agari:t[23], passage:t[21], weight:t[24]}};
                }});
            }} catch(e) {{
                results[id] = [{{error: e.message}}];
            }}
            await new Promise(r => setTimeout(r, 400));
        }}
        return results;
    }}
    """

    result = page.evaluate(script)
    return result or {}

def filter_candidate_races(races_info: list[dict]) -> list[dict]:
    """条件C候補（頭数14以下・1800m以上）だけ返す"""
    candidates = []
    for r in races_info:
        if r['dist'] >= MIN_DIST and r['n_horses'] <= MAX_HORSES:
            # 障害・新馬・未勝利除外
            name = r.get('race_name','')
            if any(kw in name for kw in ['障害','ハードル','スティープル','新馬','未勝利']):
                continue
            candidates.append(r)
    return candidates

def run_fetch(date_str: str = None) -> dict:
    """メイン取得関数"""
    date_str = date_str or get_race_date()
    print(f"[fetch] 対象日: {date_str}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)

        # 1. レース一覧取得
        race_list = fetch_race_list(page, date_str)
        print(f"[fetch] 8〜11R発見: {len(race_list)}レース")

        # 2. 各レースの詳細取得
        race_details = []
        for r in race_list:
            try:
                detail = fetch_odds_and_shutuba(page, r['race_id'])
                detail['rnum'] = r['rnum']
                race_details.append(detail)
                print(f"[fetch] {r['race_id']}: {detail['race_name']} {detail['dist']}m {detail['n_horses']}頭")
                time.sleep(0.5)
            except Exception as e:
                print(f"[fetch] ERROR {r['race_id']}: {e}")

        # 3. 条件C候補に絞る
        candidates = filter_candidate_races(race_details)
        print(f"[fetch] 条件C候補: {len(candidates)}レース")

        # 4. 候補レースの全馬戦績取得
        for r in candidates:
            horse_ids = [h['horse_id'] for h in r['horses'] if h.get('horse_id')]
            if horse_ids:
                histories = fetch_horse_histories(page, horse_ids)
                r['histories'] = histories
                print(f"[fetch] 戦績取得: {r['race_name']} {len(histories)}頭")

        browser.close()

    result = {
        'date': date_str,
        'all_races': race_details,
        'candidates': candidates,
    }

    # 保存
    out_path = f"/home/claude/keiba_agent/data/races_{date_str}.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[fetch] 保存: {out_path}")

    return result

if __name__ == '__main__':
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_fetch(date_arg)

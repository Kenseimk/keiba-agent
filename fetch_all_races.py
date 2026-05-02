# -*- coding: utf-8 -*-
"""
fetch_all_races.py
指定日の全レース（R番号制限なし）を取得してJSONに保存する
"""
import json, re, time, sys
from playwright.sync_api import sync_playwright

BASE_URL = "https://race.netkeiba.com"

def fetch_all(date_str: str):
    print(f"[fetch] 対象日: {date_str}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)

        url = f"{BASE_URL}/top/race_list.html?kaisai_date={date_str}"
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        races = page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="race_id="]'));
            const results = {};
            links.forEach(a => {
                const m = a.href.match(/race_id=(\d{12})/);
                if (!m) return;
                const rid = m[1];
                const rnum = parseInt(rid.slice(10, 12));
                const container = a.closest('li,div,td,dl') || a.parentElement;
                const txt = (container?.textContent || a.textContent).replace(/\s+/g,' ').trim();
                const distM = txt.match(/(\d{3,4})m/);
                const horsesM = txt.match(/(\d{1,2})頭/);
                const isDirt = txt.includes('ダート') || /ダ\d/.test(txt);
                if (!results[rid]) results[rid] = {
                    race_id: rid,
                    rnum: rnum,
                    text: txt.slice(0,120),
                    dist_hint: distM ? parseInt(distM[1]) : 0,
                    n_hint: horsesM ? parseInt(horsesM[1]) : 0,
                    course_hint: isDirt ? 'ダート' : (txt.includes('芝') ? '芝' : 'ダート'),
                };
            });
            return Object.values(results);
        }""")
        races = races or []
        print(f"[fetch] 全R発見: {len(races)}レース")

        race_details = []
        for r in sorted(races, key=lambda x: x['race_id']):
            rid = r['race_id']
            try:
                shutuba_url = f"{BASE_URL}/race/shutuba.html?race_id={rid}"
                page.goto(shutuba_url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)

                horses = page.evaluate("""() => {
                    const links = Array.from(document.querySelectorAll('a[href*="/horse/"]'));
                    const horses = {};
                    links.forEach(a => {
                        const m = a.href.match(/\\/horse\\/(\\d{10})/);
                        const name = a.textContent.trim().replace(/^\\d+\\s*\\n?/,'').trim();
                        if (m && name.length > 1 && !horses[m[1]]) {
                            const row = a.closest('tr');
                            const jLink = row?.querySelector('a[href*="/jockey/"]');
                            horses[m[1]] = {name, horse_id: m[1], jockey: jLink?.textContent.trim() || ''};
                        }
                    });
                    return Object.values(horses);
                }""")

                race_meta = page.evaluate("""() => {
                    const metaEl = document.querySelector('.RaceData01') ||
                                   document.querySelector('.RaceData') ||
                                   document.querySelector('[class*="RaceData"]');
                    const meta = metaEl?.textContent || '';
                    const titleEl = document.querySelector('h2.RaceName') ||
                                    document.querySelector('.RaceName') ||
                                    document.querySelector('[class*="RaceName"]');
                    const title = titleEl?.textContent?.trim() || '';
                    const bodyText = document.body.innerText.slice(0, 1000);
                    return {meta, title, bodyText};
                }""")

                odds_url = f"{BASE_URL}/odds/index.html?race_id={rid}"
                page.goto(odds_url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)

                odds_data = page.evaluate("""() => {
                    const rows = Array.from(document.querySelectorAll('tr')).filter(r => r.querySelectorAll('td').length >= 4);
                    const data = [];
                    rows.forEach(r => {
                        const tds = Array.from(r.querySelectorAll('td')).map(c => c.textContent.trim());
                        const link = r.querySelector('a[href*="/horse/"]');
                        if (link && /^\\d+$/.test(tds[0])) {
                            const m = link.href.match(/\\/horse\\/(\\d{10})/);
                            const oddsCell = tds.find(t => /^\\d+\\.\\d$/.test(t));
                            data.push({
                                pop: parseInt(tds[0]),
                                name: link.textContent.trim(),
                                odds: oddsCell ? parseFloat(oddsCell) : parseFloat(tds[tds.length-1]),
                                horse_id: m ? m[1] : null
                            });
                        }
                    });
                    return data.filter(d => d.pop && d.odds);
                }""")

                meta_text = (race_meta.get('meta','') + ' ' +
                             race_meta.get('title','') + ' ' +
                             race_meta.get('bodyText',''))
                dist_m = re.search(r'(\d{4})m', meta_text) or re.search(r'(\d{3})m', meta_text)
                course_m = 'ダート' if 'ダート' in meta_text else ('芝' if '芝' in meta_text else 'ダート')
                dist = int(dist_m.group(1)) if dist_m else r.get('dist_hint', 0)
                race_name = race_meta.get('title','').strip() or f"{r['rnum']}R"

                detail = {
                    'race_id':   rid,
                    'race_name': race_name,
                    'dist':      dist,
                    'course':    course_m,
                    'n_horses':  len(odds_data),
                    'rnum':      r['rnum'],
                    'horses':    horses,
                    'odds':      odds_data,
                }
                race_details.append(detail)
                print(f"[fetch] {rid[-4:]}R {race_name} {course_m}{dist}m {len(odds_data)}頭")
                time.sleep(0.5)
            except Exception as e:
                print(f"[fetch] ERROR {rid}: {e}")

        browser.close()

    out_path = f"data/races_all_{date_str}.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({'date': date_str, 'all_races': race_details}, f, ensure_ascii=False, indent=2)
    print(f"[fetch] 保存: {out_path} ({len(race_details)}レース)")

if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else '20260405'
    fetch_all(date_arg)

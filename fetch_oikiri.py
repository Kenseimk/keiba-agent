"""
fetch_oikiri.py  追い切りデータ取得
=====================================================
netkeiba から各馬の調教評価・タイムを取得し、
data/oikiri_YYYYMMDD.json に保存する。

【無料モード】(デフォルト)
  oikiri.html から S/A/B/C/D 評価のみ取得

【プレミアムモード】(--premium / .env に認証情報設定)
  training.html から調教コース・タイム・強度を取得
  .env に以下を設定:
    NETKEIBA_LOGIN_ID=your_email@example.com
    NETKEIBA_PASSWORD=your_password

格納形式:
  horse_name → {
    'eval':    'S'/'A'/'B'/'C'/'D',
    'course':  'ウッド'/'坂路'/'ポリ'/'ダート'/'芝',
    'time_5f': float or None,
    'time_4f': float or None,
    'time_3f': float or None,
    'time_1f': float or None,
    'style':   '馬なり'/'一杯'/'強め'/'軽め',
    'date':    'YYYY/MM/DD',
    'source':  'free' or 'premium',
  }

使い方:
  python fetch_oikiri.py --date 20260405
  python fetch_oikiri.py --date 20260405 --premium
  python fetch_oikiri.py --date 20260405 --race_ids 202606030411
"""

import os, re, json, time, datetime, argparse
from playwright.sync_api import sync_playwright

BASE_URL  = "https://race.netkeiba.com"
DB_URL    = "https://db.netkeiba.com"
LOGIN_URL = "https://regist.netkeiba.com/account/?pid=login"
DATA_DIR  = 'data'
SLEEP_SEC = 2.0

# 追い切り評価テキスト → 評価コード
EVAL_MAP = {
    'S': 'S', 'A': 'A', 'B': 'B', 'C': 'C', 'D': 'D',
    '◎': 'S', '○': 'A', '▲': 'B', '△': 'C',
}

# 評価コード → uscore因子スコア (0〜10)
EVAL_SCORE = {
    'S': 9.5,
    'A': 7.5,
    'B': 6.0,
    'C': 4.0,
    'D': 2.5,
}

# 調教コース → カテゴリ
COURSE_CATEGORY = {
    'ウッド': 'wood', 'W': 'wood', '南W': 'wood', '北W': 'wood', 'CW': 'wood',
    '坂路': 'slope',
    'ポリ': 'poly', 'ポリトラック': 'poly', 'P': 'poly',
    'ダート': 'dirt',
    '芝': 'turf',
}

# ────────────────────────────────────────────────
# 調教コース別の基準タイム (3F秒) ※上位馬の目安
# ────────────────────────────────────────────────
COURSE_STD_3F = {
    'wood':  36.5,   # ウッド 3F 36.5秒以下 = 優秀
    'slope': 38.5,   # 坂路 3F 38.5秒以下 = 優秀
    'poly':  37.0,
    'dirt':  38.0,
    'turf':  36.0,
}


def _load_env():
    """プロジェクトの .env からログイン情報を読み込む"""
    env = {}
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    # 環境変数からも取得（上書き）
    for k in ('NETKEIBA_LOGIN_ID', 'NETKEIBA_PASSWORD'):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def netkeiba_login(page, login_id: str, password: str) -> bool:
    """
    netkeiba にログインする。
    成功時 True、失敗時 False を返す。
    """
    print('  netkeiba ログイン中...', end=' ', flush=True)
    page.goto(LOGIN_URL, wait_until='domcontentloaded')
    page.wait_for_timeout(1500)

    try:
        page.fill('input[name="login_id"]', login_id)
        page.fill('input[name="pswd"]', password)
        page.click('input[type="image"], input[type="submit"]')
        page.wait_for_timeout(2500)

        # ログイン成功判定: regist ページから離れていれば成功
        current = page.url
        if 'regist.netkeiba.com' in current and 'login' in current:
            print('失敗 (認証エラー)')
            return False
        print(f'成功 ({current[:50]})')
        return True
    except Exception as e:
        print(f'例外: {e}')
        return False


def parse_training_detail(page, horse_id: str, horse_name: str) -> dict:
    """
    training.html から最終追い切り情報を取得。
    プレミアムログイン済みの page を渡す。

    戻り値: {
        'course', 'time_5f', 'time_4f', 'time_3f', 'time_1f',
        'style', 'date', 'source': 'premium'
    }
    """
    url = f'{DB_URL}/horse/training.html?id={horse_id}'
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(2000)

    # ページがプレミアム案内にリダイレクトされた場合はスキップ
    if 'premium' in page.url or 'regist' in page.url:
        return {}

    data = page.evaluate(r"""() => {
        // 追い切りテーブルを探す
        const tables = Array.from(document.querySelectorAll('table'));
        let trainTable = null;
        for (const t of tables) {
            const text = t.textContent;
            if (text.includes('コース') && (text.includes('3F') || text.includes('タイム'))) {
                trainTable = t;
                break;
            }
        }
        if (!trainTable) return null;

        // ヘッダー行でカラム位置を特定
        const headerRow = trainTable.querySelector('tr');
        const headers = Array.from(headerRow?.querySelectorAll('th,td') || []).map(c => c.textContent.trim());

        // データ行 (最初のデータ行 = 最新追い切り)
        const dataRows = Array.from(trainTable.querySelectorAll('tr')).slice(1);
        if (dataRows.length === 0) return {headers, rows: []};

        const rows = dataRows.slice(0, 3).map(row => {
            const cells = Array.from(row.querySelectorAll('td,th')).map(c => c.textContent.trim());
            return cells;
        });

        return {headers, rows};
    }""")

    if not data or not data.get('rows'):
        return {}

    headers = data.get('headers', [])
    first_row = data['rows'][0] if data['rows'] else []

    # カラムインデックスを動的に特定
    def find_col(keywords):
        for i, h in enumerate(headers):
            if any(k in h for k in keywords):
                return i
        return -1

    col_date   = find_col(['日付', '年月日'])
    col_course = find_col(['コース', '場所'])
    col_5f     = find_col(['5F', '5ハロン'])
    col_4f     = find_col(['4F', '4ハロン'])
    col_3f     = find_col(['3F', '3ハロン'])
    col_1f     = find_col(['1F', '1ハロン', 'ラスト'])
    col_style  = find_col(['追', '方法', '強度', '状態'])

    def safe_get(row, idx):
        return row[idx] if 0 <= idx < len(row) else ''

    def parse_time(s):
        s = s.strip()
        if not s or s in ('-', '**'):
            return None
        # "1:XX.X" 形式 → 秒
        m = re.match(r'(\d+):(\d+)\.(\d)', s)
        if m:
            return int(m.group(1)) * 60 + float(f'{m.group(2)}.{m.group(3)}')
        # "XX.X" 形式
        try:
            return float(s)
        except ValueError:
            return None

    def parse_course(s):
        s = s.strip()
        for key in COURSE_CATEGORY:
            if key in s:
                return key
        return s

    return {
        'date':    safe_get(first_row, col_date),
        'course':  parse_course(safe_get(first_row, col_course)),
        'time_5f': parse_time(safe_get(first_row, col_5f)),
        'time_4f': parse_time(safe_get(first_row, col_4f)),
        'time_3f': parse_time(safe_get(first_row, col_3f)),
        'time_1f': parse_time(safe_get(first_row, col_1f)),
        'style':   safe_get(first_row, col_style),
        'source':  'premium',
    }


def parse_oikiri_page(page, race_id: str) -> dict:
    """
    追い切りページ (oikiri.html) から S/A/B/C/D 評価を取得。
    戻り値: {horse_name: {'eval': str, 'source': 'free'}}
    """
    url = f'{BASE_URL}/race/oikiri.html?race_id={race_id}'
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(int(SLEEP_SEC * 1000))

    data = page.evaluate(r"""() => {
        const results = {};
        const rows = Array.from(document.querySelectorAll('tr'));
        rows.forEach(row => {
            const horseLink = row.querySelector('a[href*="/horse/"]');
            if (!horseLink) return;
            const name = horseLink.textContent.trim();
            if (!name) return;

            const tds = Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim());
            // horse_id
            const idMatch = horseLink.href.match(/\/horse\/(\d{10})/);
            const horse_id = idMatch ? idMatch[1] : '';

            // 評価 (S/A/B/C/D)
            let evalChar = '';
            for (const td of tds) {
                if (/^[SABCD]$/.test(td)) { evalChar = td; break; }
            }
            if (!results[name]) {
                results[name] = {eval: evalChar, horse_id: horse_id};
            }
        });
        return results;
    }""")

    result = {}
    for name, info in (data or {}).items():
        result[name] = {
            'eval':     EVAL_MAP.get(info.get('eval', ''), ''),
            'horse_id': info.get('horse_id', ''),
            'course':   '',
            'time_5f':  None,
            'time_4f':  None,
            'time_3f':  None,
            'time_1f':  None,
            'style':    '',
            'date':     '',
            'source':   'free',
        }
    return result


def fetch_oikiri_for_date(
    date_str: str,
    race_ids:  list = None,
    premium:   bool = False,
) -> dict:
    """
    指定日付の全レースの追い切りデータを取得。
    premium=True の場合は .env の認証情報でログインして調教タイムも取得。

    戻り値: {race_id: {horse_name: {...}}}
    """
    if race_ids is None:
        json_path = os.path.join(DATA_DIR, f'races_{date_str}.json')
        if not os.path.exists(json_path):
            print(f'[ERROR] {json_path} が見つかりません')
            return {}
        with open(json_path, encoding='utf-8') as f:
            day_data = json.load(f)
        races_list = day_data.get('all_races', []) + day_data.get('candidates', [])
        race_ids = list({r['race_id'] for r in races_list})

    print(f'対象レース: {len(race_ids)} R  (プレミアム: {premium})')

    # horse_id マップ: race JSON から取得
    horse_id_map: dict[str, str] = {}
    json_path = os.path.join(DATA_DIR, f'races_{date_str}.json')
    if os.path.exists(json_path):
        with open(json_path, encoding='utf-8') as f:
            day_data = json.load(f)
        for r in day_data.get('all_races', []) + day_data.get('candidates', []):
            for h in r.get('horses', []):
                if h.get('horse_id') and h.get('name'):
                    horse_id_map[h['name']] = h['horse_id']

    all_oikiri: dict[str, dict] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()

        # プレミアムログイン
        logged_in = False
        if premium:
            env = _load_env()
            login_id = env.get('NETKEIBA_LOGIN_ID', '')
            password  = env.get('NETKEIBA_PASSWORD', '')
            if not login_id or login_id == 'your_email@example.com':
                print('[ERROR] .env に NETKEIBA_LOGIN_ID / NETKEIBA_PASSWORD を設定してください')
                browser.close()
                return {}
            logged_in = netkeiba_login(page, login_id, password)
            if not logged_in:
                print('[ERROR] ログイン失敗。無料モードにフォールバックします')
                premium = False

        # レースごとに評価を取得
        for race_id in sorted(race_ids):
            print(f'  {race_id} 評価取得...', end=' ', flush=True)
            try:
                oikiri = parse_oikiri_page(page, race_id)
                n_eval = sum(1 for v in oikiri.values() if v.get('eval'))
                print(f'{len(oikiri)}頭 (評価:{n_eval}頭)', flush=True)
                all_oikiri[race_id] = oikiri
            except Exception as e:
                print(f'ERROR: {e}')
                all_oikiri[race_id] = {}
            time.sleep(SLEEP_SEC)

        # プレミアムモード: 各馬の詳細調教タイム取得
        if premium and logged_in:
            print()
            print('調教タイム取得中 (プレミアム)...')
            for race_id, horses in all_oikiri.items():
                for name, info in horses.items():
                    hid = info.get('horse_id') or horse_id_map.get(name, '')
                    if not hid:
                        continue
                    try:
                        detail = parse_training_detail(page, hid, name)
                        if detail:
                            info.update(detail)
                            t3f = detail.get('time_3f')
                            print(f'    {name:18} {detail.get("course",""):6} '
                                  f'3F={t3f if t3f else "-":5} '
                                  f'style={detail.get("style",""):6}')
                        time.sleep(1.5)
                    except Exception as e:
                        print(f'    {name}: ERROR {e}')

        browser.close()

    return all_oikiri


def save_oikiri(date_str: str, oikiri_data: dict):
    out_path = os.path.join(DATA_DIR, f'oikiri_{date_str}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({'date': date_str, 'races': oikiri_data}, f, ensure_ascii=False, indent=2)
    print(f'\n保存: {out_path}')


def load_oikiri(date_str: str) -> dict:
    """{race_id: {horse_name: {...}}} を返す"""
    path = os.path.join(DATA_DIR, f'oikiri_{date_str}.json')
    if not os.path.exists(path):
        return {}
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('races', {})


def build_oikiri_db(date_str: str) -> dict:
    """{horse_name: oikiri_info} の flat dict を返す"""
    races = load_oikiri(date_str)
    db: dict[str, dict] = {}
    for race_id, horses in races.items():
        for name, info in horses.items():
            if name not in db:
                db[name] = info
    return db


def oikiri_score(horse_name: str, oikiri_db: dict) -> float:
    """
    馬名から追い切りスコアを返す (0〜10)。

    プレミアムデータあり → タイム+強度+評価の総合スコア
    無料データのみ      → 評価コードのみ
    """
    if not oikiri_db:
        return 5.0
    info = oikiri_db.get(horse_name)
    if not info:
        return 5.0

    # ── ベーススコア: 評価コード ──────────────────────
    eval_code = info.get('eval', '')
    base = EVAL_SCORE.get(eval_code, 5.0)

    # ── プレミアムデータがある場合はタイムで補正 ─────
    if info.get('source') == 'premium' and info.get('time_3f') is not None:
        course_raw = info.get('course', '')
        cat = COURSE_CATEGORY.get(course_raw, '')
        t3f = info.get('time_3f')
        t1f = info.get('time_1f')
        style = info.get('style', '')

        if cat and t3f is not None:
            std = COURSE_STD_3F.get(cat, 38.0)
            diff = t3f - std   # 負=速い, 正=遅い
            if diff <= -2.0:
                base = min(base + 2.0, 10.0)
            elif diff <= -1.0:
                base = min(base + 1.5, 10.0)
            elif diff <= 0.0:
                base = min(base + 0.5, 10.0)
            elif diff <= 1.0:
                pass  # 変化なし
            else:
                base = max(base - 0.5, 0.0)

        # ラスト1F補正 (速い上がり)
        if t1f is not None:
            if t1f <= 11.5:
                base = min(base + 0.5, 10.0)
            elif t1f >= 12.5:
                base = max(base - 0.3, 0.0)

        # 強度補正
        if '一杯' in style or '強め' in style:
            base = min(base + 0.3, 10.0)
        elif '軽め' in style:
            base = max(base - 0.5, 0.0)

    return base


def main():
    parser = argparse.ArgumentParser(description='追い切りデータ取得')
    parser.add_argument('--date', default=datetime.date.today().strftime('%Y%m%d'),
                        help='取得日付 YYYYMMDD')
    parser.add_argument('--race_ids', nargs='*', default=None,
                        help='取得するrace_id (省略時は当日JSON全レース)')
    parser.add_argument('--premium', action='store_true',
                        help='プレミアムログインで調教タイムも取得')
    args = parser.parse_args()

    print(f'=== 追い切りデータ取得: {args.date} ===')
    oikiri = fetch_oikiri_for_date(args.date, args.race_ids, premium=args.premium)

    if oikiri:
        save_oikiri(args.date, oikiri)
        # サマリー表示
        for race_id, horses in oikiri.items():
            print(f'\n{race_id}:')
            for name, info in sorted(horses.items()):
                t3f = info.get('time_3f')
                print(f'  {name:20} eval={info.get("eval") or "-":2} '
                      f'course={info.get("course") or "-":6} '
                      f'3F={t3f if t3f else "-"} '
                      f'style={info.get("style") or "-"}')
    else:
        print('データが取得できませんでした')


if __name__ == '__main__':
    main()

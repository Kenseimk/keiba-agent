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
    # full-width variants (netkeiba HTML uses 全角)
    'Ｗ': 'wood', '美Ｗ': 'wood', '南Ｗ': 'wood', '北Ｗ': 'wood', 'ＣＷ': 'wood',
    '坂路': 'slope',
    'ポリ': 'poly', 'ポリトラック': 'poly', 'P': 'poly', 'Ｐ': 'poly',
    '美Ｐ': 'poly', 'ＤＰ': 'poly',
    'ダート': 'dirt',
    '芝': 'turf',
}


def _normalize_course(raw: str) -> str:
    """コース名を正規化: 改行・余分な空白・一番時計などを除去して最初のトークンを返す"""
    if not raw:
        return ''
    # 改行やスペースで分割して最初のトークン
    token = raw.strip().split('\n')[0].strip()
    return token

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

        # login_id フォームの中にある image ボタンをクリック
        submit_btn = page.locator('input[name="login_id"]').locator(
            'xpath=ancestor::form//input[@type="image" or @type="submit"]'
        ).first
        with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
            submit_btn.click()
        page.wait_for_timeout(3000)

        # nkauth クッキーがあればログイン成功
        cookies = {c['name']: c['value'] for c in page.context.cookies()}
        if 'nkauth' in cookies:
            print('成功')
            return True

        # フォールバック: URL チェック
        if 'regist.netkeiba.com' not in page.url or 'login' not in page.url:
            print(f'成功 (URL: {page.url[:50]})')
            return True

        print('失敗 (認証クッキーなし)')
        return False
    except Exception as e:
        print(f'例外: {e}')
        return False


def parse_training_detail(page, horse_id: str, horse_name: str) -> dict:
    """
    training.html から最終追い切り情報を取得。
    プレミアムログイン済みの page を渡す。

    実際のカラム構成:
      日付 | コース | 馬場 | 乗り役 | 調教タイム | 位置 | 脚色 | 評価 | 映像

    調教タイム列は改行区切りで複数タイムが入る:
      (7)99.1 / 67.8 / 52.3 / 37.4 / 11.6
      → 後ろから: [-1]=1F, [-2]=3F, [-3]=4F, [-4]=5F

    戻り値: {
        'course', 'time_5f', 'time_4f', 'time_3f', 'time_1f',
        'style', 'eval_text', 'date', 'source': 'premium'
    }
    """
    url = f'{DB_URL}/horse/training.html?id={horse_id}'
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(2000)

    if 'premium' in page.url or 'regist' in page.url:
        return {}

    data = page.evaluate(r"""() => {
        // class="race_table_01 nk_tb_common" のテーブルを取得
        const tables = Array.from(document.querySelectorAll('table.race_table_01'));
        if (!tables.length) return null;

        const trainTable = tables[0];
        const rows = Array.from(trainTable.querySelectorAll('tr'));

        // ヘッダー確認
        const headers = Array.from(rows[0]?.querySelectorAll('th,td') || [])
                            .map(c => c.textContent.trim());

        // 最新データ行を探す (短評行をスキップ)
        let dataRow = null;
        for (let i = 1; i < rows.length; i++) {
            const cells = Array.from(rows[i].querySelectorAll('td'));
            // 日付パターン (YYYY/MM/DD) があればデータ行
            if (cells.length >= 4 && /\d{4}\/\d{2}\/\d{2}/.test(cells[0]?.textContent || '')) {
                dataRow = cells.map(c => c.textContent.trim());
                break;
            }
        }

        return dataRow ? {headers, row: dataRow} : {headers, row: null};
    }""")

    if not data or not data.get('row'):
        return {}

    headers = data.get('headers', [])
    row     = data['row']

    # カラムインデックス (ヘッダーから動的に特定)
    def find_col(keywords):
        for i, h in enumerate(headers):
            if any(k in h for k in keywords):
                return i
        return -1

    col_date   = find_col(['日付'])
    col_course = find_col(['コース'])
    col_time   = find_col(['調教タイム', 'タイム'])
    col_style  = find_col(['脚色'])
    col_eval   = find_col(['評価'])

    def safe(idx):
        return row[idx] if 0 <= idx < len(row) else ''

    # 調教タイム列から数値を全て抽出
    time_text = safe(col_time)
    nums = [float(m) for m in re.findall(r'\d+\.\d', time_text)]
    # 末尾から: [-1]=1F, [-2]=3F, [-3]=4F, [-4]=5F
    def pick(idx):
        try: return nums[idx]
        except: return None

    def parse_course(s):
        s = s.strip()
        # 美W→ウッド, 栗W→ウッド, 美坂→坂路 etc.
        if 'W' in s or 'ｗ' in s or 'ウッド' in s: return 'ウッド'
        if '坂' in s:                               return '坂路'
        if 'ポリ' in s or 'P' in s:                return 'ポリ'
        if 'ダート' in s or 'ダ' in s:             return 'ダート'
        if '芝' in s:                               return '芝'
        return s

    return {
        'date':      safe(col_date).split('(')[0].strip(),
        'course':    parse_course(safe(col_course)),
        'time_5f':   pick(-4) if len(nums) >= 4 else None,
        'time_4f':   pick(-3) if len(nums) >= 3 else None,
        'time_3f':   pick(-2) if len(nums) >= 2 else None,
        'time_1f':   pick(-1) if len(nums) >= 1 else None,
        'style':     safe(col_style),
        'eval_text': safe(col_eval),
        'source':    'premium',
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

    # プレミアムの eval_text でも評価 (S/A/B/C/D が取れていない場合の補完)
    if not eval_code and info.get('source') == 'premium':
        eval_text = info.get('eval_text', '')
        if any(k in eval_text for k in ['絶好調', '仕上がり十', '完璧', '一番時計']):
            base = 9.0
        elif any(k in eval_text for k in ['仕上がる', '仕上良好', '好調持続', '動き軽快', '好気配', '上々', '出来は良', '出来良']):
            base = 7.5
        elif any(k in eval_text for k in ['キビキビ', '順調', '平均以上', '前走並み', '出来は普通']):
            base = 6.0
        elif any(k in eval_text for k in ['平凡', '標準', '物足りな', '平行線']):
            base = 4.5
        elif any(k in eval_text for k in ['気配一息', '一息', '元気なし', '疲れ']):
            base = 3.0

    # ── プレミアムデータがある場合はタイムで補正 ─────
    if info.get('source') == 'premium' and info.get('time_3f') is not None:
        course_raw = _normalize_course(info.get('course', ''))
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

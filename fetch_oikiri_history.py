# -*- coding: utf-8 -*-
"""
fetch_oikiri_history.py  過去レース追い切りデータ一括取得
=============================================================
CSVから race_id を取得し、netkeiba oikiri.html を月ごとに保存。

保存先: data/oikiri_{YYYYMM}.json
  { "month": "YYYYMM", "races": { race_id: { horse_name: {...} } } }

使い方:
  python fetch_oikiri_history.py --start 202501 --end 202603   # ブラインド期間
  python fetch_oikiri_history.py --start 202301 --end 202412   # チューニング期間
  python fetch_oikiri_history.py --test 202501010101           # 1レース動作確認
  python fetch_oikiri_history.py --start 202501 --end 202603 --premium  # 調教タイムも取得

注意:
  - 1レース約2秒, 3386R≈1.9時間 (ブラインド期間)
  - 既存ファイルの未取得race_idのみ追加取得 (resumable)
  - --premium は .env に NETKEIBA_LOGIN_ID / NETKEIBA_PASSWORD が必要
"""
import os, re, csv, json, glob, time, argparse, sys, random
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR  = 'data'
SLEEP_SEC = 3.0

# User-Agent ローテーション (ban対策)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def jitter_sleep(base_sec: float, jitter: float = 0.5):
    """base ± jitter秒のランダムスリープ"""
    t = base_sec + random.uniform(-jitter, jitter)
    time.sleep(max(1.0, t))

from fetch_oikiri import parse_oikiri_page, parse_training_detail, netkeiba_login, _load_env, EVAL_MAP


def get_race_ids_for_month(ym: str) -> list[str]:
    """raceresults_{ym}.csv からユニークなrace_idリストを返す"""
    fpath = os.path.join(DATA_DIR, f'raceresults_{ym}.csv')
    if not os.path.exists(fpath):
        return []
    ids = set()
    with open(fpath, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            rid = row.get('race_id', '').strip()
            if rid:
                ids.add(rid)
    return sorted(ids)


def load_monthly_oikiri(ym: str) -> dict:
    """既存の月別oikiriファイルを読み込む。なければ {}"""
    path = os.path.join(DATA_DIR, f'oikiri_{ym}.json')
    if not os.path.exists(path):
        return {}
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    # 両フォーマット対応: {'races': {...}} または直接 {race_id: {...}}
    if 'races' in data:
        return data['races']
    return data


def save_monthly_oikiri(ym: str, races: dict):
    """月別oikiriデータを保存"""
    path = os.path.join(DATA_DIR, f'oikiri_{ym}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'month': ym, 'races': races}, f, ensure_ascii=False, indent=2)
    return path


def fetch_month(ym: str, get_page_fn, premium: bool = False, logged_in: bool = False,
                sleep_sec: float = SLEEP_SEC, restart_every: int = 50) -> tuple[dict, callable]:
    """
    1ヶ月分のrace_idを取得してoikiriデータを返す。
    get_page_fn: 新しいpageオブジェクトを返す関数 (クラッシュ時に再生成)
    戻り値: (races dict, 更新後の get_page_fn)
    """
    all_ids = get_race_ids_for_month(ym)
    if not all_ids:
        print(f'  [{ym}] CSVなし → スキップ')
        return {}, get_page_fn

    # 既存データロード (有効データのみ: 1頭以上いる race_id)
    existing_raw = load_monthly_oikiri(ym)
    existing = {rid: v for rid, v in existing_raw.items() if v}
    done_ids = set(existing.keys())
    todo_ids = [rid for rid in all_ids if rid not in done_ids]

    print(f'  [{ym}] 全{len(all_ids)}R / 未取得{len(todo_ids)}R / 取得済{len(done_ids)}R')

    if not todo_ids:
        return existing, get_page_fn

    races = dict(existing)
    page = get_page_fn()
    races_since_restart = 0

    for i, race_id in enumerate(todo_ids):
        # 予防的再起動 (restart_every レースごと) + UA変更
        if races_since_restart >= restart_every:
            pause = random.uniform(8, 15)
            print(f'  [予防再起動] {races_since_restart}レース完了 → {pause:.0f}秒休憩後UA変更して再起動', flush=True)
            save_monthly_oikiri(ym, races)
            try: page.context.browser.close()
            except Exception: pass
            time.sleep(pause)
            page = get_page_fn()
            races_since_restart = 0

        try:
            oikiri = parse_oikiri_page(page, race_id)
            n_eval = sum(1 for v in oikiri.values() if v.get('eval'))
            races[race_id] = oikiri
            races_since_restart += 1
            print(f'    ({i+1}/{len(todo_ids)}) {race_id}: {len(oikiri)}頭 eval:{n_eval}頭', flush=True)
        except Exception as e:
            err_str = str(e)
            print(f'    ({i+1}/{len(todo_ids)}) {race_id}: ERROR {err_str[:60]}')
            races[race_id] = {}
            # クラッシュ検出 → 即再起動
            if any(k in err_str.lower() for k in ['crash', 'closed', 'target', 'disconnected']):
                print(f'  [CRASH検出] ブラウザ再起動します。')
                save_monthly_oikiri(ym, races)
                try: page.context.browser.close()
                except Exception: pass
                time.sleep(3)
                page = get_page_fn()
                races_since_restart = 0
                continue

        # 連続5回 0頭ならIPブロックとみなして中断
        recent = [v for v in list(races.values())[-5:]]
        if len(recent) >= 5 and all(len(v) == 0 for v in recent):
            print(f'  [BAN検出] 連続5回データなし → IPブロックの可能性。このレースはスキップして続行。', flush=True)
            save_monthly_oikiri(ym, races)
            # クラッシュでなくBANなのでブラウザ再起動しても無意味 → 長めに待って続行
            wait = random.uniform(30, 60)
            print(f'  [{ym}] {wait:.0f}秒待機後に続行...', flush=True)
            time.sleep(wait)
            continue

        jitter_sleep(sleep_sec)

        # 20Rごとに少し長めの休憩 (ban対策)
        if (i + 1) % 20 == 0:
            extra = random.uniform(3, 8)
            print(f'  [小休憩] {i+1}R完了 → {extra:.0f}秒追加休憩', flush=True)
            time.sleep(extra)

        if (i + 1) % 10 == 0:
            save_monthly_oikiri(ym, races)

    save_monthly_oikiri(ym, races)
    return races, get_page_fn


def main():
    parser = argparse.ArgumentParser(description='過去追い切りデータ一括取得')
    parser.add_argument('--start', default='202501',
                        help='開始年月 YYYYMM (default: 202501)')
    parser.add_argument('--end', default='202603',
                        help='終了年月 YYYYMM (default: 202603)')
    parser.add_argument('--test', default=None, metavar='RACE_ID',
                        help='1レースだけ取得してoikiri.htmlの動作確認')
    parser.add_argument('--premium', action='store_true',
                        help='プレミアムモード (調教タイムも取得)')
    parser.add_argument('--sleep', type=float, default=SLEEP_SEC,
                        help=f'レース間スリープ秒 (default: {SLEEP_SEC})')
    args = parser.parse_args()

    from playwright.sync_api import sync_playwright

    sleep_sec = args.sleep

    # 月リスト生成
    if args.test:
        months = []
    else:
        y0, m0 = int(args.start[:4]), int(args.start[4:])
        y1, m1 = int(args.end[:4]),   int(args.end[4:])
        months = []
        y, m = y0, m0
        while (y, m) <= (y1, m1):
            months.append(f'{y:04d}{m:02d}')
            m += 1
            if m > 12:
                m = 1; y += 1

    total_months = len(months)
    print(f'=== 追い切り履歴取得 ===')
    if args.test:
        print(f'テストモード: {args.test}')
    else:
        print(f'期間: {args.start} 〜 {args.end} ({total_months}ヶ月)')
    print(f'プレミアム: {args.premium}')

    # GitHub Actions 並列実行時のランダム遅延 (同一時刻起動で集中しないよう分散)
    if os.environ.get('GITHUB_ACTIONS'):
        import socket
        ip = socket.gethostbyname(socket.gethostname())
        wait_init = random.uniform(5, 30)
        print(f'[GitHub Actions] IP={ip}  初期待機: {wait_init:.0f}秒')
        time.sleep(wait_init)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=random_ua(),
            locale='ja-JP',
            extra_http_headers={'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8'},
        )
        page = context.new_page()

        # ログイン
        logged_in = False
        if args.premium:
            env = _load_env()
            lid = env.get('NETKEIBA_LOGIN_ID', '')
            pwd = env.get('NETKEIBA_PASSWORD', '')
            if not lid or lid == 'your_email@example.com':
                print('[ERROR] .env に認証情報を設定してください')
                browser.close()
                return
            logged_in = netkeiba_login(page, lid, pwd)
            if not logged_in:
                print('[WARN] ログイン失敗 → 無料モードで続行')
                args.premium = False

        # テストモード
        if args.test:
            print(f'\n{args.test} のoikiriページを取得中...')
            oikiri = parse_oikiri_page(page, args.test)
            if oikiri:
                print(f'取得成功: {len(oikiri)}頭')
                for name, info in list(oikiri.items())[:5]:
                    print(f'  {name}: eval={info.get("eval","-")} horse_id={info.get("horse_id","")}')
            else:
                print('データなし (過去レースのoikiri.htmlは取得不可能な可能性あり)')
            browser.close()
            return

        def make_page():
            ua = random_ua()
            br = p.chromium.launch(headless=True)
            return br.new_context(
                user_agent=ua,
                locale='ja-JP',
                extra_http_headers={'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8'},
            ).new_page()

        # 月ごとに取得
        get_page_fn = make_page
        for i, ym in enumerate(months):
            print(f'\n[{i+1}/{total_months}] {ym} 処理中...')
            try:
                races, get_page_fn = fetch_month(
                    ym, make_page, premium=args.premium,
                    logged_in=logged_in, sleep_sec=sleep_sec,
                    restart_every=50)
            except Exception as e:
                print(f'  [{ym}] 予期しないエラー: {e}')
                races = {}

            total_horses = sum(len(v) for v in races.values())
            print(f'  保存済: {len(races)}R, {total_horses}頭')
            # 月間休憩 (ban対策: 次の月の前に30〜60秒待機)
            if i < total_months - 1:
                pause = random.uniform(30, 60)
                print(f'  [月間休憩] 次月まで {pause:.0f}秒待機...', flush=True)
                time.sleep(pause)

    print('\n=== 完了 ===')
    # サマリー
    for ym in months:
        path = os.path.join(DATA_DIR, f'oikiri_{ym}.json')
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                d = json.load(f)
            races = d.get('races', d)
            n_races  = len(races)
            n_horses = sum(len(v) for v in races.values())
            n_eval   = sum(1 for r in races.values()
                           for v in r.values() if v.get('eval'))
            print(f'  {ym}: {n_races}R {n_horses}頭 (eval付き:{n_eval}頭)')


if __name__ == '__main__':
    main()

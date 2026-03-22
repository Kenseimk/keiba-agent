"""
main.py  keiba-agent エントリポイント
Usage: python main.py <command> [date_str]
Commands:
  morning       朝の予想（scout→analyst→strategist→verifier→reporter）
  prerace       発走30分前チェック（tracker→reporter）
  evening       夜間学習（learner→tracker.record→reporter）
  cashier_end   月末精算（cashier→reporter）
  cashier_start 月初補充（cashier→controller→reporter）
  test_discord  接続テスト（reporter）
"""

import sys, json, datetime, os
from datetime import timezone, timedelta
from pathlib import Path

JST      = timezone(timedelta(hours=9))
DATA_DIR = Path('data')
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR  = Path('logs')
LOG_DIR.mkdir(exist_ok=True)


def log(msg: str, level: str = 'INFO'):
    ts = datetime.datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}][{level}] {msg}"
    print(line)
    log_file = LOG_DIR / f"{datetime.datetime.now(JST).strftime('%Y%m%d')}.log"
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def is_race_day(date: datetime.date = None) -> bool:
    d = date or datetime.datetime.now(JST).date()
    return d.weekday() in (5, 6)


# ===== MORNING =====
def run_morning(date_str: str = None):
    from agents.scout      import run_scout
    from agents.analyst    import analyze_races
    from agents.strategist import plan_all_races, format_strategy_output
    from agents.verifier   import run_verifier, format_verifier_output
    from agents.reporter   import report_morning, report_error
    from notion_store      import save_predictions

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== morning開始 {date_str} ===")

    try:
        # 1. scout: 情報収集
        races_data = run_scout(date_str)
        if not races_data.get('candidates'):
            report_morning(date_str, "本日は参加対象レースなし（条件C/A以上のレースがありませんでした）", "—", "なし")
            return

        # 2. analyst: スコア分析
        analyzed = analyze_races(races_data)
        if not analyzed:
            report_morning(date_str, "本日は参加対象レースなし（条件C/A以上のレースがありませんでした）", "—", "なし")
            return

        # 3. strategist: 馬券プラン生成
        planned = plan_all_races(analyzed)
        selector_text = format_strategy_output(planned)

        # 4. verifier: 反証
        verifier_result = run_verifier(selector_text, planned)
        verifier_text   = format_verifier_output(verifier_result)
        verdict         = verifier_result.get('verdict', 'なし')

        # 5. Notionに保存
        for p in planned:
            p['date'] = date_str
        save_predictions(date_str, planned)

        # 6. reporter: Discord通知
        report_morning(date_str, selector_text, verifier_text, verdict)
        log("=== morning完了 ===")

    except Exception as e:
        log(f"morningエラー: {e}", level='ERROR')
        report_error('morning', str(e))


# ===== PRERACE =====
def run_prerace(date_str: str = None):
    from agents.tracker   import run_prerace as tracker_prerace
    from agents.reporter  import report_prerace, report_error
    from notion_store     import load_predictions

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== prerace開始 {date_str} ===")

    try:
        predictions = load_predictions(date_str)
        results     = tracker_prerace(date_str, predictions)
        report_prerace(results)
        log("=== prerace完了 ===")
    except Exception as e:
        log(f"preraceエラー: {e}", level='ERROR')
        report_error('prerace', str(e))


# ===== EVENING =====
def run_evening(date_str: str = None):
    from agents.learner   import run_learner, format_learner_output
    from agents.tracker   import run_record
    from agents.reporter  import report_evening, report_error
    from notion_store     import load_predictions

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== evening開始 {date_str} ===")

    try:
        # 1. learner: 学習・分析
        log_entry    = run_learner(date_str)
        learner_text = format_learner_output(log_entry)
        comparisons  = log_entry.get('comparisons', [])

        # 2. tracker: DB記録
        predictions = load_predictions(date_str)
        recorded    = {}
        if predictions:
            recorded = run_record(date_str, predictions, comparisons)

        # 3. reporter: Discord通知
        report_evening(date_str, learner_text, recorded)
        log("=== evening完了 ===")

    except Exception as e:
        log(f"eveningエラー: {e}", level='ERROR')
        report_error('evening', str(e))


# ===== CASHIER_END =====
def run_cashier_end(date_str: str = None):
    from agents.cashier  import run_month_end
    from agents.reporter import report_month_end, report_error

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    year, month = int(date_str[:4]), int(date_str[4:6])
    log(f"=== cashier_end開始 {year}/{month}月 ===")

    try:
        result = run_month_end(year, month)
        report_month_end(result)
        log("=== cashier_end完了 ===")
    except Exception as e:
        log(f"cashier_endエラー: {e}", level='ERROR')
        report_error('cashier_end', str(e))


# ===== CASHIER_START =====
def run_cashier_start(date_str: str = None):
    from agents.cashier     import run_month_start
    from agents.reporter    import report_month_start, report_error

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    year, month = int(date_str[:4]), int(date_str[4:6])
    log(f"=== cashier_start開始 {year}/{month}月 ===")

    try:
        result = run_month_start(year, month)
        limits = result.get('limits', {})
        report_month_start(result, limits)

        # オッズ影響ライン到達チェック
        from agents.cashier import check_odds_impact_threshold, format_odds_impact_alert
        check = check_odds_impact_threshold(result.get('balance', 0))
        if check['reached']:
            alert_msg = format_odds_impact_alert(check)
            from agents.reporter import _send
            _send(alert_msg)
            log(f"⚠️ オッズ影響ライン到達アラート送信: 単勝上限{check['tansho']:,}円")

        log("=== cashier_start完了 ===")
    except Exception as e:
        log(f"cashier_startエラー: {e}", level='ERROR')
        report_error('cashier_start', str(e))


# ===== REFACTOR =====
def run_refactor(date_str: str = None):
    from agents.refactor import run_refactor as _refactor
    from agents.reporter import report_error
    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== refactor開始 {date_str} ===")
    try:
        result = _refactor(date_str)
        log(f"refactor完了: {result.get('improved', {}).get('verdict', '不明')}")
    except Exception as e:
        log(f"refactorエラー: {e}", level='ERROR')
        report_error('refactor', str(e))


def run_apply_params(date_str: str = None):
    """ユーザーが承認したパラメータを適用する"""
    from agents.refactor import apply_params, _load_current_params
    from agents.reporter import _send
    # 最新のNotion記録からパラメータを読み込んで適用
    # ここではシンプルに最新のparams_*.jsonを適用
    import glob
    files = sorted(glob.glob(str(DATA_DIR / 'proposed_params_*.json')))
    if not files:
        log("適用するパラメータファイルなし")
        return
    with open(files[-1]) as f:
        params = json.load(f)
    apply_params(params)
    params_json = json.dumps(params, ensure_ascii=False, indent=2)
    _send(f"✅ パラメータを適用しました\n```json\n{params_json}\n```")
    log(f"パラメータ適用完了: {params}")


# ===== TEST_DISCORD =====
def run_test_discord():
    from agents.reporter import report_test
    ok = report_test()
    log(f"Discord接続テスト: {'成功' if ok else '失敗'}")


# ===== ENTRYPOINT =====
if __name__ == '__main__':
    cmd      = sys.argv[1] if len(sys.argv) > 1 else 'morning'
    date_arg = sys.argv[2] if len(sys.argv) > 2 else None

    dispatch = {
        'morning':       lambda: run_morning(date_arg),
        'prerace':       lambda: run_prerace(date_arg),
        'evening':       lambda: run_evening(date_arg),
        'cashier_end':   lambda: run_cashier_end(date_arg),
        'cashier_start': lambda: run_cashier_start(date_arg),
        'refactor':      lambda: run_refactor(date_arg),
        'apply_params':  lambda: run_apply_params(date_arg),
        'test_discord':  lambda: run_test_discord(),
    }

    if cmd not in dispatch:
        print(f"不明なコマンド: {cmd}")
        print(f"使用可能: {', '.join(dispatch.keys())}")
        sys.exit(1)

    if cmd in ('morning', 'prerace', 'evening') and not (date_arg or is_race_day()):
        log("本日はレース開催日ではありません")
    else:
        dispatch[cmd]()

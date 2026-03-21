"""
main.py  keiba-agent メインスケジューラー

使い方:
  python main.py morning          # 朝の予想実行（手動）
  python main.py evening          # 夜の学習実行（手動）
  python main.py morning 20260322 # 日付指定
  python main.py test_discord     # Discord接続テスト

GitHub Actions / cron での自動実行:
  毎週土日祝 09:30 → morning
  毎週土日祝 21:00 → evening
"""

import sys, json, datetime, os
from datetime import timezone, timedelta
JST = timezone(timedelta(hours=9))
from pathlib import Path

# ========== 設定 ==========
DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK', '')
BUDGET          = int(os.environ.get('KEIBA_BUDGET', '10000'))
DATA_DIR        = Path('data')
LOG_DIR         = Path('logs')

# ========== 祝日チェック ==========
def is_race_day(date: datetime.date = None) -> bool:
    """土日祝日かどうかを判定"""
    d = date or datetime.datetime.now(JST).date()
    if d.weekday() >= 5:  # 土日
        return True
    try:
        import jpholiday
        return jpholiday.is_holiday(d)
    except ImportError:
        # jpholidayなければ土日のみ
        return False

# ========== 朝の処理 ==========
def run_morning(date_str: str = None):
    """朝の予想パイプライン"""
    from fetch_race import run_fetch
    from agents.selector import run_selector, format_selector_output
    from agents.verifier import run_verifier, format_verifier_output
    from discord_notify import notify_morning, notify_error

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== 朝の予想開始 {date_str} ===")

    # 1. レースデータ取得
    try:
        races_data = run_fetch(date_str)
    except Exception as e:
        msg = f"fetch_race エラー: {e}"
        log(msg, level='ERROR')
        if DISCORD_WEBHOOK:
            notify_error(DISCORD_WEBHOOK, msg)
        return

    candidates = races_data.get('candidates', [])
    if not candidates:
        log("本日は条件C候補レースなし")
        if DISCORD_WEBHOOK:
            notify_morning(DISCORD_WEBHOOK,
                          "本日は参加対象レースがありません",
                          "—", "なし", date_str)
        return

    # 2. エージェント①（選出）
    try:
        selected = run_selector(races_data, BUDGET)
        selector_text = format_selector_output(selected)
        log(f"選出: {len(selected)}レース")
    except Exception as e:
        msg = f"selector エラー: {e}"
        log(msg, level='ERROR')
        selector_text = f"エラー: {e}"
        selected = []

    # 3. エージェント②（反証）
    verifier_text = "—"
    verdict       = "なし"
    if selected and DISCORD_WEBHOOK:
        try:
            ver_result    = run_verifier(selector_text, selected)
            verifier_text = format_verifier_output(ver_result)
            verdict       = ver_result.get('verdict', '要注意')
            log(f"反証評価: {verdict}")
        except Exception as e:
            log(f"verifier エラー: {e}", level='WARN')
            verifier_text = f"反証エージェントエラー: {e}"

    # 4. 予測結果を保存
    pred_file = DATA_DIR / f'selected_{date_str}.json'
    with open(pred_file, 'w', encoding='utf-8') as f:
        json.dump(selected, f, ensure_ascii=False, indent=2, default=str)
    log(f"予測保存: {pred_file}")

    # 5. Discord通知
    if DISCORD_WEBHOOK:
        ok = notify_morning(DISCORD_WEBHOOK, selector_text, verifier_text, verdict, date_str)
        log(f"Discord通知: {'成功' if ok else '失敗'}")
    else:
        print("\n" + "="*60)
        print("【エージェント①（選出）】")
        print(selector_text)
        print("\n【エージェント②（反証）】")
        print(verifier_text)
        print(f"\n最終評価: {verdict}")

    log("=== 朝の予想完了 ===")

# ========== 夜の処理 ==========
def run_evening(date_str: str = None):
    """夜の学習パイプライン"""
    from agents.learner import run_learner, format_learner_output
    from discord_notify import notify_evening, notify_error

    date_str = date_str or datetime.datetime.now(JST).strftime('%Y%m%d')
    log(f"=== 夜間学習開始 {date_str} ===")

    try:
        log_entry     = run_learner(date_str)
        learner_text  = format_learner_output(log_entry)
    except Exception as e:
        msg = f"learner エラー: {e}"
        log(msg, level='ERROR')
        if DISCORD_WEBHOOK:
            notify_error(DISCORD_WEBHOOK, msg)
        return

    if DISCORD_WEBHOOK:
        ok = notify_evening(DISCORD_WEBHOOK, learner_text, date_str)
        log(f"Discord通知: {'成功' if ok else '失敗'}")
    else:
        print(learner_text)

    log("=== 夜間学習完了 ===")

# ========== ユーティリティ ==========
def log(msg: str, level: str = 'INFO'):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] [{level}] {msg}"
    print(line)
    LOG_DIR.mkdir(exist_ok=True)
    with open(LOG_DIR / 'agent.log', 'a') as f:
        f.write(line + '\n')

# ========== エントリポイント ==========
if __name__ == '__main__':
    cmd      = sys.argv[1] if len(sys.argv) > 1 else 'morning'
    date_arg = sys.argv[2] if len(sys.argv) > 2 else None

    if cmd == 'morning':
        if date_arg or is_race_day():
            run_morning(date_arg)
        else:
            log("本日はレース開催日ではありません")

    elif cmd == 'evening':
        if date_arg or is_race_day():
            run_evening(date_arg)
        else:
            log("本日はレース開催日ではありません")

    elif cmd == 'test_discord':
        from discord_notify import test_webhook
        if not DISCORD_WEBHOOK:
            print("DISCORD_WEBHOOK 環境変数を設定してください")
        else:
            ok = test_webhook(DISCORD_WEBHOOK)
            print("接続テスト:", "成功" if ok else "失敗")

    elif cmd == 'check_today':
        today = datetime.date.today()
        print(f"今日({today})はレース開催日: {is_race_day(today)}")

    else:
        print(__doc__)

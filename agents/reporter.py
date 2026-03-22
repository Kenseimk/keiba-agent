"""
agents/reporter.py  通知専任エージェント
全エージェントからの出力をまとめてDiscord送信
- 各エージェントが個別にDiscordを叩く構造を一元化
- フォーマット変更・通知先追加が1ファイルで完結
"""
import os, requests
from datetime import datetime, timezone, timedelta

JST            = timezone(timedelta(hours=9))
DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK', '')

MAX_LENGTH = 1900  # Discord上限2000文字に余裕を持たせる


def _send(content: str) -> bool:
    """Discord Webhookに送信（長文は分割）"""
    if not DISCORD_WEBHOOK:
        print(f"[reporter] DISCORD_WEBHOOK未設定")
        print(content)
        return False

    # 長い場合は分割送信
    chunks = []
    while len(content) > MAX_LENGTH:
        split_at = content.rfind('\n', 0, MAX_LENGTH)
        if split_at == -1:
            split_at = MAX_LENGTH
        chunks.append(content[:split_at])
        content = content[split_at:].lstrip('\n')
    chunks.append(content)

    ok = True
    for chunk in chunks:
        try:
            resp = requests.post(
                DISCORD_WEBHOOK,
                json={"content": chunk},
                timeout=10
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"[reporter] Discord送信エラー: {e}")
            ok = False
    return ok


def report_morning(date_str: str, selector_text: str,
                   verifier_text: str, verdict: str) -> bool:
    """朝の予想通知"""
    now = datetime.now(JST).strftime('%Y/%m/%d')
    header = f"**競馬予想 {now}**\n**エージェント② 最終評価: {verdict}**\n"
    body = (
        f"**エージェント①（選出）**\n{selector_text}\n\n"
        f"**エージェント②（反証）**\n{verifier_text}\n\n"
        f"購入は手動で行ってください / keiba-agent v4.0"
    )
    return _send(header + body)


def report_prerace(results: list) -> bool:
    """発走30分前チェック通知"""
    if not results:
        return True
    from agents.tracker import format_prerace_message
    ok = True
    for r in results:
        msg = format_prerace_message(r)
        if not _send(msg):
            ok = False
    return ok


def report_evening(date_str: str, learner_text: str,
                   recorded: dict = None) -> bool:
    """夜間学習レポート通知"""
    now  = datetime.now(JST).strftime('%Y/%m/%d')
    body = f"**夜間学習レポート {now}**\n{learner_text}"
    if recorded:
        body += f"\n\n📊 DB記録: {recorded.get('saved',0)}件保存 / {recorded.get('skipped',0)}件スキップ"
    return _send(body)


def report_month_end(result: dict) -> bool:
    """月末精算通知"""
    profit = result.get('profit', 0)
    sign   = '+' if profit >= 0 else ''
    msg = (
        f"## 💰 月末軍資金レポート {result['year']}年{result['month']}月\n\n"
        f"**レース数**: {result.get('race_count', 0)}件\n"
        f"**投資総額**: {result.get('invest', 0):,}円\n"
        f"**回収総額**: {result.get('payout', 0):,}円\n"
        f"**月次損益**: {sign}{profit:,}円\n\n"
        + ('✅ 軍資金追加: ' + f"{result.get('added',0):,}円（利益70%）"
           if result.get('added', 0) > 0
           else '⏭️ 今月はマイナスのため追加なし')
    )
    return _send(msg)


def report_month_start(result: dict, limits: dict) -> bool:
    """月初補充・掛け金設定通知"""
    balance = result.get('balance', 0)
    year    = result['year']
    month   = result['month']
    msg = (
        f"## 💰 月初補充 {year}年{month}月\n\n"
        f"✅ {result['added']:,}円を補充しました\n"
        f"**現在の軍資金残高**: {balance:,}円\n\n"
        f"## 💴 今月の掛け金上限\n\n"
        f"| 馬券 | 上限 |\n|------|------|\n"
        f"| 単勝   | **{limits.get('単勝',0):,}円** |\n"
        f"| 三連複 | **{limits.get('三連複',0):,}円** |\n"
        f"| 三連単 | **{limits.get('三連単',0):,}円** |\n"
    )
    return _send(msg)


def report_error(context: str, error: str) -> bool:
    """エラー通知"""
    return _send(f"⚠️ **エラー [{context}]**\n{error}")


def report_test() -> bool:
    """接続テスト"""
    return _send("keiba-agent 接続テスト OK ✅")

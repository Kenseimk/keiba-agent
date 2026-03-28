"""
predict_notify.py - 予測結果をDiscordに通知するスクリプト

GitHub Actions から呼ばれる。
predict_input.csv を読み込み、両戦略の対象馬を判定してDiscordに送信。

環境変数:
  DISCORD_WEBHOOK_URL  - Discord Webhook URL（必須）
  CURRENT_CAPITAL      - 現在の軍資金（デフォルト: 70000）
"""
import sys, io, os, requests as req_lib, argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from strategy import build_prev_history, ana_candidates, fukusho_candidates, split_by_grade
from predict import load_input

def send_discord(webhook_url, embeds):
    if not webhook_url:
        print('[WARN] DISCORD_WEBHOOK_URL が未設定')
        return
    payload = {'embeds': embeds}
    r = req_lib.post(webhook_url, json=payload, timeout=15)
    if r.status_code in (200, 204):
        print('✅ Discord送信完了')
    else:
        print(f'[WARN] Discord送信失敗: {r.status_code} {r.text[:100]}')

def build_embeds(ana_cands, fuk_cands, capital, date_str):
    import datetime
    today = date_str or datetime.date.today().strftime('%Y/%m/%d')

    embeds = []

    # ── ヘッダー ──
    embeds.append({
        'title': f'🏇 競馬予測 {today}',
        'description': f'軍資金: ¥{capital:,}',
        'color': 0x1a73e8,
    })

    # ── 穴馬複勝 ──
    seen = set()
    ana_lines = []
    total_bet_ana = 0
    for c in ana_cands:
        if c['race_id'] in seen: continue
        seen.add(c['race_id'])
        total_bet_ana += c['bet']
        ana_lines.append(
            f"**[{c['race_id']}]** {c['name']}\n"
            f"　オッズ {c['odds']:.1f}倍 / {c['pop']}番人気 / 複勝確率 {c['prob']:.1f}%\n"
            f"　→ ベット **¥{c['bet']:,}**"
        )

    embeds.append({
        'title': f'🎯 穴馬複勝戦略 ({len(seen)}レース / ¥{total_bet_ana:,})',
        'description': '\n\n'.join(ana_lines) if ana_lines else '本日の対象馬なし',
        'color': 0xe67e22,
        'footer': {'text': 'オッズ10〜30倍 / 4〜18番人気 / 複勝確率25%以上'},
    })

    # ── 隠れ末脚型複勝 ──
    seen2 = set()
    fuk_lines = []
    total_bet_fuk = 0
    for c in fuk_cands:
        if c['race_id'] in seen2: continue
        seen2.add(c['race_id'])
        total_bet_fuk += c['bet']
        adv = f' / 2位より{c["f3_adv"]:.1f}秒速い' if c.get('f3_adv', 0) > 0 else ''
        fuk_lines.append(
            f"**[{c['race_id']}]** {c['name']}\n"
            f"　オッズ {c['odds']:.1f}倍 / {c['pop']}番人気\n"
            f"　前走: 上がり{c['prev_f3rank']}位 / {c['prev_finish']}着 / "
            f"最終コーナー{c['prev_corner']}番手{adv}\n"
            f"　→ ベット **¥{c['bet']:,}**"
        )

    embeds.append({
        'title': f'⚡ 隠れ末脚型複勝戦略 ({len(seen2)}レース / ¥{total_bet_fuk:,})',
        'description': '\n\n'.join(fuk_lines) if fuk_lines else '本日の対象馬なし',
        'color': 0x2ecc71,
        'footer': {'text': '前走上がり1位 / 前走7着以下 / オッズ14〜18倍 / 6〜12番人気'},
    })

    # ── 合計 ──
    total = total_bet_ana + total_bet_fuk
    embeds.append({
        'title': '💰 本日の合計',
        'description': (
            f"穴馬: ¥{total_bet_ana:,}\n"
            f"隠れ末脚: ¥{total_bet_fuk:,}\n"
            f"**合計ベット: ¥{total:,}**\n"
            f"残り軍資金(概算): ¥{capital - total:,}"
        ),
        'color': 0x9b59b6,
    })

    return embeds

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=int,
                        default=int(os.environ.get('CURRENT_CAPITAL', '70000')))
    parser.add_argument('--input', default='predict_input.csv')
    parser.add_argument('--data',  default='data')
    parser.add_argument('--date',  default='')
    args = parser.parse_args()

    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL', '')
    capital     = args.capital

    print(f'軍資金: ¥{capital:,}')
    print('過去データ読み込み中...')

    # prev_history 構築
    prev_history, n_races, _ = build_prev_history(args.data)
    print(f'{n_races}レース / 馬{len(prev_history)}頭 読み込み完了')

    # 入力CSV確認
    if not os.path.exists(args.input):
        print(f'[INFO] {args.input} なし → 本日はレースなし')
        send_discord(webhook_url, [{
            'title': '🏇 競馬予測',
            'description': '本日はJRA開催なし',
            'color': 0x95a5a6,
        }])
        return

    # 予測実行
    races_input, race_grades = load_input(args.input)
    if not races_input:
        print('[INFO] 入力レースなし')
        send_discord(webhook_url, [{
            'title': '🏇 競馬予測',
            'description': '本日はJRA開催なし',
            'color': 0x95a5a6,
        }])
        return

    print(f'{len(races_input)}レース / {sum(len(v) for v in races_input.values())}頭 読み込み完了')

    regular_races, _ = split_by_grade(races_input, race_grades)
    ana_cands = ana_candidates(regular_races, capital)
    fuk_cands = fukusho_candidates(regular_races, prev_history, capital)

    # コンソール出力
    print(f'\n穴馬 対象: {len(set(c["race_id"] for c in ana_cands))}レース')
    for c in ana_cands:
        print(f'  [{c["race_id"]}] {c["name"]} {c["odds"]}倍 {c["pop"]}番人気 確率{c["prob"]:.1f}% ¥{c["bet"]:,}')

    print(f'\n隠れ末脚 対象: {len(set(c["race_id"] for c in fuk_cands))}レース')
    for c in fuk_cands:
        print(f'  [{c["race_id"]}] {c["name"]} {c["odds"]}倍 {c["pop"]}番人気 前走{c["prev_finish"]}着 ¥{c["bet"]:,}')

    # Discord送信
    embeds = build_embeds(ana_cands, fuk_cands, capital, args.date)
    send_discord(webhook_url, embeds)

if __name__ == '__main__':
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

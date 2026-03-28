"""
predict.py - 実戦予測スクリプト

【使い方】
1. predict_input.csv に出馬表を入力（同ディレクトリのテンプレートを参照）
2. python predict.py --capital 現在の軍資金 を実行
3. 穴馬・隠れ末脚型それぞれの対象馬とベット額が出力される

【入力CSV形式: predict_input.csv】
race_id,馬名,単勝オッズ,人気,騎手,馬体重
R01,ホウオウエース,14.5,7,武豊,480(+2)
R01,ビワハヤヒデ,22.0,9,福永,462(0)
R02,サクラバクシンオー,16.0,8,川田,470(-4)
...
※ race_id が同じ行が同一レース（複数レース可）
※ 馬体重は省略可（穴馬スコアに影響なし）
"""
import sys, io, csv, argparse, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from strategy import ANA_PARAMS, FUKUSHO_PARAMS, ana_candidates, fukusho_candidates, build_prev_history
from collections import defaultdict

# ══════════════════════════════════════════════════════════
# 入力CSV 読み込み
# ══════════════════════════════════════════════════════════
def load_input(path):
    races = defaultdict(list)
    with open(path, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                races[row['race_id'].strip()].append({
                    'name':       row['馬名'].strip(),
                    'odds':       float(row['単勝オッズ']),
                    'popularity': int(row['人気']),
                    'jockey':     row.get('騎手', '').strip(),
                    'weight':     row.get('馬体重', '').strip(),
                    'umaban':     row.get('馬番', '').strip(),
                })
            except Exception as e:
                print(f'  [WARN] 行スキップ: {row} ({e})')
    return races

# ══════════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description='競馬実戦予測スクリプト')
    parser.add_argument('--capital', type=int, required=True,
                        help='現在の軍資金（例: 70000）')
    parser.add_argument('--input', default='predict_input.csv',
                        help='入力CSVパス（デフォルト: predict_input.csv）')
    parser.add_argument('--data',   default='data',
                        help='過去データディレクトリ（デフォルト: data）')
    args = parser.parse_args()

    print('=' * 65)
    print('=== 競馬実戦予測 ===')
    print(f'軍資金: ¥{args.capital:,}')
    print('=' * 65)

    # 過去データ読み込み → prev_history 構築
    print('\n過去データ読み込み中...')
    prev_history, n_races, latest_ym = build_prev_history(args.data)
    print(f'  {n_races}レース / 馬{len(prev_history)}頭 の前走データ構築完了')
    print(f'  最新データ月: {latest_ym}')

    # 入力レース読み込み
    if not os.path.exists(args.input):
        print(f'\n[ERROR] {args.input} が見つかりません。')
        print('以下の形式でCSVを作成してください:')
        print('race_id,馬名,単勝オッズ,人気,騎手,馬体重')
        print('R01,ホウオウエース,14.5,7,武豊,480(+2)')
        sys.exit(1)

    print(f'\n入力ファイル: {args.input}')
    races_input = load_input(args.input)
    total_horses = sum(len(v) for v in races_input.values())
    print(f'  {len(races_input)}レース / {total_horses}頭 読み込み完了')

    # ── 穴馬戦略 ────────────────────────────────────────
    print('\n' + '=' * 65)
    print('【穴馬複勝戦略】')
    print('  条件: オッズ10〜30倍 / 4〜18番人気 / 8頭立て以上')
    print(f'  Kelly: 確率≥35%→3%(上限¥20,000) / ≥30%→2%(¥15,000) / その他→1.5%(¥8,000)')
    print()

    ana_cands = ana_candidates(races_input, args.capital)
    seen_ana  = set()
    if not ana_cands:
        print('  対象馬なし')
    else:
        total_bet_ana = 0
        for c in ana_cands:
            if c['race_id'] in seen_ana:
                continue  # 1レース1頭（最高確率）
            seen_ana.add(c['race_id'])
            total_bet_ana += c['bet']
            print(f"  ✅ [{c['race_id']}] {c['name']}")
            print(f"     オッズ: {c['odds']:.1f}倍  人気: {c['pop']}番人気  複勝確率: {c['prob']:.1f}%")
            print(f"     → ベット: ¥{c['bet']:,}")
        print(f'\n  合計ベット予定: ¥{total_bet_ana:,}')

    # ── 隠れ末脚型複勝戦略 ──────────────────────────────
    print('\n' + '=' * 65)
    print('【隠れ末脚型複勝戦略】')
    print('  条件: 前走上がり3F 1位 / 前走7着以下 / 今走オッズ14〜18倍 / 6〜12番人気')
    print(f'  Kelly: 軍資金の3%(上限¥15,000)')
    print()

    fuk_cands = fukusho_candidates(races_input, prev_history, args.capital)
    seen_fuk  = set()
    if not fuk_cands:
        print('  対象馬なし')
    else:
        total_bet_fuk = 0
        for c in fuk_cands:
            if c['race_id'] in seen_fuk:
                continue  # 1レース1頭
            seen_fuk.add(c['race_id'])
            total_bet_fuk += c['bet']
            adv_str = f'  前走2位より{c["f3_adv"]:.1f}秒速い' if c['f3_adv'] > 0 else ''
            print(f"  ✅ [{c['race_id']}] {c['name']}")
            print(f"     オッズ: {c['odds']:.1f}倍  人気: {c['pop']}番人気")
            print(f"     前走: 上がり{c['prev_f3rank']}位 / {c['prev_finish']}着 / "
                  f"最終コーナー{c['prev_corner']}番手 / {c['prev_field']}頭立て{adv_str}")
            print(f"     → ベット: ¥{c['bet']:,}")
        print(f'\n  合計ベット予定: ¥{total_bet_fuk:,}')

    # ── 合計 ─────────────────────────────────────────────
    print('\n' + '=' * 65)
    ana_total = sum(c['bet'] for c in ana_cands if c['race_id'] in seen_ana)
    fuk_total = sum(c['bet'] for c in fuk_cands if c['race_id'] in seen_fuk)
    print(f'【本日の合計ベット予定】')
    print(f'  穴馬:      ¥{ana_total:,}  ({len(seen_ana)}レース)')
    print(f'  隠れ末脚:  ¥{fuk_total:,}  ({len(seen_fuk)}レース)')
    print(f'  残り軍資金(概算): ¥{args.capital - ana_total - fuk_total:,}')
    print('=' * 65)

if __name__ == '__main__':
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

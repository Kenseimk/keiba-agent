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
import sys, io, csv, argparse, os, re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from strategy import (ANA_PARAMS, FUKUSHO_PARAMS,
                      ana_candidates, fukusho_candidates, build_prev_history,
                      graded_race_analysis, split_by_grade)
from race_specific import race_specific_analysis, print_race_specific
from collections import defaultdict

# ══════════════════════════════════════════════════════════
# 入力CSV 読み込み
# ══════════════════════════════════════════════════════════
def load_input(path):
    """CSVを読み込み {race_id: [horse_dict, ...]} を返す。
    各 race_id には grade キーを付与（race_meta にも格納）。
    """
    races = defaultdict(list)
    race_grades = {}  # race_id -> grade
    with open(path, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                rid = row['race_id'].strip()
                grade = row.get('grade', '').strip()
                race_grades[rid] = grade
                races[rid].append({
                    'name':       row['馬名'].strip(),
                    'odds':       float(row['単勝オッズ']),
                    'popularity': int(row['人気']),
                    'jockey':     row.get('騎手', '').strip(),
                    'weight':     row.get('馬体重', '').strip(),
                    'umaban':     row.get('馬番', '').strip(),
                    'grade':      grade,
                })
            except Exception as e:
                print(f'  [WARN] 行スキップ: {row} ({e})')
    return races, race_grades

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
    races_input, race_grades = load_input(args.input)
    total_horses = sum(len(v) for v in races_input.values())
    print(f'  {len(races_input)}レース / {total_horses}頭 読み込み完了')

    # 重賞と非重賞に分割
    regular_races, graded_races = split_by_grade(races_input, race_grades)
    if graded_races:
        grade_summary = ', '.join(f'{g}:{rid.split("_",2)[-1]}' for rid, (g, _) in graded_races.items())
        print(f'  うち重賞: {len(graded_races)}レース ({grade_summary})')

    # ══════════════════════════════════════════════════════
    # 【一般レース】汎用戦略
    # ══════════════════════════════════════════════════════
    print('\n' + '=' * 65)
    print('【一般レース】汎用戦略')
    print('=' * 65)

    # ── 穴馬戦略 ────────────────────────────────────────
    print('\n▶ 穴馬複勝戦略')
    print('  条件: オッズ10〜30倍 / 4〜18番人気 / 8頭立て以上')
    print()

    ana_cands = ana_candidates(regular_races, args.capital)
    seen_ana  = set()
    if not ana_cands:
        print('  対象馬なし')
    else:
        total_bet_ana = 0
        for c in ana_cands:
            if c['race_id'] in seen_ana:
                continue
            seen_ana.add(c['race_id'])
            total_bet_ana += c['bet']
            print(f"  ✅ [{c['race_id']}] {c['name']}")
            print(f"     オッズ: {c['odds']:.1f}倍  人気: {c['pop']}番人気  複勝確率: {c['prob']:.1f}%")
            print(f"     → ベット: ¥{c['bet']:,}")
        print(f'\n  合計ベット予定: ¥{total_bet_ana:,}')

    # ── 隠れ末脚型複勝戦略 ──────────────────────────────
    print('\n▶ 隠れ末脚型複勝戦略')
    print('  条件: 前走上がり3F 1位 / 前走7着以下 / 今走オッズ14〜18倍 / 6〜12番人気')
    print()

    fuk_cands = fukusho_candidates(regular_races, prev_history, args.capital)
    seen_fuk  = set()
    if not fuk_cands:
        print('  対象馬なし')
    else:
        total_bet_fuk = 0
        for c in fuk_cands:
            if c['race_id'] in seen_fuk:
                continue
            seen_fuk.add(c['race_id'])
            total_bet_fuk += c['bet']
            adv_str = f'  前走2位より{c["f3_adv"]:.1f}秒速い' if c['f3_adv'] > 0 else ''
            print(f"  ✅ [{c['race_id']}] {c['name']}")
            print(f"     オッズ: {c['odds']:.1f}倍  人気: {c['pop']}番人気")
            print(f"     前走: 上がり{c['prev_f3rank']}位 / {c['prev_finish']}着 / "
                  f"最終コーナー{c['prev_corner']}番手 / {c['prev_field']}頭立て{adv_str}")
            print(f"     → ベット: ¥{c['bet']:,}")
        print(f'\n  合計ベット予定: ¥{total_bet_fuk:,}')

    # ══════════════════════════════════════════════════════
    # 【重賞レース】グレード別分析
    # ══════════════════════════════════════════════════════
    if graded_races:
        print('\n' + '=' * 65)
        print('【重賞レース】グレード別分析（参考）')
        print('  ※ 汎用戦略対象外。上位候補を表示します。')
        print('=' * 65)

        for race_id, (grade, horses) in graded_races.items():
            race_label = race_id.split('_', 2)[-1] if '_' in race_id else race_id
            # レース名（グレード表記を除く）
            race_name_clean = re.sub(r'[\(（]G[123][\)）]', '', race_label).strip()
            print(f'\n  [{grade}] {race_label}  {len(horses)}頭立て')

            # レース特化分析（過去データがあれば）
            specific = race_specific_analysis(race_name_clean, horses, prev_history, args.data)
            if not specific['no_data']:
                print_race_specific(specific, grade=grade, top_n=5)
            else:
                # 過去データ不足時は汎用グレード分析にフォールバック
                cands = graded_race_analysis(horses, grade, prev_history)
                if not cands:
                    print('    → 対象候補なし')
                else:
                    fav_odds = min(h['odds'] for h in horses)
                    print(f'  ⚠️  過去特化データなし（汎用分析）  1番人気: {fav_odds:.1f}倍')
                    for c in cands[:5]:
                        f3_str = f"前走上がり{c['prev_f3rank']}位/{c['prev_finish']}着" if c['prev_f3rank'] else '前走データなし'
                        print(f"    {c['pop']:2d}番人気 {c['odds']:5.1f}倍  {c['name']}  複勝確率{c['prob']:.1f}%  {f3_str}")

    # ══════════════════════════════════════════════════════
    # 合計
    # ══════════════════════════════════════════════════════
    print('\n' + '=' * 65)
    ana_total = sum(c['bet'] for c in ana_cands if c['race_id'] in seen_ana)
    fuk_total = sum(c['bet'] for c in fuk_cands if c['race_id'] in seen_fuk)
    print(f'【本日の合計ベット予定（一般レース）】')
    print(f'  穴馬:      ¥{ana_total:,}  ({len(seen_ana)}レース)')
    print(f'  隠れ末脚:  ¥{fuk_total:,}  ({len(seen_fuk)}レース)')
    print(f'  残り軍資金(概算): ¥{args.capital - ana_total - fuk_total:,}')
    print('=' * 65)

if __name__ == '__main__':
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

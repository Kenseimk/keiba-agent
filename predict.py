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
import sys, io, csv, argparse, math, re, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest_core import (
    load_data, update_prev_history,
    top3_prob, parse_margin,
    ODDS_TOP3, FAV_ADJ, FIELD_ADJ, UNDERVALUED_THRESHOLD, UNDERVALUED_BONUS
)
from collections import defaultdict

# ══════════════════════════════════════════════════════════
# 戦略パラメータ（確定版）
# ══════════════════════════════════════════════════════════
ANA_PARAMS = {
    'odds_min': 10, 'odds_max': 30, 'prob_min': 25.0,
    'field_min': 8, 'pop_min': 4, 'pop_max': 18,
    'kelly_tiers': [[35, 0.03, 20000], [30, 0.02, 15000], [0, 0.015, 8000]],
}

FUKUSHO_PARAMS = {
    'prev_f3rank_max': 1,   # 前走上がり3F: 1位
    'prev_finish_min': 7,   # 前走着順: 7着以下
    'prev_field_min':  8,   # 前走頭数: 8頭以上
    'odds_min': 14.0,       # オッズ: 14倍以上
    'odds_max': 18.0,       #         18倍未満
    'pop_min':  6,          # 人気: 6番人気以上
    'pop_max': 12,
    'field_min': 8,         # 出走頭数: 8頭以上
    'kelly_pct': 0.030,     # Kelly: 3.0%
    'kelly_max': 15000,     # 上限: ¥15,000
}

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
# 穴馬戦略: top3_prob で候補抽出
# ══════════════════════════════════════════════════════════
def ana_candidates(races_input, capital):
    p = ANA_PARAMS
    results = []
    for race_id, horses in races_input.items():
        if len(horses) < p['field_min']:
            continue
        fav_odds  = min(h['odds'] for h in horses)
        field_size = len(horses)
        for h in horses:
            if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
            if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
            prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
            if prob >= p['prob_min']:
                # Kelly ベット額
                bet = 0
                for thresh, pct, cap_limit in p['kelly_tiers']:
                    if prob >= thresh:
                        bet = min(int(capital * pct / 100) * 100, cap_limit)
                        break
                results.append({
                    'race_id': race_id,
                    'name':    h['name'],
                    'odds':    h['odds'],
                    'pop':     h['popularity'],
                    'prob':    prob,
                    'bet':     bet,
                })
    results.sort(key=lambda x: -x['prob'])
    return results

# ══════════════════════════════════════════════════════════
# 隠れ末脚型複勝: prev_history と照合
# ══════════════════════════════════════════════════════════
def fukusho_candidates(races_input, prev_history, capital):
    p = FUKUSHO_PARAMS
    results = []
    for race_id, horses in races_input.items():
        if len(horses) < p['field_min']:
            continue
        for h in horses:
            if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
            if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
            ph = prev_history.get(h['name'])
            if ph is None: continue
            if ph['field_size'] < p['prev_field_min']: continue
            if ph['f3rank'] > p['prev_f3rank_max']: continue
            if ph['finish_rank'] < p['prev_finish_min']: continue
            bet = min(int(capital * p['kelly_pct'] / 100) * 100, p['kelly_max'])
            results.append({
                'race_id':    race_id,
                'name':       h['name'],
                'odds':       h['odds'],
                'pop':        h['popularity'],
                'prev_f3rank': ph['f3rank'],
                'prev_finish': ph['finish_rank'],
                'prev_corner': ph.get('last_corner', '?'),
                'prev_field':  ph['field_size'],
                'f3_adv':      ph.get('f3_advantage', 0.0),
                'bet':         bet,
            })
    results.sort(key=lambda x: -x['odds'])
    return results

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
    races_hist, jstats = load_data(args.data)
    by_month = defaultdict(list)
    for rid, info in races_hist.items():
        by_month[info['ym']].append((rid, info))

    prev_history  = {}
    prev_history2 = {}
    for ym in sorted(by_month.keys()):
        for rid, info in by_month[ym]:
            update_prev_history(info['horses'], prev_history, prev_history2)
    print(f'  {len(races_hist)}レース / 馬{len(prev_history)}頭 の前走データ構築完了')
    print(f'  最新データ月: {sorted(by_month.keys())[-1]}')

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
    total_all = sum(
        c['bet'] for c in ana_cands if c['race_id'] not in (set(seen_ana) - {c['race_id']})
    )
    print('\n' + '=' * 65)
    ana_total  = sum(c['bet'] for i, c in enumerate(ana_cands)
                     if c['race_id'] in seen_ana and
                     next((j for j, x in enumerate(ana_cands) if x['race_id'] == c['race_id']), -1) == i)
    fuk_total  = sum(c['bet'] for i, c in enumerate(fuk_cands)
                     if c['race_id'] in seen_fuk and
                     next((j for j, x in enumerate(fuk_cands) if x['race_id'] == c['race_id']), -1) == i)

    print(f'【本日の合計ベット予定】')
    print(f'  穴馬:      ¥{sum(c["bet"] for c in ana_cands[:len(seen_ana)]):,}  ({len(seen_ana)}レース)')
    print(f'  隠れ末脚:  ¥{sum(c["bet"] for c in fuk_cands[:len(seen_fuk)]):,}  ({len(seen_fuk)}レース)')
    print(f'  残り軍資金(概算): ¥{args.capital - sum(c["bet"] for c in ana_cands[:len(seen_ana)]) - sum(c["bet"] for c in fuk_cands[:len(seen_fuk)]):,}')
    print('=' * 65)

if __name__ == '__main__':
    main()

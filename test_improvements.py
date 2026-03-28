"""
test_improvements.py - 改善案を1つずつ検証するスクリプト

ベースライン:
  穴馬:  ROI 128.6%
  複勝:  ROI 130.0%

改善案:
  [複勝] ①前走F3圧倒的優位フィルタ (prev_f3_adv_min)
  [複勝] ②2走前も同パターン         (require_2race_pattern)
  [複勝] ①+② 組み合わせ
  [穴馬] ③前走F3優位スコアを穴馬にも追加（prev_historyあり版）
  [複勝] ④オッズ帯精緻化
  [複勝] ⑤人気帯調整
"""
import sys, io, copy
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

from backtest_core import load_data, run_ana_backtest, run_fukusho_backtest

ANA_BASE = {
    'odds_min': 10, 'odds_max': 30, 'prob_min': 25.0,
    'count_max': 15, 'field_min': 8, 'pop_min': 4, 'pop_max': 18,
    'kelly_tiers': [[35, 0.03, 20000], [30, 0.02, 15000], [0, 0.015, 8000]],
}

FUKUSHO_BASE = {
    'prev_f3rank_max': 1, 'prev_finish_min': 7, 'prev_field_min': 8,
    'odds_min': 12.0, 'odds_max': 18.0,
    'pop_min': 6, 'pop_max': 12, 'field_min': 8,
    'count_max': 15, 'kelly_pct': 0.02, 'kelly_max': 12000,
}

def fmt(m):
    return (f"ROI={m['roi']:.1f}%  bets={m['count']}  hit={m['hit_rate']:.1f}%  "
            f"red={m['red_months']}/23  cap=¥{m['final_capital']:,}")

def tf(races, jstats, p, label):
    m = run_fukusho_backtest(races, jstats, p)
    tag = '✅' if m['roi'] > FUKUSHO_BASE_ROI and m['count'] >= 30 else '  '
    print(f"  {tag} {label}")
    print(f"      {fmt(m)}")
    return m

def ta(races, jstats, p, label):
    m = run_ana_backtest(races, p)
    tag = '✅' if m['roi'] > ANA_BASE_ROI else '  '
    print(f"  {tag} {label}")
    print(f"      {fmt(m)}")
    return m

FUKUSHO_BASE_ROI = 130.0
ANA_BASE_ROI     = 128.6

def main():
    print("データ読み込み中...")
    races, jstats = load_data('data')
    print(f"{len(races)}レース / 騎手{len(jstats)}名\n")

    print("=" * 70)
    print("【ベースライン】")
    base_ana = run_ana_backtest(races, ANA_BASE)
    base_fuk = run_fukusho_backtest(races, jstats, FUKUSHO_BASE)
    print(f"  穴馬:  {fmt(base_ana)}")
    print(f"  複勝:  {fmt(base_fuk)}")

    # ══════════════════════════════════════════════════
    # 改善① 前走F3圧倒的優位フィルタ（2位との差）
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【複勝 改善①】前走F3圧倒的優位フィルタ (prev_f3_adv_min)")
    print("  狙い: 2位より0.2秒以上速かった馬に絞り込む")
    best_fuk_1 = base_fuk; best_adv = 0.0
    for adv in [0.1, 0.2, 0.3, 0.5]:
        p = copy.deepcopy(FUKUSHO_BASE)
        p['prev_f3_adv_min'] = adv
        m = tf(races, jstats, p, f"prev_f3_adv_min={adv}秒")
        if m['roi'] > best_fuk_1['roi'] and m['count'] >= 20:
            best_fuk_1 = m; best_adv = adv
    print(f"  ▶ 最良: adv_min={best_adv}秒  ROI={best_fuk_1['roi']:.1f}%")

    # ══════════════════════════════════════════════════
    # 改善② 2走前も同パターン（より緩い条件で）
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【複勝 改善②】2走前にも同パターンがあった馬（緩条件）")
    print("  狙い: f3rank=1かつ大敗が2回連続 = 再現性の高い過小評価馬")
    for f3max2, fin_min2 in [(1, 5), (1, 6), (2, 5), (2, 6)]:
        p = copy.deepcopy(FUKUSHO_BASE)
        p['require_2race_pattern'] = True
        p['prev_f3rank_max'] = 1
        p['prev2_finish_min'] = fin_min2
        # prev_f3rank_maxを2走前にも使う（FUKUSHO_BASE内の値）
        tf(races, jstats, p, f"2走前: f3rank≤{f3max2} finish≥{fin_min2}")

    # ══════════════════════════════════════════════════
    # 改善③ オッズ帯の精緻化
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【複勝 改善③】オッズ帯の精緻化")
    best_fuk_3 = base_fuk; best_odds = (12.0, 18.0)
    for o_min, o_max in [(10.0, 18.0), (11.0, 18.0), (12.0, 20.0),
                          (12.0, 16.0), (13.0, 18.0), (12.0, 22.0)]:
        p = copy.deepcopy(FUKUSHO_BASE)
        p['odds_min'] = o_min; p['odds_max'] = o_max
        m = tf(races, jstats, p, f"odds:{o_min}-{o_max}")
        if m['roi'] > best_fuk_3['roi'] and m['count'] >= 30:
            best_fuk_3 = m; best_odds = (o_min, o_max)
    print(f"  ▶ 最良: odds={best_odds}  ROI={best_fuk_3['roi']:.1f}%")

    # ══════════════════════════════════════════════════
    # 改善④ 人気帯調整
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【複勝 改善④】人気帯調整")
    best_fuk_4 = base_fuk; best_pop = (6, 12)
    for p_min, p_max in [(5, 12), (6, 12), (7, 12), (6, 10), (6, 14), (5, 10)]:
        p = copy.deepcopy(FUKUSHO_BASE)
        p['pop_min'] = p_min; p['pop_max'] = p_max
        m = tf(races, jstats, p, f"pop:{p_min}-{p_max}")
        if m['roi'] > best_fuk_4['roi'] and m['count'] >= 30:
            best_fuk_4 = m; best_pop = (p_min, p_max)
    print(f"  ▶ 最良: pop={best_pop}  ROI={best_fuk_4['roi']:.1f}%")

    # ══════════════════════════════════════════════════
    # 改善⑤ 穴馬：前走実績フィルタ（run_ana_backtestにprev_history追加版）
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【穴馬 改善⑤】prob_min閾値の細かい調整")
    best_ana_5 = base_ana; best_prob = 25.0
    for prob in [22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0]:
        p = copy.deepcopy(ANA_BASE)
        p['prob_min'] = prob
        m = ta(races, jstats, p, f"prob_min={prob}")
        if m['roi'] > best_ana_5['roi'] and m['count'] >= 200:
            best_ana_5 = m; best_prob = prob
    print(f"  ▶ 最良: prob_min={best_prob}  ROI={best_ana_5['roi']:.1f}%")

    # ══════════════════════════════════════════════════
    # 改善⑥ 穴馬：オッズ帯精緻化
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【穴馬 改善⑥】オッズ帯精緻化")
    best_ana_6 = base_ana; best_ana_odds = (10, 30)
    for o_min, o_max in [(10, 25), (10, 20), (12, 30), (8, 30), (10, 35)]:
        p = copy.deepcopy(ANA_BASE)
        p['odds_min'] = o_min; p['odds_max'] = o_max
        m = ta(races, jstats, p, f"odds:{o_min}-{o_max}")
        if m['roi'] > best_ana_6['roi'] and m['count'] >= 200:
            best_ana_6 = m; best_ana_odds = (o_min, o_max)
    print(f"  ▶ 最良: odds={best_ana_odds}  ROI={best_ana_6['roi']:.1f}%")

    # ══════════════════════════════════════════════════
    # 改善① best が有効なら best + ③ 組み合わせ
    # ══════════════════════════════════════════════════
    if best_adv > 0 and best_fuk_1['roi'] > FUKUSHO_BASE_ROI:
        print("\n" + "=" * 70)
        print(f"【複勝 改善①+③】adv_min={best_adv} + オッズ帯調整")
        for o_min, o_max in [(10.0, 18.0), (12.0, 20.0), (12.0, 22.0), (11.0, 20.0)]:
            p = copy.deepcopy(FUKUSHO_BASE)
            p['prev_f3_adv_min'] = best_adv
            p['odds_min'] = o_min; p['odds_max'] = o_max
            tf(races, jstats, p, f"adv≥{best_adv} odds:{o_min}-{o_max}")

    # ══════════════════════════════════════════════════
    # サマリ
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("【最終サマリ】")
    print(f"  穴馬 baseline:         ROI={base_ana['roi']:.1f}%  bets={base_ana['count']}")
    print(f"  複勝 baseline:         ROI={base_fuk['roi']:.1f}%  bets={base_fuk['count']}")
    if best_adv > 0:
        print(f"  複勝 改善①(F3優位):   ROI={best_fuk_1['roi']:.1f}%  bets={best_fuk_1['count']}  (adv≥{best_adv}秒)")
    print(f"  複勝 改善③(オッズ):   ROI={best_fuk_3['roi']:.1f}%  bets={best_fuk_3['count']}  (odds={best_odds})")
    print(f"  複勝 改善④(人気):     ROI={best_fuk_4['roi']:.1f}%  bets={best_fuk_4['count']}  (pop={best_pop})")
    print(f"  穴馬 改善⑤(prob):     ROI={best_ana_5['roi']:.1f}%  bets={best_ana_5['count']}  (prob≥{best_prob})")
    print(f"  穴馬 改善⑥(オッズ):  ROI={best_ana_6['roi']:.1f}%  bets={best_ana_6['count']}  (odds={best_ana_odds})")

if __name__ == '__main__':
    main()

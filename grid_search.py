"""
grid_search.py - パラメータグリッドサーチ
穴馬戦略のベストパラメータを探索する
"""
import sys, glob, csv, io, re, math
from collections import defaultdict
from math import comb
from itertools import product

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

ODDS_TOP3 = {'10-19': 23.6, '20-49': 13.4}
FAV_ADJ   = {'1x': -1.3, '2-3': 0.0, '4-5': +1.1, '6+': +3.4}
FIELD_ADJ = {'few': +5.3, 'mid': 0.0, 'many': -4.2}
UNDERVALUED_THRESHOLD = {4: 6.0, 5: 8.0, 6: 10.0, 7: 12.0, 8: 15.0}
UNDERVALUED_BONUS = 8.0

def get_ob(o):   return '10-19' if 10 <= o < 20 else '20-49' if 20 <= o < 50 else None
def get_fb(fo):  return '1x' if fo < 2 else '2-3' if fo < 4 else '4-5' if fo < 6 else '6+'
def get_fsb(fs): return 'few' if fs <= 8 else 'mid' if fs <= 12 else 'many'

def top3_prob(odds, fav_odds, field_size, popularity):
    ob = get_ob(odds)
    if ob is None: return 0.0
    base = ODDS_TOP3[ob]
    adj  = FAV_ADJ[get_fb(fav_odds)] + FIELD_ADJ[get_fsb(field_size)]
    if odds < UNDERVALUED_THRESHOLD.get(popularity, 999): adj += UNDERVALUED_BONUS
    return max(0.0, min(100.0, base + adj))

def parse_payout(s):
    result = {}
    if not s or not s.strip(): return result
    for part in s.split('|'):
        part = part.strip()
        if ':' not in part: continue
        key, pay = part.rsplit(':', 1)
        try: result[key.strip()] = int(pay.strip())
        except: pass
    return result

def load_csv_data(data_dir):
    races = {}
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        file_ym = f'{m.group(1)}-{m.group(2)}' if m else None
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                race_id = row['race_id']
                venue   = row['場コード']
                if venue not in JRA_VENUES: continue
                ym = file_ym or (row['年'] + '-' + race_id[4:6])
                if race_id not in races:
                    races[race_id] = {
                        'horses': [], 'ym': ym,
                        'fukusho_raw': row.get('複勝払戻', ''),
                    }
                try:
                    finish_rank = int(row['着順'])
                    odds        = float(row['単勝オッズ'])
                    popularity  = int(row['人気'])
                    umaban      = row['馬番'].strip()
                except: continue
                races[race_id]['horses'].append({
                    'name':        row['馬名'],
                    'umaban':      umaban,
                    'finish_rank': finish_rank,
                    'odds':        odds,
                    'popularity':  popularity,
                })
    return races

def run_backtest_param(races, cfg):
    """
    cfg = {
        odds_min, odds_max, prob_min, count_max,
        kelly_thresholds: [(prob, frac, cap), ...],  # 降順
        pop_min,  # 人気下限
    }
    """
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = 100000
    total_invest = total_ret = total_count = total_hit = 0
    worst_roi = 9999.0
    monthly_rois = []

    for ym in sorted(by_month.keys()):
        capital += 20000
        races_m = by_month[ym]

        ana_cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if not horses: continue
            field_size = len(horses)
            fav_odds   = min(h['odds'] for h in horses)
            for h in horses:
                if h['popularity'] < cfg['pop_min']: continue
                if not (cfg['odds_min'] <= h['odds'] < cfg['odds_max']): continue
                prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                if prob >= cfg['prob_min']:
                    ana_cands.append((prob, h, race_id, info))

        ana_cands.sort(key=lambda x: -x[0])
        seen_races = set()
        ana_selected = []
        for prob, h, race_id, info in ana_cands:
            if race_id in seen_races: continue
            seen_races.add(race_id)
            # Kelly風ベット額計算
            bet = 0
            for threshold, frac, cap_limit in cfg['kelly_thresholds']:
                if prob >= threshold:
                    bet = min(int(capital * frac / 100) * 100, cap_limit)
                    break
            if bet >= 100:
                ana_selected.append((prob, h, race_id, info, bet))
            if len(ana_selected) >= cfg['count_max']: break

        m_invest = m_ret = m_hit = 0
        for prob, h, race_id, info, bet in ana_selected:
            capital -= bet
            m_invest += bet
            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_ret += payout
                m_hit += 1

        total_invest += m_invest
        total_ret    += m_ret
        total_count  += len(ana_selected)
        total_hit    += m_hit
        if m_invest > 0:
            mr = m_ret / m_invest * 100
            monthly_rois.append(mr)
            if mr < worst_roi: worst_roi = mr

    roi = total_ret / total_invest * 100 if total_invest else 0
    hit_rate = total_hit / total_count * 100 if total_count else 0
    # 月別赤字カウント
    red_months = sum(1 for r in monthly_rois if r < 100)
    return {
        'roi': roi,
        'invest': total_invest,
        'ret': total_ret,
        'count': total_count,
        'hit': total_hit,
        'hit_rate': hit_rate,
        'final_capital': int(capital),
        'red_months': red_months,
        'worst_month_roi': worst_roi if monthly_rois else 0,
    }

def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else 'data'
    print(f'データ読み込み中...')
    races = load_csv_data(data_dir)
    print(f'{len(races)}レース読み込み完了')
    print()

    # グリッドサーチ空間
    odds_max_list  = [20, 25, 30, 35, 40]
    odds_min_list  = [10, 12]
    prob_min_list  = [22, 25, 27, 30]
    count_max_list = [10, 15, 20, 30]
    pop_min_list   = [4, 5]

    # Kelly設定バリエーション（(確率閾値, 資金%, 上限額) リスト）
    kelly_variants = {
        'aggressive': [(35, 0.05, 40000), (30, 0.04, 30000), (0, 0.03, 20000)],
        'standard':   [(35, 0.04, 30000), (30, 0.03, 20000), (0, 0.02, 10000)],
        'conservative':[(35, 0.03, 20000), (30, 0.02, 15000), (0, 0.015, 8000)],
        'flat2pct':   [(0, 0.02, 10000)],
        'flat3pct':   [(0, 0.03, 15000)],
    }

    results = []
    total_combos = (len(odds_max_list) * len(odds_min_list) * len(prob_min_list) *
                    len(count_max_list) * len(pop_min_list) * len(kelly_variants))
    print(f'グリッドサーチ: {total_combos}通りを探索中...')
    print()

    done = 0
    for odds_max, odds_min, prob_min, count_max, pop_min, (kname, kthresh) in product(
            odds_max_list, odds_min_list, prob_min_list, count_max_list, pop_min_list,
            kelly_variants.items()):

        if odds_min >= odds_max: continue

        cfg = {
            'odds_min': odds_min, 'odds_max': odds_max,
            'prob_min': prob_min, 'count_max': count_max,
            'pop_min': pop_min,
            'kelly_thresholds': kthresh,
        }
        r = run_backtest_param(races, cfg)
        r['cfg'] = {
            'odds': f'{odds_min}-{odds_max}',
            'prob': prob_min,
            'count': count_max,
            'pop': pop_min,
            'kelly': kname,
        }
        results.append(r)
        done += 1

    # ROI降順ソート
    results.sort(key=lambda x: -x['roi'])

    print('=' * 90)
    print('=== グリッドサーチ結果 TOP30（ROI降順）===')
    print('=' * 90)
    print(f'{"ROI":>7} | {"件数":>5} | {"的中率":>7} | {"最終資金":>10} | {"赤字月":>5} | {"最悪月ROI":>9} | 設定')
    print('-' * 90)
    for r in results[:30]:
        c = r['cfg']
        cfg_str = f'odds:{c["odds"]} prob:{c["prob"]} cnt:{c["count"]} pop:{c["pop"]}+ kelly:{c["kelly"]}'
        print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | ¥{r["final_capital"]:>9,} | {r["red_months"]:>5} | {r["worst_month_roi"]:>8.1f}% | {cfg_str}')

    print()
    print('=== バランス重視 TOP10（ROI≥120% かつ 赤字月≤8）===')
    print('-' * 90)
    balanced = [r for r in results if r['roi'] >= 120 and r['red_months'] <= 8]
    balanced.sort(key=lambda x: (-x['roi'], x['red_months']))
    for r in balanced[:10]:
        c = r['cfg']
        cfg_str = f'odds:{c["odds"]} prob:{c["prob"]} cnt:{c["count"]} pop:{c["pop"]}+ kelly:{c["kelly"]}'
        print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | ¥{r["final_capital"]:>9,} | {r["red_months"]:>5} | {r["worst_month_roi"]:>8.1f}% | {cfg_str}')

    if not balanced:
        print('  (条件を満たす設定なし — ROI≥115% かつ 赤字月≤9 で再検索)')
        balanced2 = [r for r in results if r['roi'] >= 115 and r['red_months'] <= 9]
        balanced2.sort(key=lambda x: (-x['roi'], x['red_months']))
        for r in balanced2[:10]:
            c = r['cfg']
            cfg_str = f'odds:{c["odds"]} prob:{c["prob"]} cnt:{c["count"]} pop:{c["pop"]}+ kelly:{c["kelly"]}'
            print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | ¥{r["final_capital"]:>9,} | {r["red_months"]:>5} | {r["worst_month_roi"]:>8.1f}% | {cfg_str}')

if __name__ == '__main__':
    main()

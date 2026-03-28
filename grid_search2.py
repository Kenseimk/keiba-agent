"""
grid_search2.py - より詳細な探索
- 2層ベット（高確率ゾーンと中確率ゾーンで異なる配分）
- フィールドサイズフィルタ
- 人気フィルタ（4番人気以上のみ等）
- conservative kelly固定で他パラメータを深掘り
"""
import sys, glob, csv, io, re, math
from collections import defaultdict
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
                    races[race_id] = {'horses': [], 'ym': ym, 'fukusho_raw': row.get('複勝払戻', '')}
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

def run_backtest(races, cfg):
    """
    cfg keys:
        odds_min, odds_max, prob_min, count_max, pop_min, pop_max,
        field_min, field_max,          # 出走頭数フィルタ
        kelly_tiers: [(prob_th, pct, cap), ...],  # 降順
        per_race: bool,                # 1レース1頭制限
    """
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = 100000
    total_invest = total_ret = total_count = total_hit = 0
    monthly_rois = []

    for ym in sorted(by_month.keys()):
        capital += 20000
        races_m = by_month[ym]

        ana_cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if not horses: continue
            field_size = len(horses)
            if not (cfg['field_min'] <= field_size <= cfg['field_max']): continue
            fav_odds   = min(h['odds'] for h in horses)
            for h in horses:
                if not (cfg['pop_min'] <= h['popularity'] <= cfg['pop_max']): continue
                if not (cfg['odds_min'] <= h['odds'] < cfg['odds_max']): continue
                prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                if prob >= cfg['prob_min']:
                    ana_cands.append((prob, h, race_id, info))

        ana_cands.sort(key=lambda x: -x[0])
        seen_races = set()
        ana_selected = []
        for prob, h, race_id, info in ana_cands:
            if cfg['per_race'] and race_id in seen_races: continue
            seen_races.add(race_id)
            bet = 0
            for threshold, frac, cap_limit in cfg['kelly_tiers']:
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
            monthly_rois.append(m_ret / m_invest * 100)

    roi = total_ret / total_invest * 100 if total_invest else 0
    red_months = sum(1 for r in monthly_rois if r < 100)
    worst = min(monthly_rois) if monthly_rois else 0
    return {
        'roi': roi, 'count': total_count, 'hit': total_count and total_hit/total_count*100 or 0,
        'final_cap': int(capital), 'red_months': red_months, 'worst': worst,
        'invest': total_invest, 'ret': total_ret,
    }

def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else 'data'
    print('データ読み込み中...')
    races = load_csv_data(data_dir)
    print(f'{len(races)}レース\n')

    # =====================================================================
    # フェーズ1: フィールドサイズ × 人気範囲 × 確率閾値 探索
    # conservative kelly固定
    # =====================================================================
    print('【フェーズ1】フィールドサイズ・人気・確率の組み合わせ探索')
    print('=' * 100)

    base_kelly = [(35, 0.03, 20000), (30, 0.02, 15000), (0, 0.015, 8000)]

    phase1_results = []
    for field_min, field_max in [(6,18), (8,18), (10,18), (12,18), (6,16), (8,16)]:
        for pop_min, pop_max in [(4,18), (4,8), (4,10), (5,10), (5,12), (6,12)]:
            for prob_min in [22, 25, 27, 30]:
                for odds_max in [20, 25, 30]:
                    cfg = {
                        'odds_min': 10, 'odds_max': odds_max,
                        'prob_min': prob_min, 'count_max': 15,
                        'pop_min': pop_min, 'pop_max': pop_max,
                        'field_min': field_min, 'field_max': field_max,
                        'kelly_tiers': base_kelly, 'per_race': True,
                    }
                    r = run_backtest(races, cfg)
                    r['label'] = f'field:{field_min}-{field_max} pop:{pop_min}-{pop_max} prob:{prob_min} odds:<{odds_max}'
                    phase1_results.append(r)

    phase1_results.sort(key=lambda x: -x['roi'])
    print(f'{"ROI":>7} | {"件数":>5} | {"的中率":>7} | {"最終資金":>10} | {"赤字月":>5} | 設定')
    print('-' * 100)
    for r in phase1_results[:20]:
        print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit"]:>6.1f}% | ¥{r["final_cap"]:>9,} | {r["red_months"]:>5} | {r["label"]}')

    # =====================================================================
    # フェーズ2: Kelly配分の深掘り（最良フィールド設定を使用）
    # =====================================================================
    print()
    print('【フェーズ2】Kelly配分バリエーション探索（フィールド8-18, 人気4-10固定）')
    print('=' * 100)

    kelly_variants = {
        'cons_015': [(35, 0.03, 20000), (30, 0.02, 15000), (0, 0.015, 8000)],
        'cons_010': [(35, 0.03, 20000), (30, 0.02, 15000), (0, 0.010, 6000)],
        'cons_020': [(35, 0.03, 20000), (30, 0.02, 15000), (0, 0.020, 10000)],
        'std':      [(35, 0.04, 30000), (30, 0.03, 20000), (0, 0.020, 10000)],
        'agg':      [(35, 0.05, 40000), (30, 0.04, 30000), (0, 0.030, 20000)],
        'flat_15':  [(0, 0.015, 8000)],
        'flat_20':  [(0, 0.020, 10000)],
        'flat_25':  [(0, 0.025, 12000)],
        'two_tier_only_high': [(30, 0.025, 15000)],  # prob>=30のみ
        'high_focus': [(35, 0.05, 30000), (30, 0.03, 20000)],  # 30未満は除外
    }

    phase2_results = []
    for kname, ktiers in kelly_variants.items():
        for prob_min in [22, 25, 27, 30]:
            for odds_max in [20, 25, 30]:
                cfg = {
                    'odds_min': 10, 'odds_max': odds_max,
                    'prob_min': prob_min, 'count_max': 20,
                    'pop_min': 4, 'pop_max': 10,
                    'field_min': 8, 'field_max': 18,
                    'kelly_tiers': ktiers, 'per_race': True,
                }
                r = run_backtest(races, cfg)
                r['label'] = f'kelly:{kname} prob:{prob_min} odds:<{odds_max}'
                phase2_results.append(r)

    phase2_results.sort(key=lambda x: -x['roi'])
    print(f'{"ROI":>7} | {"件数":>5} | {"的中率":>7} | {"最終資金":>10} | {"赤字月":>5} | 設定')
    print('-' * 100)
    for r in phase2_results[:20]:
        print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit"]:>6.1f}% | ¥{r["final_cap"]:>9,} | {r["red_months"]:>5} | {r["label"]}')

    # =====================================================================
    # フェーズ3: 上位設定の月別詳細
    # =====================================================================
    print()
    print('【フェーズ3】上位3設定の月別損益')
    print('=' * 100)

    # phase1とphase2の合算でバランス重視TOP
    all_results = phase1_results + phase2_results
    balanced = [r for r in all_results if r['roi'] >= 120 and r['red_months'] <= 8 and r['count'] >= 50]
    balanced.sort(key=lambda x: (-x['roi'], x['red_months']))

    top_cfgs = [
        # 現在設定（ベースライン）
        {'odds_min':10,'odds_max':30,'prob_min':25,'count_max':15,'pop_min':4,'pop_max':18,
         'field_min':1,'field_max':99,'kelly_tiers':[(35,0.03,20000),(30,0.02,15000),(0,0.015,8000)],
         'per_race':True, 'label':'現在設定(conservative kelly)'},
    ]
    # バランス重視上位2設定を追加（重複除去）
    seen_labels = set()
    for r in balanced[:5]:
        if r['label'] not in seen_labels:
            seen_labels.add(r['label'])
            # ラベルから設定を再構築するのは難しいので、phase1から探す
            pass

    for label, cfg_override in [
        ('フィールド8-18 人気4-10 prob25 odds<30 cons_015',
         {'odds_min':10,'odds_max':30,'prob_min':25,'count_max':20,'pop_min':4,'pop_max':10,
          'field_min':8,'field_max':18,'kelly_tiers':[(35,0.03,20000),(30,0.02,15000),(0,0.015,8000)],'per_race':True}),
        ('フィールド8-18 人気4-10 prob27 odds<25 high_focus',
         {'odds_min':10,'odds_max':25,'prob_min':27,'count_max':20,'pop_min':4,'pop_max':10,
          'field_min':8,'field_max':18,'kelly_tiers':[(35,0.05,30000),(30,0.03,20000)],'per_race':True}),
    ]:
        by_month_detail = defaultdict(list)
        for race_id, info in races.items():
            by_month_detail[info['ym']].append((race_id, info))

        capital = 100000
        print(f'\n--- {label} ---')
        print(f'{"月":<8} | {"件数":>4} | {"的中":>4} | {"投資":>8} | {"払戻":>8} | {"ROI":>7} | {"軍資金":>10}')
        print('-' * 65)
        total_inv = total_ret = 0
        for ym in sorted(by_month_detail.keys()):
            capital += 20000
            races_m = by_month_detail[ym]
            ana_cands = []
            for race_id, info in races_m:
                horses = info['horses']
                if not horses: continue
                field_size = len(horses)
                if not (cfg_override['field_min'] <= field_size <= cfg_override['field_max']): continue
                fav_odds = min(h['odds'] for h in horses)
                for h in horses:
                    if not (cfg_override['pop_min'] <= h['popularity'] <= cfg_override['pop_max']): continue
                    if not (cfg_override['odds_min'] <= h['odds'] < cfg_override['odds_max']): continue
                    prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
                    if prob >= cfg_override['prob_min']:
                        ana_cands.append((prob, h, race_id, info))

            ana_cands.sort(key=lambda x: -x[0])
            seen = set()
            selected = []
            for prob, h, race_id, info in ana_cands:
                if race_id in seen: continue
                seen.add(race_id)
                bet = 0
                for threshold, frac, cap_limit in cfg_override['kelly_tiers']:
                    if prob >= threshold:
                        bet = min(int(capital * frac / 100) * 100, cap_limit)
                        break
                if bet >= 100: selected.append((prob, h, race_id, info, bet))
                if len(selected) >= cfg_override['count_max']: break

            m_inv = m_ret = m_hit = 0
            for prob, h, race_id, info, bet in selected:
                capital -= bet
                m_inv += bet
                fukusho = parse_payout(info['fukusho_raw'])
                if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                    payout = int(bet * fukusho[h['umaban']] / 100)
                    capital += payout
                    m_ret += payout
                    m_hit += 1
            total_inv += m_inv
            total_ret += m_ret
            roi_m = m_ret/m_inv*100 if m_inv else 0
            mark = '+' if roi_m >= 100 else ' '
            print(f'{ym} | {len(selected):>4} | {m_hit:>4} | ¥{m_inv//1000:>5}k | ¥{m_ret//1000:>5}k | {mark}{roi_m:>5.0f}% | ¥{capital:>9,}')
        roi_total = total_ret/total_inv*100 if total_inv else 0
        print(f'{"合計":<8} | {"":>4} | {"":>4} | ¥{total_inv//1000:>5}k | ¥{total_ret//1000:>5}k | {roi_total:>6.1f}% | ¥{capital:>9,}')

if __name__ == '__main__':
    main()

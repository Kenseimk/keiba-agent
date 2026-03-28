"""
grid_search_kakure.py - 隠れ末脚型複勝 新戦略の検証

シグナル: 前走上がり3F上位 かつ 前走敗退 → 市場が過小評価している馬
EVプラスが確認済みの odds 10-15倍帯を中心に探索
"""
import sys, glob, csv, re, io
from collections import defaultdict
from itertools import product

if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

def parse_payout(s):
    result = {}
    if not s or not s.strip(): return result
    for part in s.split('|'):
        if ':' not in part: continue
        key, pay = part.rsplit(':', 1)
        try: result[key.strip()] = int(pay.strip())
        except: pass
    return result

def load_data(data_dir='data'):
    races = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        file_ym = f'{m.group(1)}-{m.group(2)}' if m else None
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                race_id = row['race_id']
                if row['場コード'] not in JRA_VENUES: continue
                ym = file_ym
                if race_id not in races:
                    races[race_id] = {'horses': [], 'ym': ym, 'fukusho_raw': row.get('複勝払戻', '')}
                try:
                    races[race_id]['horses'].append({
                        'name':        row['馬名'],
                        'umaban':      row['馬番'].strip(),
                        'finish_rank': int(row['着順']),
                        'odds':        float(row['単勝オッズ']),
                        'popularity':  int(row['人気']),
                        'f3':          row.get('上がり3F', '').strip(),
                        'corner':      row.get('通過順', '').strip(),
                    })
                except: pass
    return races

def update_prev_history(race_horses, prev_history):
    valid = [(float(h['f3']), h['name']) for h in race_horses if h['f3'].strip()]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(race_horses)
    for h in race_horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': int(nums[-1]) if nums else None,
            }
        except: pass

def run_backtest(races, cfg):
    """
    隠れ末脚型複勝バックテスト
    cfg: odds_min/max, pop_min/max, field_min,
         prev_f3rank_max, prev_finish_min, prev_field_min,
         kelly_pct, kelly_max, count_max, per_race
    """
    by_month = defaultdict(list)
    for race_id, info in races.items():
        by_month[info['ym']].append((race_id, info))

    capital = 70000
    prev_history = {}
    total_invest = total_ret = total_count = total_hit = 0
    monthly_rois = []

    for ym in sorted(by_month.keys()):
        capital += 20000
        races_m = by_month[ym]

        cands = []
        for race_id, info in races_m:
            horses = info['horses']
            if len(horses) < cfg['field_min']: continue
            for h in horses:
                if not (cfg['pop_min'] <= h['popularity'] <= cfg['pop_max']): continue
                if not (cfg['odds_min'] <= h['odds'] < cfg['odds_max']): continue
                ph = prev_history.get(h['name'])
                if ph is None: continue
                if ph['field_size'] < cfg['prev_field_min']: continue
                if ph['f3rank'] > cfg['prev_f3rank_max']: continue
                if ph['finish_rank'] < cfg['prev_finish_min']: continue
                # スコア: オッズ高い順（高配当期待）
                cands.append((h['odds'], h, race_id, info))

        cands.sort(key=lambda x: -x[0])
        seen = set()
        selected = []
        for odds_val, h, race_id, info in cands:
            if cfg['per_race'] and race_id in seen: continue
            seen.add(race_id)
            bet = min(int(capital * cfg['kelly_pct'] / 100) * 100, cfg['kelly_max'])
            if bet >= 100: selected.append((h, race_id, info, bet))
            if len(selected) >= cfg['count_max']: break

        m_inv = m_ret = m_hit = 0
        for h, race_id, info, bet in selected:
            capital -= bet
            m_inv += bet
            fukusho = parse_payout(info['fukusho_raw'])
            if h['finish_rank'] <= 3 and h['umaban'] in fukusho:
                payout = int(bet * fukusho[h['umaban']] / 100)
                capital += payout
                m_ret += payout
                m_hit += 1

        # prev_history 更新（ベット後）
        for race_id, info in races_m:
            update_prev_history(info['horses'], prev_history)

        reinvest = int(max(0, m_ret - m_inv) * 0.70 / 100) * 100
        capital += reinvest

        total_invest += m_inv
        total_ret    += m_ret
        total_count  += len(selected)
        total_hit    += m_hit
        if m_inv > 0:
            monthly_rois.append(m_ret / m_inv * 100)

    roi = total_ret / total_invest * 100 if total_invest else 0
    red = sum(1 for r in monthly_rois if r < 100)
    worst = min(monthly_rois) if monthly_rois else 0
    return {
        'roi': round(roi, 2), 'count': total_count, 'hit': total_hit,
        'hit_rate': round(total_hit / total_count * 100, 1) if total_count else 0,
        'red_months': red, 'worst': round(worst, 1),
        'final_cap': int(capital),
    }

def main():
    print('データ読み込み中...')
    races = load_data('data')
    print(f'{len(races)}レース\n')

    # ── グリッドサーチ ──
    results = []
    grid = list(product(
        [1, 2, 3],          # prev_f3rank_max
        [4, 5, 6],          # prev_finish_min
        [10, 12],           # odds_min
        [15, 18, 22],       # odds_max
        [4, 5],             # pop_min
        [12, 15],           # pop_max
        [8],                # prev_field_min (固定)
        [8],                # field_min (固定)
        [True, False],      # per_race
        [0.015, 0.020],     # kelly_pct
        [15],               # count_max (固定)
    ))

    print(f'グリッドサーチ: {len(grid)}通り探索中...')
    for (f3max, fin_min, o_min, o_max, p_min, p_max,
         pf_min, f_min, per_race, kelly, cnt) in grid:
        if o_min >= o_max: continue
        cfg = {
            'prev_f3rank_max': f3max, 'prev_finish_min': fin_min,
            'odds_min': o_min, 'odds_max': o_max,
            'pop_min': p_min, 'pop_max': p_max,
            'prev_field_min': pf_min, 'field_min': f_min,
            'per_race': per_race, 'kelly_pct': kelly,
            'kelly_max': 12000, 'count_max': cnt,
        }
        r = run_backtest(races, cfg)
        r['cfg'] = cfg
        results.append(r)

    results.sort(key=lambda x: -x['roi'])

    print('\n=== TOP30 (ROI降順) ===')
    print(f'{"ROI":>7} | {"件数":>5} | {"的中率":>7} | {"赤字月":>5} | {"最終資金":>10} | 設定')
    print('-' * 95)
    for r in results[:30]:
        c = r['cfg']
        print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | '
              f'{r["red_months"]:>5} | ¥{r["final_cap"]:>9,} | '
              f'f3≤{c["prev_f3rank_max"]} fin≥{c["prev_finish_min"]} '
              f'odds:{c["odds_min"]}-{c["odds_max"]} pop:{c["pop_min"]}-{c["pop_max"]} '
              f'pfield≥{c["prev_field_min"]} kelly:{c["kelly_pct"]} cnt:{c["count_max"]} '
              f'{"1/race" if c["per_race"] else "multi"}')

    print('\n=== バランス重視 (ROI≥110%, 赤字月≤9, 件数≥60) ===')
    balanced = [r for r in results if r['roi'] >= 110 and r['red_months'] <= 9 and r['count'] >= 60]
    balanced.sort(key=lambda x: (-x['roi'], x['red_months']))
    if balanced:
        for r in balanced[:15]:
            c = r['cfg']
            print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | '
                  f'{r["red_months"]:>5} | ¥{r["final_cap"]:>9,} | '
                  f'f3≤{c["prev_f3rank_max"]} fin≥{c["prev_finish_min"]} '
                  f'odds:{c["odds_min"]}-{c["odds_max"]} pop:{c["pop_min"]}-{c["pop_max"]} '
                  f'kelly:{c["kelly_pct"]} cnt:{c["count_max"]} '
                  f'{"1/race" if c["per_race"] else "multi"}')
    else:
        print('  条件なし → ROI≥105%, 赤字月≤10で再検索')
        balanced2 = [r for r in results if r['roi'] >= 105 and r['red_months'] <= 10 and r['count'] >= 40]
        balanced2.sort(key=lambda x: (-x['roi'], x['red_months']))
        for r in balanced2[:15]:
            c = r['cfg']
            print(f'{r["roi"]:>6.1f}% | {r["count"]:>5} | {r["hit_rate"]:>6.1f}% | '
                  f'{r["red_months"]:>5} | ¥{r["final_cap"]:>9,} | '
                  f'f3≤{c["prev_f3rank_max"]} fin≥{c["prev_finish_min"]} '
                  f'odds:{c["odds_min"]}-{c["odds_max"]} pop:{c["pop_min"]}-{c["pop_max"]} '
                  f'kelly:{c["kelly_pct"]} cnt:{c["count_max"]} '
                  f'{"1/race" if c["per_race"] else "multi"}')

if __name__ == '__main__':
    main()

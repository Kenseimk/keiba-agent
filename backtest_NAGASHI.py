# -*- coding: utf-8 -*-
"""
backtest_NAGASHI.py  NAGASHI モデル バックテスト (正式版)
=========================================================
戦略名: NAGASHI
概要  : ◎○二軸 × ▲☆△ながし 三連単C1

フィルター条件:
  - 8R〜11R
  - 出走頭数 <= 14頭
  - ◎ win_prob >= 20%
  - ◎ odds <= 4.0倍
  - ◎ + ○ win_prob合計 >= 45%  ← 改善ポイント (旧: 35%)

買い目:
  - 三連単C1: ◎○二軸 × ▲☆△ながし (win_prob 3〜6位 計4頭)
  - 計8通 × 100円 = 800円/R

バックテスト期間: 202501〜202603

結果:
  84R  的中23R(27.4%)  投資66,600円  回収87,260円  +20,660円  ROI 131.0%
  黒字月: 5ヶ月 / 赤字月: 10ヶ月  月平均収支: +1,377円

改善の根拠:
  失敗パターン分析より:
    - ◎が3着以内に入らない: 19.7%
    - ◎OKだが○が入らない:  32.5%  ← 最大の失敗原因
    - ◎○OKだが▲が入らない: 28.3%
  ◎+○合計wp≥45%とすることで○の信頼度を高め、
  的中率を 19.4% → 27.4% に改善

実行:
  python backtest_NAGASHI.py
  python backtest_NAGASHI.py --start 202501 --end 202603
  python backtest_NAGASHI.py --verbose
"""
import os, re, csv, glob, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'

from collections import defaultdict
from itertools import permutations

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db, make_race_info
from uscore import (
    analyze_race_uscore, build_trainer_stats, build_jockey_stats,
    should_exclude_uscore,
)

# ── パラメータ ────────────────────────────────────
RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
HONMEI_WP_MIN   = 20.0   # ◎ win_prob 下限
HONMEI_ODDS_MAX = 4.0    # ◎ オッズ上限
WP_SUM_MIN      = 45.0   # ◎+○ win_prob合計下限 (旧: 35%)
N_AITE          = 4      # ながし相手頭数 (win_prob 3〜6位)
MARKET_ALPHA    = 0.4
BET             = 100


def load_sanrentan(data_dir, start_ym, end_ym):
    db = {}
    for fpath in sorted(glob.glob(f'{data_dir}/raceresults_*.csv')):
        m = re.search(r'(\d{6})\.csv', fpath)
        ym = m.group(1) if m else ''
        if ym < start_ym or ym > end_ym:
            continue
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if rid not in db:
                    db[rid] = {}
                san = row.get('三連単払戻', '').strip()
                if san and san != '-':
                    mm = re.search(r'(\d+)\s*[→]\s*(\d+)\s*[→]\s*(\d+):(\d+)', san)
                    if mm:
                        key = (mm.group(1), mm.group(2), mm.group(3))
                        db[rid][key] = int(mm.group(4))
    return db


def run(test_start='202501', test_end='202603', verbose=False):
    print(f'=== NAGASHI バックテスト ===')
    print(f'テスト期間: {test_start} 〜 {test_end}')
    print(f'条件: {RNUM_MIN}-{RNUM_MAX}R / 頭数≤{MAX_FIELD} / '
          f'◎wp≥{HONMEI_WP_MIN}% / ◎odds≤{HONMEI_ODDS_MAX} / ◎+○wp≥{WP_SUM_MIN}%')
    print(f'買い目: 三連単C1 ◎○二軸×▲☆△({N_AITE}頭) = 8通/R\n')

    races = load_all_csv_races('data')
    print(f'全レース: {len(races):,}R')

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=test_start)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'horse_db: {len(horse_db):,}頭\n')

    sanrentan_db = load_sanrentan('data', test_start, test_end)
    test_rids = sorted(rid for rid, info in races.items()
                       if test_start <= info['file_ym'] <= test_end)

    monthly = defaultdict(lambda: {'cost': 0, 'ret': 0, 'races': 0, 'hits': 0})

    for race_id in test_rids:
        info = races[race_id]
        if should_exclude_uscore(info['race_name']):
            continue
        if all(h['odds'] == 0.0 for h in info['horse_list']):
            continue

        rnum = int(race_id[-2:])
        if not (RNUM_MIN <= rnum <= RNUM_MAX):
            continue
        if info['n_field'] > MAX_FIELD:
            continue

        race_obj = make_race_info(info)
        try:
            sc = analyze_race_uscore(
                race_obj, horse_db, None, None,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
                market_alpha=MARKET_ALPHA,
            )
        except:
            continue
        if not sc:
            continue

        wp_map   = {h['name']: h['win_prob'] for h in sc}
        odds_map = {h['name']: h['odds']     for h in info['horse_list']}
        uma_map  = {h['name']: h['umaban']   for h in info['horse_list']}
        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)

        hn = sorted_wp[0]['name']
        rn = sorted_wp[1]['name'] if len(sorted_wp) > 1 else None
        if not rn:
            continue

        # フィルター
        if wp_map.get(hn, 0) < HONMEI_WP_MIN:
            continue
        if odds_map.get(hn, 99) > HONMEI_ODDS_MAX:
            continue
        if wp_map.get(hn, 0) + wp_map.get(rn, 0) < WP_SUM_MIN:
            continue

        # ながし相手: win_prob 3〜(N_AITE+2)位
        aite = [h['name'] for h in sorted_wp[2:2+N_AITE]]
        u_h = uma_map.get(hn, '')
        u_r = uma_map.get(rn, '')
        u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
        if not u_h or not u_r or not u_a:
            continue

        # 三連単C1
        tix = set()
        for perm in permutations([u_h, u_r]):
            for a in u_a:
                tix.add((*perm, a))

        san  = sanrentan_db.get(race_id, {})
        cost = len(tix) * BET
        ret  = sum(san.get(t, 0) * BET // 100 for t in tix)
        hit  = int(ret > 0)

        ym = info['file_ym']
        monthly[ym]['cost']  += cost
        monthly[ym]['ret']   += ret
        monthly[ym]['races'] += 1
        monthly[ym]['hits']  += hit

        if verbose and hit:
            won = [t for t in tix if t in san]
            pay = max(san.get(t, 0) for t in tix)
            print(f'  ✓ {race_id} {info["race_name"]}  '
                  f'◎{hn}/wp{wp_map[hn]:.0f}%  ○{rn}/wp{wp_map[rn]:.0f}%  '
                  f'→ {ret:,}円 (配当{pay:,}円)')

    # 月別集計
    print(f'{"月":>8}  {"R数":>4}  {"的中":>4}  {"的中率":>6}  {"投資":>8}  {"回収":>8}  {"収支":>9}  {"ROI":>7}')
    print('-' * 72)
    total = defaultdict(int)
    for ym in sorted(monthly):
        s = monthly[ym]
        n = s['races']; h = s['hits']; c = s['cost']; r = s['ret']
        roi = r / c * 100 if c else 0
        mark = '✓' if roi >= 100 else '✗'
        print(f'{ym:>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  {c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}% {mark}')
        for k in ['races', 'hits', 'cost', 'ret']:
            total[k] += s[k]

    print('-' * 72)
    n = total['races']; h = total['hits']; c = total['cost']; r = total['ret']
    roi = r / c * 100 if c else 0
    print(f'{"合計":>8}  {n:>4}  {h:>4}  {h/n*100:>5.1f}%  {c:>8,}  {r:>8,}  {r-c:>+9,}  {roi:>6.1f}%')
    print()
    black = sum(1 for s in monthly.values() if s['ret'] > s['cost'])
    red   = len(monthly) - black
    print(f'黒字月: {black}ヶ月  赤字月: {red}ヶ月')
    if monthly:
        print(f'月平均投資: {c//len(monthly):,}円  月平均収支: {(r-c)//len(monthly):+,}円')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',   default='202501')
    parser.add_argument('--end',     default='202603')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    run(args.start, args.end, args.verbose)

# -*- coding: utf-8 -*-
"""
predict_nagashi.py - NAGASHI v1.0 当日予測スクリプト

使い方:
  python predict_nagashi.py               # 本日のデータ
  python predict_nagashi.py 20260412      # 日付指定

NAGASHI v1.0 フィルター:
  - 8R〜11R / <=14頭
  - ◎ win_prob >= 15%
  - ◎ odds <= 4.0倍
  - ◎+○ win_prob合計 >= 35%

買い目:
  三連単C1: ◎○二軸 × (win_prob 3〜7位 計5頭) ながし
  計10通り × ¥100 = ¥1,000/R
"""
import os, sys, json, datetime
from collections import defaultdict
from itertools import permutations

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import analyze_race_uscore, build_trainer_stats, build_jockey_stats

# ── NAGASHI v1.0 パラメータ ──
HONMEI_WP_MIN   = 15.0
HONMEI_ODDS_MAX = 4.0
WP_SUM_MIN      = 35.0
N_AITE          = 5
MARKET_ALPHA    = 0.4
BET             = 100

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
}


def load_horse_db(data_dir='data'):
    """全CSVから horse_db を構築"""
    print('horse_db 構築中 ...', end=' ', flush=True)
    races = load_all_csv_races(data_dir)
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races)
    for name in horse_db:
        horse_db[name].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'{len(races):,} レース / {len(horse_db):,} 頭')
    return horse_db, races


def make_race_info_from_fetch(r: dict) -> dict:
    """fetch_race.py 出力 → analyze_race_uscore 用 race_info"""
    race_id = r['race_id']
    race_ym = race_id[:6]  # 例 202606

    # horses と odds をマージ
    horse_map = {h['name']: h for h in r.get('horses', [])}
    odds_list  = r.get('odds', [])

    horses = []
    for h in r.get('horses', []):
        horses.append({
            'name':     h['name'],
            'jockey':   h.get('jockey', ''),
            'gate_num': h.get('gate_num') or h.get('umaban') or 0,
        })

    return {
        'race_id':    race_id,
        'race_name':  r.get('race_name', ''),
        'venue_code': race_id[4:6],
        'race_ym':    race_ym,
        'dist':       r.get('dist', 0),
        'course':     r.get('course', 'ダート'),
        'track_cond': r.get('track_cond', ''),
        'horses':     horses,
        'odds':       [{'name': o['name'], 'odds': o['odds'], 'pop': o['pop']}
                       for o in odds_list],
    }


def run_nagashi(date_str: str = None):
    date_str = date_str or datetime.date.today().strftime('%Y%m%d')
    json_path = f'data/races_{date_str}.json'

    if not os.path.exists(json_path):
        print(f'[ERROR] {json_path} が見つかりません。先に fetch_race.py {date_str} を実行してください。')
        return

    with open(json_path, encoding='utf-8') as f:
        fetch_data = json.load(f)

    races_today = fetch_data.get('all_races', [])
    print(f'\n=== NAGASHI v1.0 予測 {date_str} ===')
    print(f'対象レース数(8〜11R全体): {len(races_today)}')

    # horse_db 構築
    horse_db, _ = load_horse_db()
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)

    results = []

    print()
    for r in sorted(races_today, key=lambda x: x['race_id']):
        race_id  = r['race_id']
        rnum     = int(race_id[-2:])
        n_horses = r.get('n_horses', len(r.get('odds', [])))
        venue    = VENUE_MAP.get(race_id[4:6], race_id[4:6])

        # NAGASHI フィルター1: 8〜11R / <=14頭
        if not (8 <= rnum <= 11):
            continue
        if n_horses > 14:
            print(f'  [{race_id}] {venue} {rnum}R {r["race_name"]} {r["dist"]}m {n_horses}頭  → スキップ (頭数超過)')
            continue

        race_info = make_race_info_from_fetch(r)
        if not race_info['odds']:
            print(f'  [{race_id}] オッズなし → スキップ')
            continue

        try:
            sc = analyze_race_uscore(
                race_info, horse_db, None, None,
                trainer_stats=trainer_stats,
                jockey_stats=jockey_stats,
                market_alpha=MARKET_ALPHA,
            )
        except Exception as e:
            print(f'  [{race_id}] uscore error: {e}')
            continue

        if not sc:
            continue

        wp_map   = {h['name']: h['win_prob'] for h in sc}
        odds_map = {o['name']: o['odds']     for o in race_info['odds']}
        uma_map  = {}
        for idx, o in enumerate(sorted(race_info['odds'], key=lambda x: x['pop']), 1):
            uma_map[o['name']] = str(idx)  # 枠番なしのため人気順番号で代替

        sorted_wp = sorted(sc, key=lambda h: h['win_prob'], reverse=True)

        hn = sorted_wp[0]['name']
        rn = sorted_wp[1]['name'] if len(sorted_wp) > 1 else None
        if not rn:
            continue

        wp_hn  = wp_map.get(hn, 0)
        odds_hn = odds_map.get(hn, 99)
        wp_sum = wp_hn + wp_map.get(rn, 0)

        passes = (
            wp_hn   >= HONMEI_WP_MIN and
            odds_hn <= HONMEI_ODDS_MAX and
            wp_sum  >= WP_SUM_MIN
        )

        status = '★採用' if passes else '  ----'
        print(f'{status} [{race_id}] {venue} {rnum}R {r["race_name"]} {r["dist"]}m {r.get("course","")} {n_horses}頭')
        print(f'        ◎ {hn}  wp:{wp_hn:.1f}%  odds:{odds_hn:.1f}倍')
        print(f'        ○ {rn}  wp:{wp_map.get(rn,0):.1f}%  sum:{wp_sum:.1f}%')

        if passes:
            aite = [h['name'] for h in sorted_wp[2:2+N_AITE]]
            print(f'        ながし相手: {" / ".join(aite[:N_AITE])}')

            # 馬番マップ (odds順で人気番号として代替)
            pop_map = {o['name']: o['pop'] for o in race_info['odds']}
            u_h = str(pop_map.get(hn, '?'))
            u_r = str(pop_map.get(rn, '?'))
            u_a = [str(pop_map.get(n, '?')) for n in aite]

            tix = set()
            for perm in permutations([hn, rn]):
                for a in aite:
                    if a not in perm:
                        tix.add(perm + (a,))

            bet_total = len(tix) * BET
            print(f'        三連単 {len(tix)}通り × ¥{BET} = ¥{bet_total:,}')
            print(f'        発走予定: {r.get("start_time","?")}')

            results.append({
                'race_id':   race_id,
                'venue':     venue,
                'rnum':      rnum,
                'race_name': r['race_name'],
                'dist':      r['dist'],
                'course':    r.get('course',''),
                'start_time':r.get('start_time','?'),
                'honmei':    hn,
                'niban':     rn,
                'wp_honmei': wp_hn,
                'wp_sum':    wp_sum,
                'odds_honmei': odds_hn,
                'aite':      aite,
                'tickets':   len(tix),
                'bet_total': bet_total,
            })
        print()

    # サマリー
    print('=' * 60)
    print(f'NAGASHI v1.0 採用レース: {len(results)}R')
    total_bet = sum(r['bet_total'] for r in results)
    print(f'合計投資額: ¥{total_bet:,}')
    print()

    if results:
        print('【買い目まとめ】')
        for r in results:
            print(f"  {r['venue']} {r['rnum']}R {r['race_name']} ({r['start_time']})")
            print(f"  三連単: ◎{r['honmei']}・○{r['niban']} × {'/'.join(r['aite'])} ながし")
            print(f"  {r['tickets']}通り × ¥{BET} = ¥{r['bet_total']:,}")
            print()


if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_nagashi(date_arg)

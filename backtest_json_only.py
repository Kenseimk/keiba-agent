"""
backtest_json_only.py - JSONデータだけで完結するバックテスト

data/netkeiba_YYYYMM.json から
全馬券種の実際の払戻データを集計して回収率を算出する。

CSVなし・JSONのみで動作。

使い方:
    python backtest_json_only.py data
"""
import os, sys, json, glob, re
from collections import defaultdict

def load_json_data(data_dir):
    all_data = {}
    files = sorted(glob.glob(f'{data_dir}/netkeiba_*.json'))
    if not files:
        print(f'[ERROR] {data_dir}/*.json が見つかりません')
        sys.exit(1)
    for f in files:
        try:
            with open(f, encoding='utf-8') as fp:
                d = json.load(fp)
            all_data.update(d)
            ym = re.search(r'netkeiba_(\d{6})', f)
            print(f'  {os.path.basename(f)}: {len(d)}件')
        except Exception as e:
            print(f'  {f}: エラー {e}')
    print(f'合計: {len(all_data)}レース\n')
    return all_data

def run(data_dir):
    print(f'=== データ読み込み ===')
    all_data = load_json_data(data_dir)

    # 月別グループ化
    by_month = defaultdict(list)
    for race_id, race in all_data.items():
        ym = race_id[:4] + '-' + race_id[4:6]
        by_month[ym].append((race_id, race))

    # ============================================================
    # 全馬券種の払戻データを集計（JRA全レース実測値）
    # ============================================================
    payout_stats = defaultdict(lambda: {'count': 0, 'total_payout': 0})
    payout_dist   = defaultdict(list)  # 払戻金額の分布

    for race_id, race in all_data.items():
        payouts = race.get('payouts', {})
        for ptype, entries in payouts.items():
            for e in entries:
                p = e.get('payout', 0)
                payout_stats[ptype]['count']       += 1
                payout_stats[ptype]['total_payout'] += p
                payout_dist[ptype].append(p)

    print('=' * 70)
    print('=== JRA全レース 馬券種別 払戻統計（実測値）===')
    print(f'期間: {sorted(by_month.keys())[0]} 〜 {sorted(by_month.keys())[-1]}')
    print(f'総レース数: {len(all_data):,}')
    print()
    print(f'{"馬券種":<10} | {"組数":>8} | {"平均払戻/100円":>14} | {"回収率":>8} | {"最小":>8} | {"中央値":>8} | {"最大":>10}')
    print('-' * 75)

    ptype_order = ['単勝','複勝','枠連','馬連','ワイド','馬単','三連複','三連単','3連複','3連単']
    shown = set()
    for ptype in ptype_order:
        if ptype not in payout_stats: continue
        # 3連複と三連複を統合
        canonical = ptype.replace('3連複','三連複').replace('3連単','三連単')
        if canonical in shown: continue
        shown.add(canonical)

        s = payout_stats[ptype]
        dist = sorted(payout_dist[ptype])
        if not dist: continue
        avg  = s['total_payout'] / s['count']
        roi  = avg / 100 * 100  # 100円あたりの回収率
        med  = dist[len(dist)//2]
        mark = '✅' if roi >= 70 else '❌'
        print(f'{mark} {canonical:<8} | {s["count"]:>8,} | ¥{avg:>12,.0f} | {roi:>7.1f}% | ¥{dist[0]:>6,} | ¥{med:>6,} | ¥{dist[-1]:>8,}')

    # ============================================================
    # 上がり3F と着順の相関分析
    # ============================================================
    print()
    print('=' * 70)
    print('=== 上がり3F × 着順 相関分析 ===')
    print()

    agari_by_rank = defaultdict(list)
    for race_id, race in all_data.items():
        horses = race.get('horses', [])
        if not horses: continue
        valid = [h for h in horses if h.get('agari_3f') and h.get('finish_rank')]
        if not valid: continue
        best_agari = min(h['agari_3f'] for h in valid)
        for h in valid:
            rank = h['finish_rank']
            if rank <= 10:
                diff = h['agari_3f'] - best_agari
                agari_by_rank[rank].append(diff)

    print(f'{"着順":<6} | {"サンプル":>8} | {"平均タイム差":>12} | {"最速比率":>10}')
    print('-' * 45)
    for rank in range(1, 9):
        diffs = agari_by_rank[rank]
        if not diffs: continue
        avg_diff  = sum(diffs) / len(diffs)
        fastest_rate = sum(1 for d in diffs if d == 0.0) / len(diffs) * 100
        print(f'{rank}着 | {len(diffs):>8,} | +{avg_diff:>10.2f}秒 | {fastest_rate:>9.1f}%')

    # ============================================================
    # コーナー通過順位 × 着順 相関
    # ============================================================
    print()
    print('=== 最終コーナー順位 × 1着率 ===')
    print()

    corner_win = defaultdict(lambda: {'total':0, 'win':0})
    for race_id, race in all_data.items():
        horses = race.get('horses', [])
        field_size = len(horses)
        if field_size < 6: continue
        for h in horses:
            lc = h.get('last_corner')
            fr = h.get('finish_rank')
            if lc and fr:
                pos_group = '1-3位' if lc <= 3 else '4-6位' if lc <= 6 else '7位以下'
                corner_win[pos_group]['total'] += 1
                if fr == 1: corner_win[pos_group]['win'] += 1

    print(f'{"最終コーナー":<10} | {"頭数":>8} | {"1着率":>8} | {"3着以内率":>10}')
    print('-' * 45)

    corner_top3 = defaultdict(lambda: {'total':0, 'top3':0})
    for race_id, race in all_data.items():
        horses = race.get('horses', [])
        if len(horses) < 6: continue
        for h in horses:
            lc = h.get('last_corner')
            fr = h.get('finish_rank')
            if lc and fr:
                pos_group = '1-3位' if lc <= 3 else '4-6位' if lc <= 6 else '7位以下'
                corner_top3[pos_group]['total'] += 1
                if fr <= 3: corner_top3[pos_group]['top3'] += 1

    for grp in ['1-3位', '4-6位', '7位以下']:
        w = corner_win[grp]
        t = corner_top3[grp]
        if w['total'] == 0: continue
        win_rate  = w['win']  / w['total']  * 100
        top3_rate = t['top3'] / t['total']  * 100
        print(f'{grp:<10} | {w["total"]:>8,} | {win_rate:>7.1f}% | {top3_rate:>9.1f}%')

    # ============================================================
    # 月別払戻サマリ
    # ============================================================
    print()
    print('=' * 70)
    print('=== 月別データ件数 ===')
    print()
    for ym in sorted(by_month.keys()):
        races = by_month[ym]
        total_payouts = sum(len(r.get('payouts',{})) for _,r in races)
        has_agari = sum(1 for _,r in races if any(h.get('agari_3f') for h in r.get('horses',[])))
        print(f'  {ym}: {len(races)}レース / 払戻{total_payouts}種 / 上がり取得{has_agari}レース')

if __name__ == '__main__':
    data_dir = sys.argv[1] if len(sys.argv) > 1 else 'data'
    run(data_dir)

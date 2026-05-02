# -*- coding: utf-8 -*-
"""
backtest_ml_april2026.py
2026年4月の全JSON → ML三連複予測 → 実際の結果と照合してROI算出
"""
import os, sys, json, csv, re
from collections import defaultdict

os.environ['PYTHONIOENCODING'] = 'utf-8'

import lightgbm as lgb
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats, should_exclude_uscore
from ml_features import race_ml_probs
from predict_ml_sanrenpuku import (
    load_models, json_to_info, predict, format_result_text,
    RNUM_MIN, RNUM_MAX, MAX_FIELD, EXCLUDE_KEYWORDS, BET, VENUE_MAP
)

DATA_DIR   = 'data'
RESULT_CSV       = 'data/raceresults_202604.csv'
RESULT_CSV_EXTRA = 'data/raceresults_202604_extra.csv'
APRIL_JSON = [
    'data/races_20260405.json',
    'data/races_20260406.json',
    'data/races_20260412.json',
    'data/races_20260413.json',
    'data/races_20260418.json',
]

# ── 結果CSV読み込み ──────────────────────────────────────────
def load_results(path):
    """race_id → {umaban: rank} と三連複払戻 を返す"""
    ranks   = defaultdict(dict)   # race_id -> {umaban(int): rank(int)}
    sanfuku = {}                  # race_id -> payout(int)
    if not os.path.exists(path):
        print(f'[WARN] 結果CSVなし: {path}')
        return ranks, sanfuku
    with open(path, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            rid  = row.get('race_id', '').strip()
            rank = row.get('着順', '').strip()
            ub   = row.get('馬番', '').strip()
            try:
                ranks[rid][int(ub)] = int(rank)
            except Exception:
                pass
            # 三連複払戻: "5 - 8 - 12:1590" or plain "3870" 形式
            pay_raw = row.get('三連複払戻', '').strip()
            if pay_raw and rid not in sanfuku:
                m = re.search(r'(\d[\d,]+)', pay_raw)
                if m:
                    sanfuku[rid] = int(m.group(1).replace(',', ''))
    return ranks, sanfuku

# ── メイン ──────────────────────────────────────────────────
def main():
    print('=' * 72)
    print('  2026年4月 ML三連複バックテスト')
    print('=' * 72)

    turf_win, turf_pl, dirt_win, dirt_pl = load_models()

    print('過去データ読み込み中...')
    races = load_all_csv_races(DATA_DIR)
    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym='202604')
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'  horse_db: {len(horse_db):,}頭  準備完了\n')

    ranks, sanfuku = load_results(RESULT_CSV)
    r2, s2 = load_results(RESULT_CSV_EXTRA)
    ranks.update(r2)
    sanfuku.update(s2)
    print(f'  結果CSV: {len(ranks)} レース分の着順  三連複払戻: {len(sanfuku)} レース\n')

    total_bet = 0
    total_ret = 0
    hit       = 0
    total     = 0
    details   = []

    for json_path in APRIL_JSON:
        if not os.path.exists(json_path):
            print(f'[SKIP] {json_path}')
            continue
        with open(json_path, encoding='utf-8') as f:
            race_data = json.load(f)

        date_str = race_data.get('date', '')
        file_ym  = date_str[:6] if date_str else '202604'
        all_races = race_data.get('all_races', [])
        print(f'─── {date_str}  {len(all_races)}レース ───')

        for race in all_races:
            race_id   = race.get('race_id', '')
            race_name = race.get('race_name', '')

            rnum = int(race_id[-2:]) if race_id else 0
            if not (RNUM_MIN <= rnum <= RNUM_MAX):
                continue
            if any(kw in race_name for kw in EXCLUDE_KEYWORDS):
                continue
            n_horses = race.get('n_horses', 0)
            if n_horses > MAX_FIELD:
                continue
            if all(float(o.get('odds', 0) or 0) == 0.0 for o in race.get('odds', [])):
                continue

            info = json_to_info(race, file_ym)
            res  = predict(race_id, info, horse_db, trainer_stats, jockey_stats,
                           turf_win, turf_pl, dirt_win, dirt_pl)
            if res is None:
                continue

            total += 1
            total_bet += res['bet_total']
            venue = VENUE_MAP.get(race_id[4:6], '?')

            # 着順データと照合
            race_ranks = ranks.get(race_id, {})
            payout     = sanfuku.get(race_id, 0)

            if race_ranks:
                # 買い目の三連複が的中したか (1軸ながし: combos リストで照合)
                top3 = {ub for ub, r in race_ranks.items() if r <= 3}

                hit_flag = False
                for u1, u2, u3 in res.get('combos', []):
                    try:
                        if {int(u1), int(u2), int(u3)} == top3:
                            hit_flag = True
                            break
                    except Exception:
                        pass

                if hit_flag:
                    hit += 1
                    if payout:
                        ret = payout * BET // 100
                        total_ret += ret
                        result_str = f'【★的中 ¥{ret:,}】'
                    else:
                        ret = 0
                        result_str = f'【★的中 払戻不明】'
                else:
                    ret = 0
                    top3_sorted = sorted(top3)
                    result_str = f'【外れ 3着={top3_sorted}】'
            else:
                ret = 0
                result_str = '【結果なし】'

            course_tag = 'ダ' if res['is_dirt'] else '芝'
            aite_nums  = ','.join(res['uma_aite'])
            print(f'  {venue}{rnum}R {race_name[:12]:<12} '
                  f'三連複 ◎{res["uma_honmei"]}×{aite_nums}  '
                  f'{result_str}')
            details.append({
                'race_id': race_id, 'name': race_name, 'ret': ret,
                'hit': hit_flag if race_ranks else None,
            })

        print()

    # ── サマリー ──────────────────────────────────────────────
    print('=' * 72)
    print(f'  対象レース:  {total}R')
    print(f'  合計投資:    ¥{total_bet:,}')
    print(f'  的中:        {hit}R / {total}R  ({hit/total*100:.1f}%)' if total else '  的中: -')
    print(f'  合計回収:    ¥{total_ret:,}')
    roi = total_ret / total_bet * 100 if total_bet else 0
    pl  = total_ret - total_bet
    print(f'  ROI:         {roi:.1f}%')
    print(f'  損益:        {pl:+,}円')
    print('=' * 72)

if __name__ == '__main__':
    main()

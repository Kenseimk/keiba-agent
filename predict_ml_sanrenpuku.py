# -*- coding: utf-8 -*-
"""
predict_ml_sanrenpuku.py  三連複 ML 実戦予測スクリプト
=======================================================
バックテスト最適化済みの最終戦略を使って、当日レースの買い目を出力する。

【最終戦略】
  芝: ◎ win_prob 25〜30% のレースのみ対象
  ダート: ◎ win_prob ≥ 20%
  共通: ◎+○ win_prob ≥ 45% / 1軸×6頭ながし C(6,2)=15通 ¥1,500/R
  モデル: 芝/ダート別 LightGBM × 4アンサンブル

【バックテスト実績 (4月2026)】
  1軸×6頭: 的中2/4(50%) ROI 227.5% +7,650円

使い方:
  python predict_ml_sanrenpuku.py --json data/races_20260411.json
  python predict_ml_sanrenpuku.py --date 20260411
  python predict_ml_sanrenpuku.py --json data/races_20260411.json --no-discord
"""
import os, sys, json, re, glob, datetime, argparse, itertools
os.environ['PYTHONIOENCODING'] = 'utf-8'


import lightgbm as lgb
import requests
from collections import defaultdict

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import build_trainer_stats, build_jockey_stats, should_exclude_uscore
from ml_features import race_ml_probs

# ── 戦略パラメータ ────────────────────────────────────
RNUM_MIN        = 8
RNUM_MAX        = 11
MAX_FIELD       = 14
HONMEI_WP_MIN   = 20.0   # ◎ win_prob 下限 (共通)
TURF_WP_MIN     = 25.0   # 芝: ◎ wp 下限
TURF_WP_MAX     = 30.0   # 芝: ◎ wp 上限 (バンドフィルター)
WP_SUM_MIN      = 40.0   # ◎+○ win_prob 合計下限
N_AITE          = 6      # 1軸ながし相手頭数 C(6,2)=15通
BET             = 100

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
}

EXCLUDE_KEYWORDS = ['新馬', '未勝利', '1勝クラス', '500万下', '障害', 'ハードル', 'スティープル', 'JS']


def _venue(race_id):
    return VENUE_MAP.get(race_id[4:6], '?') if len(race_id) >= 6 else '?'


def load_models():
    """芝/ダート別アンサンブルモデルをロード"""
    print('LightGBM モデル読み込み中...')
    turf_win  = ([lgb.Booster(model_file='ml_model_win_turf.txt')] +
                 [lgb.Booster(model_file=f'ml_model_win_turf_e{i}.txt') for i in range(3)])
    turf_pl   = ([lgb.Booster(model_file='ml_model_place_turf.txt')] +
                 [lgb.Booster(model_file=f'ml_model_place_turf_e{i}.txt') for i in range(3)])
    dirt_win  = ([lgb.Booster(model_file='ml_model_win_dirt.txt')] +
                 [lgb.Booster(model_file=f'ml_model_win_dirt_e{i}.txt') for i in range(3)])
    dirt_pl   = ([lgb.Booster(model_file='ml_model_place_dirt.txt')] +
                 [lgb.Booster(model_file=f'ml_model_place_dirt_e{i}.txt') for i in range(3)])
    print(f'  → 芝×4 / ダート×4 読み込み完了\n')
    return turf_win, turf_pl, dirt_win, dirt_pl


def csv_to_races(csv_path: str, file_ym: str) -> list:
    """predict_input.csv (scrape_entries v2出力) を race_ml_probs() が受け付ける
    info 形式のリストに変換する。raw_race_id/dist/course 列が必要。
    """
    import csv as csv_mod
    races_by_id: dict = {}
    with open(csv_path, encoding='utf-8-sig') as f:
        for row in csv_mod.DictReader(f):
            raw_id = row.get('raw_race_id', '').strip()
            if not raw_id or len(raw_id) != 12:
                continue
            if raw_id not in races_by_id:
                label = row.get('race_id', '')
                parts = label.split('_', 2)
                race_name = parts[2] if len(parts) >= 3 else (parts[-1] if parts else '')
                try:
                    dist_val = int(row.get('dist', 0) or 0)
                except Exception:
                    dist_val = 0
                venue_code = raw_id[4:6] if len(raw_id) >= 6 else ''
                races_by_id[raw_id] = {
                    'race_id':    raw_id,
                    'race_name':  race_name,
                    'dist':       dist_val,
                    'course':     row.get('course', '芝') or '芝',
                    'n_field':    0,
                    'n_horses':   0,
                    'venue_code': venue_code,
                    'track_cond': '',
                    'file_ym':    file_ym,
                    'horse_list': [],
                    'odds':       [],
                }
            race = races_by_id[raw_id]
            name = row.get('馬名', '').strip()
            if not name:
                continue
            try:
                odds_val = float(row.get('単勝オッズ') or 0)
                pop_val  = int(row.get('人気') or 99)
            except Exception:
                odds_val, pop_val = 0.0, 99
            try:
                gate_num = int(row.get('馬番', 0) or 0)
                umaban   = str(gate_num) if gate_num else ''
            except Exception:
                gate_num, umaban = 0, ''
            race['horse_list'].append({
                'name':     name,
                'jockey':   row.get('騎手', '').strip(),
                'odds':     odds_val,
                'pop':      pop_val,
                'gate_num': gate_num,
                'umaban':   umaban,
                'rank':     0,
                'bw':       None,
                'kg':       None,
                'trainer':  '',
            })
            race['odds'].append({'name': name, 'odds': odds_val})
    for race in races_by_id.values():
        race['n_field']  = len(race['horse_list'])
        race['n_horses'] = race['n_field']
        # 馬番重複（枠番取得バグ）を連番で修正
        umabans = [h['umaban'] for h in race['horse_list']]
        if len(umabans) != len(set(umabans)):
            for i, h in enumerate(race['horse_list']):
                h['umaban']   = str(i + 1)
                h['gate_num'] = i + 1
    return list(races_by_id.values())


def json_to_info(race: dict, file_ym: str) -> dict:
    """
    fetch_race.py が出力する JSON の1レース辞書を
    race_ml_probs() が受け付ける info 形式に変換する。

    horse_list の各要素:
      name, jockey, odds, pop, gate_num, umaban, rank, bw, kg, trainer
    """
    # odds を名前→{odds, pop} のマップへ
    odds_by_name = {o['name']: o for o in race.get('odds', [])}

    horse_list = []
    for i, h in enumerate(race.get('horses', [])):
        name   = h['name']
        jockey = h.get('jockey', '')
        o      = odds_by_name.get(name, {})
        umaban = i + 1  # JSON 内の馬リスト順 ≒ 馬番

        horse_list.append({
            'name':     name,
            'jockey':   jockey,
            'odds':     float(o.get('odds', 0.0) or 0.0),
            'pop':      int(o.get('pop', 99) or 99),
            'gate_num': umaban,
            'umaban':   str(umaban),
            'rank':     0,     # 当日は未確定
            'bw':       None,
            'kg':       None,
            'trainer':  '',
        })

    race_id    = race['race_id']
    venue_code = race_id[4:6] if len(race_id) >= 6 else ''

    return {
        'race_id':    race_id,
        'race_name':  race.get('race_name', ''),
        'dist':       int(race.get('dist', 0) or 0),
        'course':     race.get('course', 'ダート'),
        'n_field':    len(horse_list),
        'venue_code': venue_code,
        'track_cond': race.get('track_cond', ''),
        'file_ym':    file_ym,
        'horse_list': horse_list,
    }


def send_discord(webhook_url: str, embeds: list) -> None:
    if not webhook_url:
        print('[Discord] DISCORD_WEBHOOK_URL 未設定 - スキップ')
        return
    for i in range(0, len(embeds), 10):
        chunk = embeds[i:i + 10]
        r = requests.post(webhook_url, json={'embeds': chunk}, timeout=15)
        if r.status_code in (200, 204):
            print(f'  ✅ Discord送信完了 ({len(chunk)}件)')
        else:
            print(f'  [WARN] Discord送信失敗: {r.status_code} {r.text[:100]}')


def predict(race_id, info, horse_db, trainer_stats, jockey_stats,
            turf_win, turf_pl, dirt_win, dirt_pl):
    """
    1レース分の買い目を計算して返す。
    戻り値: None (非対象) または {
        'race_id', 'race_name', 'venue', 'honmei', 'renka', 'aite',
        'n_tickets', 'bet_total', 'is_dirt',
        'wp_honmei', 'pp_renka', 'wp_sum',
    }
    """
    is_dirt = 'ダ' in str(info.get('course', ''))
    mw = dirt_win if is_dirt else turf_win
    mp = dirt_pl  if is_dirt else turf_pl

    sc = race_ml_probs(race_id, info, horse_db, trainer_stats, jockey_stats, mw, mp)
    if not sc or len(sc) < 4:
        return None

    wp_map   = {h['name']: h['win_prob']   for h in sc}
    pp_map   = {h['name']: h['place_prob'] for h in sc}
    uma_map  = {h['name']: h['umaban']     for h in sc}
    odds_map = {h['name']: h['odds']       for h in sc}

    sorted_wp = sorted(sc, key=lambda h: h['win_prob'],   reverse=True)
    sorted_pp = sorted(sc, key=lambda h: h['place_prob'], reverse=True)

    hn = sorted_wp[0]['name']
    rn = next((h['name'] for h in sorted_pp if h['name'] != hn), None)
    if not rn:
        return None

    wp_h   = wp_map.get(hn, 0)
    wp_r   = wp_map.get(rn, 0)
    wp_sum = wp_h + wp_r

    # ── フィルター ──
    if wp_sum < WP_SUM_MIN:
        return None

    if is_dirt:
        if wp_h < HONMEI_WP_MIN:
            return None
    else:
        # 芝: 25〜30% バンド
        if not (TURF_WP_MIN <= wp_h < TURF_WP_MAX):
            return None

    # ── 1軸×N_AITE頭ながし ──
    aite_sorted = sorted(sc, key=lambda h: h['place_prob'], reverse=True)
    aite_cands  = [h['name'] for h in aite_sorted if h['name'] != hn]
    aite        = aite_cands[:N_AITE]

    u_h = uma_map.get(hn, '')
    u_a = [uma_map.get(n, '') for n in aite if uma_map.get(n, '')]
    if not u_h or len(u_a) < 2:
        return None

    combos    = [(u_h, a, b) for a, b in itertools.combinations(u_a, 2)]
    n_tickets = len(combos)
    return {
        'race_id':    race_id,
        'race_name':  info.get('race_name', ''),
        'venue':      _venue(race_id),
        'rnum':       int(race_id[-2:]),
        'course':     info.get('course', ''),
        'dist':       info.get('dist', 0),
        'is_dirt':    is_dirt,
        'honmei':     hn,
        'renka':      rn,
        'aite':       aite,
        'uma_honmei': u_h,
        'uma_renka':  uma_map.get(rn, ''),
        'uma_aite':   u_a,
        'combos':     combos,
        'wp_honmei':  wp_h,
        'pp_renka':   pp_map.get(rn, 0),
        'wp_sum':     wp_sum,
        'odds_h':     odds_map.get(hn, 0),
        'odds_r':     odds_map.get(rn, 0),
        'n_tickets':  n_tickets,
        'bet_total':  n_tickets * BET,
        'sc':         sc,
    }


def format_result_text(res: dict) -> str:
    """コンソール出力用テキスト"""
    course_tag = 'ダート' if res['is_dirt'] else '芝'
    lines = [
        f'  {res["venue"]} {res["rnum"]}R  {res["race_name"]}  '
        f'{course_tag}{res["dist"]}m',
        f'  ◎ {res["honmei"]} (wp{res["wp_honmei"]:.1f}%  odds{res["odds_h"]:.1f}倍)',
        f'  相手 ({len(res["aite"])}頭): {" / ".join(res["aite"])}',
        f'  買い目: 三連複 ◎1軸×{len(res["uma_aite"])}頭 = {res["n_tickets"]}通 / ¥{res["bet_total"]}',
        f'  馬番: ◎{res["uma_honmei"]} 相手{",".join(res["uma_aite"])}',
    ]
    return '\n'.join(lines)


def format_embed(res: dict) -> dict:
    """Discord embed 生成"""
    course_tag = 'ダート' if res['is_dirt'] else '芝'
    aite_str   = ' / '.join(
        f'{n}({u}番)' for n, u in zip(res['aite'], res['uma_aite'])
    )
    desc = (
        f'**◎ {res["honmei"]}** ({res["uma_honmei"]}番)  '
        f'wp{res["wp_honmei"]:.1f}%  odds{res["odds_h"]:.1f}倍\n'
        f'**相手** {aite_str}\n'
        f'**買い目** 三連複 ◎1軸 × {len(res["uma_aite"])}頭 = '
        f'**{res["n_tickets"]}通 / ¥{res["bet_total"]}**'
    )
    return {
        'title':       f'🎯 {res["venue"]} {res["rnum"]}R  {res["race_name"]}  '
                       f'{course_tag}{res["dist"]}m',
        'description': desc,
        'color':       0x1a73e8 if not res['is_dirt'] else 0xe67e22,
    }


def main():
    parser = argparse.ArgumentParser(description='三連複 ML 実戦予測')
    parser.add_argument('--date',       help='対象日 YYYYMMDD (省略=今日)')
    parser.add_argument('--json',       help='レースJSONファイルパス (fetch_race出力)')
    parser.add_argument('--input',      help='出馬表CSV (scrape_entries出力、--json の代替)')
    parser.add_argument('--data',       default='data', help='CSVデータディレクトリ')
    parser.add_argument('--no-discord', action='store_true')
    parser.add_argument('--verbose',    action='store_true', help='全馬スコア表示')
    args = parser.parse_args()

    date_str   = args.date or datetime.date.today().strftime('%Y%m%d')
    today_disp = f'{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}'
    file_ym    = date_str[:6]  # YYYYMM

    print('=' * 72)
    print(f'  三連複 ML 予測  {today_disp}')
    print(f'  芝: wp {TURF_WP_MIN}-{TURF_WP_MAX}%  /  ダート: wp≥{HONMEI_WP_MIN}%')
    print(f'  ◎+○ ≥{WP_SUM_MIN}%  /  1軸×{N_AITE}頭ながし C({N_AITE},2)={N_AITE*(N_AITE-1)//2}通')
    print('=' * 72)

    # ── モデルロード ──
    turf_win, turf_pl, dirt_win, dirt_pl = load_models()

    # ── 過去データ読み込み ──
    print('過去データ読み込み中...')
    races = load_all_csv_races(args.data)
    print(f'  全レース: {len(races):,}R')

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=file_ym)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    trainer_stats = build_trainer_stats(horse_db)
    jockey_stats  = build_jockey_stats(horse_db)
    print(f'  horse_db: {len(horse_db):,}頭  準備完了\n')

    # ── レースデータ取得 ──
    if args.input:
        # CSV モード (scrape_entries 出力)
        print(f'レースデータ読み込み (CSV): {args.input}')
        all_races = csv_to_races(args.input, file_ym)
        print(f'  {len(all_races)} レース取得\n')
    elif args.json:
        json_path = args.json
        print(f'レースデータ読み込み: {json_path}')
        with open(json_path, encoding='utf-8') as f:
            race_data = json.load(f)
        json_date = race_data.get('date', date_str)
        if json_date != date_str:
            file_ym    = json_date[:6]
            date_str   = json_date
            today_disp = f'{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}'
            print(f'  対象日: {today_disp}')
        all_races = race_data.get('all_races', [])
        print(f'  {len(all_races)} レース取得\n')
    else:
        # 日付からデフォルトJSONパスを探す
        json_path = f'{args.data}/races_{date_str}.json'
        if not os.path.exists(json_path):
            alt = f'{args.data}/races_all_{date_str}.json'
            if os.path.exists(alt):
                json_path = alt
            else:
                print(f'[ERROR] JSONファイルが見つかりません: {json_path}')
                print('  --input predict_input.csv か --json <path> を指定してください')
                sys.exit(1)
        print(f'レースデータ読み込み: {json_path}')
        with open(json_path, encoding='utf-8') as f:
            race_data = json.load(f)
        json_date = race_data.get('date', date_str)
        if json_date != date_str:
            file_ym    = json_date[:6]
            date_str   = json_date
            today_disp = f'{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}'
            print(f'  対象日: {today_disp}')
        all_races = race_data.get('all_races', [])
        print(f'  {len(all_races)} レース取得\n')

    # ── 予測実行 ──
    results   = []
    skipped   = []
    total_bet = 0

    print('─' * 72)
    for race in all_races:
        race_id   = race.get('race_id', '')
        race_name = race.get('race_name', '')

        # 基本フィルター
        rnum = int(race_id[-2:]) if race_id else 0
        if not (RNUM_MIN <= rnum <= RNUM_MAX):
            continue

        if any(kw in race_name for kw in EXCLUDE_KEYWORDS):
            print(f'  [除外] {_venue(race_id)} {rnum}R {race_name}')
            continue

        n_horses = race.get('n_horses', 0)
        if n_horses > MAX_FIELD:
            print(f'  [除外] {_venue(race_id)} {rnum}R {race_name} ({n_horses}頭 > {MAX_FIELD})')
            continue

        # オッズが全馬0のレースをスキップ
        if all(float(o.get('odds', 0) or 0) == 0.0 for o in race.get('odds', [])):
            print(f'  [除外] {_venue(race_id)} {rnum}R {race_name} (オッズ未取得)')
            continue

        info = race if args.input else json_to_info(race, file_ym)
        res  = predict(race_id, info, horse_db, trainer_stats, jockey_stats,
                       turf_win, turf_pl, dirt_win, dirt_pl)

        if res is None:
            course_type = 'ダート' if 'ダ' in str(race.get('course', '')) else '芝'
            print(f'  [対象外] {_venue(race_id)} {rnum}R {race_name} ({course_type}) '
                  f'- フィルター不通過')
            skipped.append(race_id)
        else:
            results.append(res)
            total_bet += res['bet_total']
            print(f'  [★対象] {format_result_text(res)}')
            if args.verbose:
                print('  --- 全馬スコア ---')
                for h in sorted(res['sc'], key=lambda x: x['win_prob'], reverse=True):
                    print(f'      {h["name"]:12} wp{h["win_prob"]:5.1f}% '
                          f'pp{h["place_prob"]:5.1f}% odds{h["odds"]:5.1f}倍')
            print()

    # ── サマリー ──
    print('=' * 72)
    if not results:
        print('  本日の買い目: なし (全レースがフィルター条件を満たさず)')
    else:
        print(f'  本日の買い目: {len(results)} レース  合計 ¥{total_bet:,}')
        print()
        for res in results:
            course_tag = 'ダート' if res['is_dirt'] else '芝'
            aite_nums  = ','.join(res['uma_aite'])
            print(f'  {res["venue"]} {res["rnum"]}R [{course_tag}]  '
                  f'三連複 ◎{res["uma_honmei"]} × {aite_nums}  '
                  f'{res["n_tickets"]}通 ¥{res["bet_total"]}')
        print(f'\n  合計投資: ¥{total_bet:,}')
    print('=' * 72)

    # ── Discord ──
    if not args.no_discord:
        webhook_url = os.environ.get('DISCORD_WEBHOOK_URL', '')
        embeds = []

        embeds.append({
            'title':       f'🏇 三連複 ML 予測  {today_disp}',
            'description': (
                f'芝: wp{TURF_WP_MIN}-{TURF_WP_MAX}%  /  ダート: wp≥{HONMEI_WP_MIN}%\n'
                f'対象: **{len(results)}レース**  合計: **¥{total_bet:,}**'
            ) if results else '本日の買い目なし',
            'color': 0x2ecc71,
        })

        for res in results:
            embeds.append(format_embed(res))

        if webhook_url:
            send_discord(webhook_url, embeds)
        else:
            print('\n[Discord] DISCORD_WEBHOOK_URL が未設定です')
            print('  設定方法: set DISCORD_WEBHOOK_URL=<url>  (Windows)')


if __name__ == '__main__':
    main()

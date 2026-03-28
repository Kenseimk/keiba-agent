"""
judge.py - JRA URLを貼るだけで賭け判断を出力

使い方:
  python judge.py --url "https://jra.jp/JRADB/accessD.html?CNAME=pw01dde0106202603011120260328/6C"
  python judge.py --url "..." --capital 70000 --data data
"""
import sys, io, os, re, time, argparse, requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest_core import load_data, update_prev_history, top3_prob
from predict import ANA_PARAMS, FUKUSHO_PARAMS
from scrape_entries import fetch_race_ids, fetch_shutuba, fetch_odds, HEADERS
from collections import defaultdict

# JRA会場コード → netkeiba会場コード
JRA_TO_NETKEIBA = {
    '01': '01',  # 札幌
    '02': '02',  # 函館
    '03': '03',  # 福島
    '04': '04',  # 新潟
    '05': '05',  # 東京
    '06': '09',  # 中山
    '07': '06',  # 中京
    '08': '10',  # 京都
    '09': '07',  # 阪神
    '10': '08',  # 小倉
}

VENUE_NAMES = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
    '05': '東京', '06': '中山', '07': '中京', '08': '京都',
    '09': '阪神', '10': '小倉',
}


def parse_jra_url(url):
    """JRA URLからレース情報を抽出

    CNAME例: pw01dde0106202603011120260328/6C
      pw01dde = 固定プレフィックス
      01      = 不使用
      06      = JRA会場コード (06=中山)
      2026    = 年
      03      = 開催回
      01      = 開催日
      11      = レース番号
      20260328 = 日付
    """
    # pw + 5文字 + 22桁数字 のパターン
    m = re.search(r'CNAME=pw\w{5}(\d{22})', url)
    if not m:
        raise ValueError(f'JRA URL からレース情報を取得できません: {url}')
    cname = m.group(1)  # 0106202603011120260328
    jra_venue = cname[2:4]    # 06 = 中山
    race_num  = cname[12:14]  # 11
    date_str  = cname[14:22]  # 20260328
    return jra_venue, race_num, date_str


def find_netkeiba_race_id(session, date_str, jra_venue, race_num):
    """netkeiba の race_id を日付・会場・レース番号で特定"""
    netkeiba_venue = JRA_TO_NETKEIBA.get(jra_venue)
    if not netkeiba_venue:
        raise ValueError(f'未対応の会場コード: {jra_venue}')

    race_ids = fetch_race_ids(session, date_str)
    race_num_padded = race_num.zfill(2)

    for rid in race_ids:
        if rid[4:6] == netkeiba_venue and rid[10:12] == race_num_padded:
            return rid

    return None


def build_prev_history(data_dir):
    """過去データから前走履歴を構築"""
    races_hist, _ = load_data(data_dir)
    by_month = defaultdict(list)
    for rid, info in races_hist.items():
        by_month[info['ym']].append((rid, info))

    prev_history, prev_history2 = {}, {}
    for ym in sorted(by_month.keys()):
        for rid, info in by_month[ym]:
            update_prev_history(info['horses'], prev_history, prev_history2)
    return prev_history


def judge_ana(horses, race_id_label, capital):
    """穴馬複勝戦略の判定"""
    p = ANA_PARAMS
    if len(horses) < p['field_min']:
        return None

    fav_odds  = min(h['odds'] for h in horses)
    field_size = len(horses)
    best = None

    for h in horses:
        if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
        if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
        prob = top3_prob(h['odds'], fav_odds, field_size, h['popularity'])
        if prob >= p['prob_min']:
            if best is None or prob > best['prob']:
                bet = 0
                for thresh, pct, cap_limit in p['kelly_tiers']:
                    if prob >= thresh:
                        bet = min(int(capital * pct / 100) * 100, cap_limit)
                        break
                best = {**h, 'prob': prob, 'bet': bet,
                        'fav_odds': fav_odds, 'field_size': field_size}
    return best


def judge_fukusho(horses, prev_history, race_id_label, capital):
    """隠れ末脚型複勝戦略の判定"""
    p = FUKUSHO_PARAMS
    if len(horses) < p['field_min']:
        return None

    best = None
    for h in horses:
        if not (p['pop_min'] <= h['popularity'] <= p['pop_max']): continue
        if not (p['odds_min'] <= h['odds'] < p['odds_max']): continue
        ph = prev_history.get(h['name'])
        if ph is None: continue
        if ph['field_size'] < p['prev_field_min']: continue
        if ph['f3rank'] > p['prev_f3rank_max']: continue
        if ph['finish_rank'] < p['prev_finish_min']: continue
        bet = min(int(capital * p['kelly_pct'] / 100) * 100, p['kelly_max'])
        if best is None or h['odds'] > best['odds']:
            best = {**h, 'bet': bet,
                    'prev_f3rank': ph['f3rank'],
                    'prev_finish': ph['finish_rank'],
                    'prev_corner': ph.get('last_corner', '?'),
                    'prev_field':  ph['field_size'],
                    'f3_adv':      ph.get('f3_advantage', 0.0)}
    return best


def main():
    parser = argparse.ArgumentParser(description='JRA URLから賭け判断を出力')
    parser.add_argument('--url',     required=True, help='JRA レースURL')
    parser.add_argument('--capital', type=int, default=70000, help='軍資金')
    parser.add_argument('--data',    default='data', help='過去データディレクトリ')
    args = parser.parse_args()

    # ── URL解析 ──────────────────────────────────────────
    try:
        jra_venue, race_num, date_str = parse_jra_url(args.url)
    except ValueError as e:
        print(f'[ERROR] {e}')
        sys.exit(1)

    venue_name = VENUE_NAMES.get(jra_venue, jra_venue)
    print(f'対象レース: {date_str} {venue_name}{int(race_num)}R')

    # ── netkeiba race_id 特定 ──────────────────────────
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get('https://race.netkeiba.com/', timeout=10)
    time.sleep(1)

    print('レースID検索中...')
    race_id = find_netkeiba_race_id(session, date_str, jra_venue, race_num)
    if not race_id:
        print(f'[ERROR] netkeiba で {date_str} {venue_name}{int(race_num)}R が見つかりません')
        sys.exit(1)
    print(f'  → netkeiba race_id: {race_id}')

    # ── 出馬表・オッズ取得 ────────────────────────────
    print('出馬表取得中...')
    horses_raw, race_name = fetch_shutuba(session, race_id)
    time.sleep(2)
    if not horses_raw:
        print('[ERROR] 出馬表が取得できませんでした')
        sys.exit(1)
    print(f'  → {race_name} / {len(horses_raw)}頭')

    print('オッズ取得中...')
    odds_map = fetch_odds(session, race_id)
    time.sleep(2)
    print(f'  → {len(odds_map)}件')

    # 馬データを結合
    horses = []
    for h in horses_raw:
        umaban_key = str(int(h['umaban'])) if h['umaban'].isdigit() else h['umaban']
        odds_info  = odds_map.get(umaban_key, (None, None))
        final_odds = odds_info[0] if odds_info[0] is not None else h.get('shutuba_odds')
        final_pop  = odds_info[1] if odds_info[1] is not None else h.get('shutuba_pop')
        if final_odds and final_pop:
            try:
                horses.append({
                    'name':       h['name'],
                    'odds':       float(final_odds),
                    'popularity': int(final_pop),
                    'jockey':     h['jockey'],
                    'weight':     h['weight'],
                    'umaban':     h['umaban'],
                })
            except Exception:
                pass

    if not horses:
        print('[ERROR] オッズ付きの馬データが取得できませんでした')
        sys.exit(1)

    # ── 前走履歴構築 ─────────────────────────────────
    print('過去データ読み込み中...')
    prev_history = build_prev_history(args.data)
    print(f'  → {len(prev_history)}頭の前走データ構築完了')

    race_label = f'{date_str}_{venue_name}{int(race_num)}R_{race_name}'

    # ── 判定 ─────────────────────────────────────────
    print()
    print('=' * 60)
    print(f'【賭け判断】{race_label}')
    print(f'  {len(horses)}頭立て / 1番人気オッズ: {min(h["odds"] for h in horses):.1f}倍')
    print('=' * 60)

    bet_any = False

    # 穴馬複勝
    ana = judge_ana(horses, race_label, args.capital)
    print()
    print('🎯 穴馬複勝戦略')
    if ana:
        bet_any = True
        print(f'  ✅ ベット推奨: {ana["name"]}')
        print(f'     オッズ {ana["odds"]:.1f}倍 / {ana["popularity"]}番人気 / 複勝確率 {ana["prob"]:.1f}%')
        print(f'     → ¥{ana["bet"]:,} ベット')
    else:
        print('  ❌ 対象馬なし')

    # 隠れ末脚型複勝
    fuk = judge_fukusho(horses, prev_history, race_label, args.capital)
    print()
    print('⚡ 隠れ末脚型複勝戦略')
    if fuk:
        bet_any = True
        adv = f' / 2位より{fuk["f3_adv"]:.1f}秒速い' if fuk.get('f3_adv', 0) > 0 else ''
        print(f'  ✅ ベット推奨: {fuk["name"]}')
        print(f'     オッズ {fuk["odds"]:.1f}倍 / {fuk["popularity"]}番人気')
        print(f'     前走: 上がり{fuk["prev_f3rank"]}位 / {fuk["prev_finish"]}着 / '
              f'最終コーナー{fuk["prev_corner"]}番手{adv}')
        print(f'     → ¥{fuk["bet"]:,} ベット')
    else:
        print('  ❌ 対象馬なし')

    print()
    print('=' * 60)
    if bet_any:
        total = (ana['bet'] if ana else 0) + (fuk['bet'] if fuk else 0)
        print(f'📌 結論: ベットあり（合計 ¥{total:,}）')
    else:
        print('📌 結論: このレースはスルー')
    print('=' * 60)


if __name__ == '__main__':
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

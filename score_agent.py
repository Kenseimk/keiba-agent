"""
score_agent.py  スコアエージェント メインスクリプト
======================================================
R8〜R11 を対象に全馬をスコア化し、1〜3着を予測してDiscordに通知する。

使い方:
  python score_agent.py                       # 今日の日付で実行
  python score_agent.py --date 20260328       # 日付指定
  python score_agent.py --json data/races_20260328.json  # キャッシュ使用
  python score_agent.py --no-discord          # Discord通知なし

環境変数:
  DISCORD_WEBHOOK_URL  Discordの Incoming Webhook URL
"""

import os, sys, io, json, datetime, argparse, requests

try:
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from score_agent_core import (
    load_history_db, load_models,
    score_all_horses, should_exclude, WEIGHT_PRESETS,
    compute_horse_factors, _float, _int,
)
from walkforward_winprob import (
    race_relative_features, predict_prob, load_model,
)
import numpy as np

# ─── 定数 ─────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')

# ─── ベット推奨条件 ────────────────────────────────────
BET_CONDITION = {
    'min_field':   14,
    'min_gap':     2.0,
    'odds_min':    3.0,
    'odds_max':   10.0,
    'course':     '芝',
    'min_dist':   1600,
    'track_cond': '良',
}

def check_bet(race_info: dict, scored_san: list) -> dict | None:
    """ベット推奨条件を満たすか判定。満たす場合は判定情報を返す"""
    c = BET_CONDITION
    # 頭数
    if race_info.get('n_horses', 0) < c['min_field']:
        return None
    # コース
    if race_info.get('course', '') != c['course']:
        return None
    # 距離
    if (race_info.get('dist') or 0) < c['min_dist']:
        return None
    # 馬場状態（取得できている場合のみチェック）
    track = race_info.get('track_cond', '').strip()
    track_ok = (track == c['track_cond']) if track else None  # None=未確認
    if track_ok is False:
        return None
    # 両モデル一致は呼び出し元でチェック済みとする
    # スコアギャップ
    if len(scored_san) < 2:
        return None
    gap = scored_san[0]['score'] - scored_san[1]['score']
    if gap < c['min_gap']:
        return None
    # 軸馬オッズ
    odds = scored_san[0]['odds']
    if not (c['odds_min'] <= odds <= c['odds_max']):
        return None
    return {
        'gap':          gap,
        'odds':         odds,
        'axis':         scored_san[0]['name'],
        'track_cond':   track if track else '未確認',
        'track_ok':     track_ok,
    }

# ─── 馬連シグナル判定 ──────────────────────────────────────
# バックテスト結果: 突出(max_prob≥20%) × 2位馬が市場3番人気以下 → ROI +45.3%

_LR_MODEL = None   # (w, b, mean, std) をキャッシュ

def _get_lr_model():
    global _LR_MODEL
    if _LR_MODEL is None:
        try:
            _LR_MODEL = load_model()
        except Exception:
            _LR_MODEL = None
    return _LR_MODEL


def compute_lr_probs(race_info: dict, horse_db: dict, jstats, dc_db) -> list:
    """
    ロジスティック回帰モデルで各馬の勝率を計算。
    戻り値: [{'name': ..., 'prob': ..., 'pop': ...}, ...] 確率降順
    """
    model = _get_lr_model()
    if model is None:
        return []
    w, b, mean, std = model

    course    = race_info.get('course', 'ダート')
    dist      = race_info.get('dist', 1800) or 1800
    track     = race_info.get('track_cond', '')
    venue     = race_info.get('race_id', '      ')[4:6]
    race_ym   = race_info.get('race_id', '202601')[:6]
    histories = race_info.get('histories', {})

    # odds リストから 馬名→(pop, odds) マップを作成
    odds_map = {o['name']: o for o in race_info.get('odds', [])}

    entries = []
    for h in race_info.get('horses', []):
        name   = h.get('name', '')
        jockey = h.get('jockey', '')
        o_info = odds_map.get(name, {})
        pop    = h.get('pop') or o_info.get('pop', 99) or 99
        odds   = h.get('odds') or o_info.get('odds', 0.0) or 0.0
        if not name:
            continue
        # 既存horse_db + fetchした戦績を合算
        hist = list(horse_db.get(name, []))
        raw_hist = histories.get(h.get('horse_id', ''), [])
        if isinstance(raw_hist, list):
            from walkforward_winprob import _make_record as _wf_make
            from score_agent_core import parse_avg_pos, parse_bw_change, parse_margin, parse_time
            for row in raw_hist:
                if not isinstance(row, dict): continue
                rank = _int(str(row.get('着順', '')).strip())
                if rank is None: continue
                rid = row.get('race_id', '')
                hist.append({
                    'race_id': rid, 'race_ym': rid[:6],
                    'venue_code': rid[4:6] if len(rid) >= 6 else '',
                    'rank': rank,
                    'field_size': _int(str(row.get('頭数', '0'))) or 10,
                    'agari_rank': -1, 'agari_field': 10,
                    'avg_pos': parse_avg_pos(row.get('通過順', '')),
                    'bw_chg': parse_bw_change(row.get('馬体重', '')),
                    'dist': _int(str(row.get('距離', '0'))),
                    'track_cond': row.get('馬場状態', '').strip(),
                    'margin': parse_margin(row.get('着差', '').strip(), rank),
                    'race_time': parse_time(row.get('タイム', '')),
                })
        fac = compute_horse_factors(
            name, jockey, course, dist, hist, jstats, dc_db,
            track_cond=track, venue_code=venue, race_ym=race_ym,
            pop=pop, odds=odds,
        )
        entries.append({'name': name, 'factors': fac, 'pop': pop})

    if len(entries) < 3:
        return []

    normed = race_relative_features([e['factors'] for e in entries])
    results = []
    for entry, fv in zip(entries, normed):
        fv_n = (fv - mean) / (std + 1e-8)
        prob = float(predict_prob(fv_n.reshape(1, -1), w, b)[0])
        results.append({'name': entry['name'], 'prob': prob, 'pop': entry['pop']})

    return sorted(results, key=lambda x: -x['prob'])


def check_umaren_signal(race_info: dict, horse_db: dict, jstats, dc_db) -> dict | None:
    """
    馬連シグナル判定:
      - 突出 (max_prob ≥ 20%)
      - 2位馬が市場3番人気以下

    条件を満たす場合: {'top1': ..., 'top2': ..., 'max_prob': ..., 'pop2': ...}
    満たさない場合: None
    """
    ranked = compute_lr_probs(race_info, horse_db, jstats, dc_db)
    if len(ranked) < 2:
        return None

    max_prob = ranked[0]['prob'] * 100
    if max_prob < 20.0:
        return None   # 突出でない

    pop2 = ranked[1]['pop']
    if pop2 <= 2:
        return None   # 2位馬が1〜2番人気 = 市場と同じ (ROI -19%)

    return {
        'top1':     ranked[0]['name'],
        'top2':     ranked[1]['name'],
        'max_prob': max_prob,
        'prob2':    ranked[1]['prob'] * 100,
        'pop2':     pop2,
        'ranked':   ranked,
    }


VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
}

# Discord embed の色
COLOR_HEADER = 0x2ecc71   # 緑: ヘッダー
COLOR_RACE   = 0x1a73e8   # 青: レース結果
COLOR_WARN   = 0xe74c3c   # 赤: 警告

# ─── Discord ──────────────────────────────────────────
def send_discord(embeds: list) -> None:
    if not DISCORD_WEBHOOK_URL:
        print('[Discord] DISCORD_WEBHOOK_URL 未設定 - スキップ')
        return
    # Discord上限: 1メッセージ10 embeds
    for i in range(0, len(embeds), 10):
        chunk = embeds[i:i + 10]
        r = requests.post(
            DISCORD_WEBHOOK_URL,
            json={'embeds': chunk},
            timeout=15,
        )
        if r.status_code in (200, 204):
            print(f'✅ Discord送信完了 ({len(chunk)}件)')
        else:
            print(f'[Discord] 送信失敗: {r.status_code} {r.text[:120]}')


# ─── Discord embed 生成 ────────────────────────────────
def _venue_str(race_info: dict) -> str:
    race_id = race_info.get('race_id', '')
    return VENUE_MAP.get(race_id[4:6], '?') if len(race_id) >= 6 else '?'


def _embed_model_block(scored: list, label: str, diff_horses: set = None) -> list:
    """モデル1つ分の embed 行を生成"""
    lines = [f"**{label}**", "```"]
    marks = ['★', '▲', '▽']
    for i, h in enumerate(scored[:6]):
        mark  = marks[i] if i < 3 else '  '
        nr    = f'({h["n_races"]}走)' if h['n_races'] > 0 else f'推定{h["pop"]}人気'
        flag  = ' <独自' if (diff_horses and h['name'] in diff_horses) else ''
        prob  = f'{h["win_prob"]:>4.1f}%' if 'win_prob' in h else '    -'
        lines.append(
            f"{mark}{h['name'][:10]:10} {h['jockey'][:6]:6} "
            f"{h['pop']:>3}人気 {h['odds']:>5.1f}倍  "
            f"{h['score']:>5.1f}pt {prob}  {nr}{flag}"
        )
    if len(scored) >= 3:
        t    = scored
        gap  = t[0]['score'] - t[1]['score']
        conf = '軸強' if gap >= 5 else ('軸普通' if gap >= 2 else '混戦')
        lines.append(f"-> {t[0]['name']} / {t[1]['name']} / {t[2]['name']}  [{conf} {gap:.1f}pt]")
    lines.append("```")
    return lines


def format_race_embed(race_info: dict, scored_win: list, scored_san: list) -> dict:
    """1レース分の Discord embed を生成（両モデル表示）"""
    race_name = race_info.get('race_name', '?')
    dist      = race_info.get('dist', 0)
    course    = race_info.get('course', '?')
    n_horses  = race_info.get('n_horses', len(scored_win))
    rnum      = race_info.get('rnum', '?')
    venue     = _venue_str(race_info)

    win_top3 = {h['name'] for h in scored_win[:3]}
    san_top3 = {h['name'] for h in scored_san[:3]}

    lines = [f"**{venue} {rnum}R  {race_name}  {course}{dist}m  {n_horses}頭**"]
    lines += _embed_model_block(scored_win, '[WIN] 1着最大化モデル')
    lines += _embed_model_block(scored_san, '[三連複] 三連複最大化モデル',
                                diff_horses=san_top3 - win_top3)

    # モデル合意
    if win_top3 == san_top3:
        lines.append(f"**両モデル一致** {'/'.join(win_top3)} <- 信頼度高")
    else:
        common = win_top3 & san_top3
        if common:
            lines.append(f"共通: {'/'.join(common)}  |  WIN独自: {'/'.join(win_top3-san_top3)}  |  三連複独自: {'/'.join(san_top3-win_top3)}")

    # ベット推奨判定
    bet = None
    if win_top3 == san_top3:
        bet = check_bet(race_info, scored_san)
    if bet:
        lines.append(
            f'**★★★ スコアエージェント ベット推奨 ★★★**\n'
            f'軸: {bet["axis"]}  {bet["odds"]:.1f}倍  ギャップ:{bet["gap"]:.1f}pt\n'
            f'三連複: {scored_san[0]["name"]} / {scored_san[1]["name"]} / {scored_san[2]["name"]}'
        )

    # 馬連シグナル
    sig = race_info.get('_umaren_signal')
    if sig:
        lines.append(
            f'**💰 馬連シグナル**  突出{sig["max_prob"]:.1f}% × 2位{sig["pop2"]}番人気\n'
            f'軸: {sig["top1"]} ({sig["max_prob"]:.1f}%)  '
            f'相手: {sig["top2"]} ({sig["prob2"]:.1f}%) {sig["pop2"]}番人気\n'
            f'→ **馬連 {sig["top1"]} - {sig["top2"]}**  (BT実績: ROI +45.3%)'
        )

    description = '\n'.join(lines)
    if len(description) > 4000:
        description = description[:3950] + '\n...(省略)'

    has_signal = bool(bet or sig)
    return {
        'title':       f'{"💰" if sig else ""}{"★" if bet else ""}{venue} {rnum}R  {race_name}',
        'description': description,
        'color':       0xf39c12 if has_signal else COLOR_RACE,  # 橙色でシグナル強調
    }


# ─── コンソール表示 ────────────────────────────────────
def print_race_result(race_info: dict, scored_win: list, scored_san: list) -> None:
    race_name = race_info.get('race_name', '?')
    dist      = race_info.get('dist', 0)
    course    = race_info.get('course', '?')
    n         = race_info.get('n_horses', len(scored_win))
    rnum      = race_info.get('rnum', '?')
    venue     = _venue_str(race_info)

    print(f'\n{"="*72}')
    print(f'  {venue} {rnum}R  {race_name}  {course}{dist}m  {n}頭')
    print(f'{"="*72}')

    # ── WIN モデル ──
    print(f'  [WIN モデル: 1着最大化]')
    print(f'  {"馬名":12} {"騎手":8} {"人気":>4} {"オッズ":>6}  {"スコア":>7}  {"勝率":>6}  {"データ"}')
    print(f'  {"-"*70}')
    marks = ['★', '▲', '▽']
    for i, h in enumerate(scored_win[:8]):
        mark = marks[i] if i < 3 else '  '
        nr   = f'({h["n_races"]}走)' if h['n_races'] > 0 else f'(推定:{h["pop"]}人気)'
        prob = f'{h["win_prob"]:>5.1f}%' if 'win_prob' in h else '     -'
        print(f'  {mark}{h["name"]:12} {h["jockey"]:8} {h["pop"]:>4} {h["odds"]:>6.1f}  '
              f'{h["score"]:>7.1f}  {prob}  {nr}')
    if len(scored_win) >= 3:
        t = scored_win
        gap = t[0]['score'] - t[1]['score']
        conf = '軸強' if gap >= 5 else ('軸普通' if gap >= 2 else '混戦')
        print(f'  -> {t[0]["name"]} / {t[1]["name"]} / {t[2]["name"]}  [{conf} {gap:.1f}pt]')

    print()

    # ── SANPUKU モデル ──
    print(f'  [三連複モデル: 三連複最大化]')
    print(f'  {"馬名":12} {"騎手":8} {"人気":>4} {"オッズ":>6}  {"スコア":>7}  {"勝率":>6}  {"データ"}')
    print(f'  {"-"*70}')

    # WINと三連複で順位差がある馬をハイライト
    win_top3  = {h['name'] for h in scored_win[:3]}
    san_top3  = {h['name'] for h in scored_san[:3]}
    diff_horses = san_top3 - win_top3  # 三連複でのみ上位に入る馬

    for i, h in enumerate(scored_san[:8]):
        mark   = marks[i] if i < 3 else '  '
        nr     = f'({h["n_races"]}走)' if h['n_races'] > 0 else f'(推定:{h["pop"]}人気)'
        flag   = ' <-- 三連複独自' if h['name'] in diff_horses else ''
        prob   = f'{h["win_prob"]:>5.1f}%' if 'win_prob' in h else '     -'
        print(f'  {mark}{h["name"]:12} {h["jockey"]:8} {h["pop"]:>4} {h["odds"]:>6.1f}  '
              f'{h["score"]:>7.1f}  {prob}  {nr}{flag}')
    if len(scored_san) >= 3:
        t = scored_san
        gap = t[0]['score'] - t[1]['score']
        conf = '軸強' if gap >= 5 else ('軸普通' if gap >= 2 else '混戦')
        print(f'  -> {t[0]["name"]} / {t[1]["name"]} / {t[2]["name"]}  [{conf} {gap:.1f}pt]')

    # ── モデル合意確認 ──
    if win_top3 == san_top3:
        print(f'\n  [両モデル一致] {"/".join(win_top3)} ← 信頼度高')
    else:
        common = win_top3 & san_top3
        if common:
            print(f'\n  [共通馬] {"/".join(common)}  '
                  f'[WIN独自] {"/".join(win_top3-san_top3)}  '
                  f'[三連複独自] {"/".join(san_top3-win_top3)}')

    # ── ベット推奨判定 ──
    if win_top3 == san_top3:
        bet = check_bet(race_info, scored_san)
        if bet:
            track_note = '' if bet['track_ok'] else f'  ※馬場状態{bet["track_cond"]}（要確認）'
            print(f'\n  {"="*62}')
            print(f'  ★★★ スコアエージェント ベット推奨 ★★★{track_note}')
            print(f'  軸: {bet["axis"]}  オッズ:{bet["odds"]:.1f}倍  ギャップ:{bet["gap"]:.1f}pt')
            print(f'  三連複: {scored_san[0]["name"]} / {scored_san[1]["name"]} / {scored_san[2]["name"]}')
            print(f'  {"="*62}')

    # ── 馬連シグナル ──
    sig = race_info.get('_umaren_signal')
    if sig:
        print(f'\n  {"="*62}')
        print(f'  💰 馬連シグナル  [突出{sig["max_prob"]:.1f}% × 2位が{sig["pop2"]}番人気]')
        print(f'  軸 : {sig["top1"]}  ({sig["max_prob"]:.1f}%)')
        print(f'  相手: {sig["top2"]}  ({sig["prob2"]:.1f}%)  市場{sig["pop2"]}番人気')
        print(f'  → 馬連 {sig["top1"]} - {sig["top2"]} を推奨')
        print(f'  ※ バックテスト根拠: 突出×2位3番人気以下 ROI +45.3% (432レース)')
        print(f'  {"="*62}')


# ─── レースデータ取得 ──────────────────────────────────
def fetch_races(date_str: str) -> list:
    """fetch_race.run_fetch() を呼び出して当日の全R8-R11レースを取得"""
    from fetch_race import run_fetch
    result = run_fetch(date_str)
    return result.get('all_races', [])


def load_races_from_json(json_path: str) -> list:
    """キャッシュJSONからレースデータを読み込む"""
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('all_races', [])


# ─── メイン ───────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='スコアエージェント')
    parser.add_argument('--date',       help='対象日 YYYYMMDD（省略=今日）')
    parser.add_argument('--json',       help='fetch済みJSONを使う場合のパス')
    parser.add_argument('--data',       default='data', help='データディレクトリ')
    parser.add_argument('--no-discord', action='store_true', help='Discord通知しない')
    args = parser.parse_args()

    date_str  = args.date or datetime.date.today().strftime('%Y%m%d')
    today_disp = f'{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}'

    print('=' * 72)
    print(f'  🏇  スコアエージェント  {today_disp}')
    print('=' * 72)

    # ── モデル・履歴データ読み込み ──
    print('\n[1/3] 過去データ読み込み中...')
    jstats, dc_db  = load_models(args.data)
    horse_db = load_history_db(args.data)
    print(f'  騎手DB: {len(jstats)}名  '
          f'コース実績DB: {len(dc_db)}件  '
          f'馬履歴DB: {len(horse_db)}頭')

    # ── 当日レース取得 ──
    print(f'\n[2/3] 当日レース取得中  ({date_str})...')
    if args.json:
        all_races = load_races_from_json(args.json)
        print(f'  JSONキャッシュ読み込み: {args.json}')
    else:
        all_races = fetch_races(date_str)
    print(f'  R8-R11: {len(all_races)}レース取得')

    # ── フィルタ ──
    print('\n[3/3] 対象レース判定...')
    target_races = []
    for r in all_races:
        name = r.get('race_name', '')
        if should_exclude(name):
            print(f'  [除外] {r.get("rnum","?")}R {name}')
        else:
            target_races.append(r)
            print(f'  [対象] {r.get("rnum","?")}R {name} '
                  f'{r.get("course","")} {r.get("dist",0)}m {r.get("n_horses",0)}頭')

    print(f'\nスコア計算対象: {len(target_races)} レース')
    print('─' * 72)

    if not target_races:
        print('\n対象レースなし。終了します。')
        return

    # ── スコア計算・表示 ──
    embeds = []

    embeds.append({
        'title':       f'🏇 スコアエージェント  {today_disp}',
        'description': (
            f'対象: {len(target_races)}レース（新馬・未勝利・1勝クラス除外済み）\n'
            f'馬履歴DB: {len(horse_db)}頭 / '
            f'騎手DB: {len(jstats)}名'
        ),
        'color': COLOR_HEADER,
    })

    all_results = []   # バックテスト用に保持

    for race_info in target_races:
        scored_win = score_all_horses(race_info, horse_db, jstats, dc_db,
                                      weights=WEIGHT_PRESETS['win'])
        scored_san = score_all_horses(race_info, horse_db, jstats, dc_db,
                                      weights=WEIGHT_PRESETS['sanpuku'])

        if not scored_win:
            print(f'\n  {race_info.get("rnum","?")}R: スコア計算できる馬なし')
            continue

        # 馬連シグナル判定 (ロジスティック回帰モデル)
        sig = check_umaren_signal(race_info, horse_db, jstats, dc_db)
        race_info['_umaren_signal'] = sig

        print_race_result(race_info, scored_win, scored_san)
        embeds.append(format_race_embed(race_info, scored_win, scored_san))

        all_results.append({
            'race_id':   race_info.get('race_id', ''),
            'race_name': race_info.get('race_name', ''),
            'scored_win': scored_win,
            'scored_san': scored_san,
        })

    # ── Discord送信 ──
    if not args.no_discord:
        print('\n' + '─' * 72)
        if DISCORD_WEBHOOK_URL:
            send_discord(embeds)
        else:
            print('[Discord] DISCORD_WEBHOOK_URL が未設定です')
            print('  設定方法: set DISCORD_WEBHOOK_URL=<webhook_url>  (Windows)')
            print('  または:   export DISCORD_WEBHOOK_URL=<webhook_url>  (bash)')

    print('\n' + '=' * 72)
    print('  完了')
    print('=' * 72)


if __name__ == '__main__':
    main()

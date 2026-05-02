"""
strategy_loop.py - 継続的戦略研究ループ

【フロー】
  1. STRATEGY_CANDIDATES の全候補戦略をバックテスト
  2. ROI >= THRESHOLD_PCT (130%) → 採用候補：backtest_csv.py への実装コードを生成
  3. ROI < THRESHOLD_PCT         → 不採用：結果を記録
  4. 結果を strategy_results.json に保存
  5. NOTION_TOKEN 環境変数があれば Notion に自動投稿

【使い方】
  python strategy_loop.py                        # バックテストのみ
  python strategy_loop.py --notion               # Notion 投稿あり
  NOTION_TOKEN=xxx python strategy_loop.py --notion

【新しい候補追加方法】
  STRATEGY_CANDIDATES リストに辞書を追加するだけ。
  条件タイプ "prev_history" は prev_history の f3rank/finish_rank/margin/last_corner を使用。
  条件タイプ "race_grade" は grade 列が必要（バックフィル後に有効）。
"""
import sys, io, os, glob, csv, re, json, math, argparse, requests, random, itertools
from datetime import datetime
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ═══════════════════════════════════════════════════
# 設定
# ═══════════════════════════════════════════════════
THRESHOLD_PCT   = 130.0   # 採用判定の回収率閾値
MIN_BETS        = 50      # 採用に必要な最低件数（これ未満は10年データで再検証）
STACK_BETS      = 10      # スタック対象の最低件数（これ未満は無視）
BATCH_SIZE      = 10      # 毎回自動生成する新候補数
DATA_DIR        = 'data'
RESULTS_FILE    = 'strategy_results.json'
JRA_VENUES      = {'01','02','03','04','05','06','07','08','09','10'}

# Notion 設定
NOTION_TOKEN     = os.environ.get('NOTION_TOKEN', '')
NOTION_PARENT_ID = '3311333d21b18139bfa0f2a5863d7410'  # 戦略研究ログページ
NOTION_API_URL   = 'https://api.notion.com/v1/pages'
NOTION_VERSION   = '2022-06-28'

# Discord 設定
DISCORD_WEBHOOK_URL = os.environ.get(
    'DISCORD_WEBHOOK_URL',
    'https://discord.com/api/webhooks/1484909461188640915/W9fEb0xSlVFbh7k-7trQ45nFrZTKo2Cb1P8DKZMbsIZxeHdLPt8HK65yWdPGyc3cyZ_q'
)

# ═══════════════════════════════════════════════════
# 候補戦略リスト
# ═══════════════════════════════════════════════════
# status:
#   "implemented" → 既に backtest_csv.py に組み込み済み
#   "candidate"   → 今回テスト対象
#   "rejected"    → 過去テストで不採用
#
# type:
#   "prev_history" → prev_history (f3rank, finish_rank, margin, last_corner) を条件に使う
#   "race_grade"   → grade 列が必要（バックフィル後に有効化）
#   "multi_prev"   → 2走分の履歴が必要（将来拡張）

STRATEGY_CANDIDATES = [

    # ─────────── 実装済み（参照用）───────────
    {
        'id':     'ANA_v1',
        'name':   'ANA 穴馬複勝（確率モデル）',
        'status': 'implemented',
        'type':   'ana',
        'roi_actual': 129.5,
        'bets_actual': 340,
        'note':   'top3_prob モデル。オッズ10-30倍・確率25%以上',
    },
    {
        'id':     'FUKUSHO_F1',
        'name':   'FUKUSHO 隠れ末脚型',
        'status': 'implemented',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      1,
            'prev_finish_min': 7,
            'odds_min': 14.0, 'odds_max': 18.0,
            'pop_min': 6,     'pop_max': 12,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 15000,
        'roi_actual': 169.0,
        'bets_actual': 41,
        'note':   '前走上がり1位&7着↓。条件を広げると急落するため厳守',
    },

    # ─────────── テスト候補 ───────────
    {
        'id':     'CORNER_COMEBACK_v1',
        'name':   '後方一気型（前走後方&上がり上位）',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'last_corner_min': 7,    # 前走最終コーナー7番手以降
            'f3rank_max':      2,    # 前走上がり3F 2位以内
            'odds_min': 12.0, 'odds_max': 25.0,
            'pop_min': 5,     'pop_max': 12,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 15000,
        'note':   '後方から最速上がりを記録した馬。展開が向けば連対率高い',
    },
    {
        'id':     'CORNER_COMEBACK_v2',
        'name':   '後方一気型 v2（条件緩和）',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'last_corner_min': 6,
            'f3rank_max':      3,
            'odds_min': 10.0, 'odds_max': 22.0,
            'pop_min': 4,     'pop_max': 12,
            'field_min': 8,
        },
        'bet_pct': 0.015,
        'bet_max': 10000,
        'note':   'v1 の条件を緩和。サンプル数を確保するための実験',
    },
    {
        'id':     'FUKUSHO_CORNER',
        'name':   'FUKUSHO×コーナー複合型',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      1,
            'prev_finish_min': 6,
            'last_corner_min': 5,    # 後方から最速上がり
            'odds_min': 12.0, 'odds_max': 20.0,
            'pop_min': 5,     'pop_max': 13,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 15000,
        'note':   'FUKUSHO の条件に後方スタートを追加した複合型',
    },
    {
        'id':     'FUKUSHO_WIDE',
        'name':   'FUKUSHO 拡張版（オッズ帯広げ）',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      1,
            'prev_finish_min': 7,
            'odds_min': 12.0, 'odds_max': 22.0,
            'pop_min': 5,     'pop_max': 13,
            'field_min': 8,
        },
        'bet_pct': 0.015,
        'bet_max': 12000,
        'note':   'F1 より件数を増やすため条件を若干緩和。ROI許容ラインの確認',
    },
    {
        'id':     'HIGH_F3_BAD_FINISH',
        'name':   '上がり上位&大敗型（f3rank 1-2 & 9着以下）',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      2,
            'prev_finish_min': 9,
            'odds_min': 15.0, 'odds_max': 30.0,
            'pop_min': 7,     'pop_max': 15,
            'field_min': 8,
        },
        'bet_pct': 0.015,
        'bet_max': 10000,
        'note':   '大敗したが上がり順位は高い。市場の過剰割引を狙う',
    },
    {
        'id':     'GRADE_DOWN_v1',
        'name':   'GRADE_DOWN 重賞降格馬',
        'status': 'candidate',
        'type':   'race_grade',
        'conditions': {
            'prev_grades':     ['G1', 'G2', 'G3'],
            'current_grade':   '',       # 今走は非重賞
            'odds_min': 8.0,  'odds_max': 20.0,
            'pop_min': 4,     'pop_max': 12,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 15000,
        'note':   '前走G1-G3 → 今走条件戦。grade列バックフィル後に有効化',
    },

    # ─────────── 10年データ揃い次第 再検証 ───────────
    {
        'id':     'F_f1_r7_o15-19_p7-13',
        'name':   'FUKUSHO変形 f3≤1 着≥7 15-19倍 7-13人気',
        'status': 'pending_verification',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      1,
            'prev_finish_min': 7,
            'odds_min': 15.0, 'odds_max': 19.0,
            'pop_min': 7,     'pop_max': 13,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 15000,
        'roi_actual': 191.4,
        'bets_actual': 26,
        'note':   '2024-2025の23ヶ月で26件・ROI191.4%。件数少のため10年データで再検証予定。',
    },

    # ─────────── 新候補（2026-03-28 追加）───────────
    {
        'id':     'ODDS_DRIFT_HIDDEN',
        'name':   '超高オッズ×隠れ末脚型（22〜40倍帯）',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'f3rank_max':      1,
            'prev_finish_min': 7,
            'odds_min': 22.0, 'odds_max': 40.0,
            'pop_min': 8,     'pop_max': 16,
            'field_min': 10,
        },
        'bet_pct': 0.02,
        'bet_max': 10000,
        'note':   'FUKUSHO_F1 の信号を22-40倍帯に適用。複勝払戻が高倍率になりやすい帯。',
    },
    {
        'id':     'FRONT_RUNNER_FADE',
        'name':   '先行惨敗後リベンジ型',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'last_corner_max': 3,       # 前走最終コーナー3番手以内（先行）
            'prev_finish_min': 8,       # 前走8着以下で大敗
            'margin_max':      3.0,     # 着差≤3（極端なバテではない）
            'f3rank_max':      5,       # 前走上がりも中位以上
            'odds_min': 10.0, 'odds_max': 22.0,
            'pop_min': 5,     'pop_max': 14,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 12000,
        'note':   '先行して大敗した馬は過剰に嫌われる。展開・馬場由来の凡走を狙い撃ち。',
    },
    {
        'id':     'LARGE_FIELD_DROPOUT',
        'name':   '大フィールド大敗→中小フィールド復帰型',
        'status': 'candidate',
        'type':   'prev_history',
        'conditions': {
            'prev_field_min':  16,      # 前走16頭以上の大レース
            'f3rank_max':      3,       # 前走上がり3位以内
            'prev_finish_min': 9,       # 前走9着以下で大敗
            'margin_max':      5.0,
            'odds_min': 12.0, 'odds_max': 28.0,
            'pop_min': 6,     'pop_max': 15,
            'field_min': 8,
        },
        'bet_pct': 0.02,
        'bet_max': 12000,
        'note':   '大フィールドでの大敗は展開依存が大。次走で頭数が減れば条件好転。',
    },
]


# ═══════════════════════════════════════════════════
# 着差パーサー
# ═══════════════════════════════════════════════════
MARGIN_MAP = {
    '': 0.0, 'ハナ': 0.1, 'アタマ': 0.15, 'クビ': 0.25,
    '1/2': 0.5, '3/4': 0.75, '1': 1.0, '1.1/4': 1.25,
    '1.1/2': 1.5, '1.3/4': 1.75, '2': 2.0, '2.1/2': 2.5,
    '3': 3.0, '3.1/2': 3.5, '4': 4.0, '5': 5.0, '大': 99.0,
}

def parse_margin(s):
    s = str(s).strip()
    if s in MARGIN_MAP:
        return MARGIN_MAP[s]
    try:
        return float(s)
    except:
        return 99.0


# ═══════════════════════════════════════════════════
# 券種設定
# ═══════════════════════════════════════════════════
# 各券種に対応するCSV列名
RAW_KEY = {
    'tansho':     'tansho_raw',
    'fukusho':    'fukusho_raw',
    'umaren':     'umaren_raw',
    'umatan':     'umatan_raw',
    'wide':       'wide_raw',
    'sanrenpuku': 'sanrenpuku_raw',
    'sanrentan':  'sanrentan_raw',
}

# 必要な相手馬の数（0=単体券種、1=2頭券種、2=3頭券種）
PARTNERS_NEEDED = {
    'tansho': 0, 'fukusho': 0,
    'umaren': 1, 'umatan': 1, 'wide': 1,
    'sanrenpuku': 2, 'sanrentan': 2,
}


def get_partners(anchor, horses, n, method='pop_top', prev_history=None,
                 partner_odds_min=None, partner_odds_max=None):
    """
    相手馬を選定して返す。

    method:
      'pop_top'    : 人気順上位（デフォルト）
      'odds_range' : 指定オッズ帯（B案）の馬から人気順。帯に該当なしなら人気順にフォールバック
      'f3_best'    : prev_history の前走上がり順位が最上位の馬（C案）
      'box_top3'   : 人気上位3頭を返す（D案・ボックス用）
    """
    others = [h for h in horses if h['umaban'] != anchor['umaban']]

    if method == 'odds_range':
        o_min = partner_odds_min or 3.0
        o_max = partner_odds_max or 8.0
        in_range = sorted(
            [h for h in others if o_min <= h['odds'] <= o_max],
            key=lambda h: h['popularity']
        )
        if in_range:
            return in_range[:n]
        # 帯に馬がいない場合は人気順フォールバック
        others.sort(key=lambda h: h['popularity'])
        return others[:n]

    elif method == 'f3_best':
        ph = prev_history or {}
        scored = sorted(
            others,
            key=lambda h: (ph.get(h['name'], {}).get('f3rank', 999), h['popularity'])
        )
        return scored[:n]

    elif method == 'box_top3':
        others.sort(key=lambda h: h['popularity'])
        return others[:3]  # 3頭返す（呼び出し側でボックス処理）

    else:  # pop_top
        others.sort(key=lambda h: h['popularity'])
        return others[:n]


def calc_bet_return(anchor, partners, bet_type, info, bet):
    """
    払戻を計算して返す。
    anchor  : 条件合致馬（アンカー）
    partners: 相手馬リスト（人気順）
    bet_type: 'tansho'/'fukusho'/'umaren'/'umatan'/'wide'/'sanrenpuku'/'sanrentan'
    """
    raw  = parse_payout(info.get(RAW_KEY.get(bet_type, 'fukusho_raw'), ''))
    a_n  = anchor['umaban']
    a_r  = anchor['finish_rank']

    if bet_type == 'tansho':
        if a_r == 1 and a_n in raw:
            return int(bet * raw[a_n] / 100)

    elif bet_type == 'fukusho':
        if a_r <= 3 and a_n in raw:
            return int(bet * raw[a_n] / 100)

    elif bet_type == 'umaren':
        if not partners: return 0
        p = partners[0]
        if a_r <= 2 and p['finish_rank'] <= 2:
            key = '-'.join(sorted([a_n, p['umaban']], key=lambda x: int(x)))
            if key in raw:
                return int(bet * raw[key] / 100)

    elif bet_type == 'wide':
        if not partners: return 0
        p = partners[0]
        if a_r <= 3 and p['finish_rank'] <= 3:
            key = '-'.join(sorted([a_n, p['umaban']], key=lambda x: int(x)))
            if key in raw:
                return int(bet * raw[key] / 100)

    elif bet_type == 'umatan':
        if not partners: return 0
        p = partners[0]
        # アンカー1着 → 相手2着
        if a_r == 1 and p['finish_rank'] == 2:
            key = f'{a_n}-{p["umaban"]}'
            if key in raw:
                return int(bet * raw[key] / 100)

    elif bet_type == 'sanrenpuku':
        if len(partners) < 2: return 0
        p1, p2 = partners[0], partners[1]
        ranks = {a_r, p1['finish_rank'], p2['finish_rank']}
        if ranks == {1, 2, 3}:
            key = '-'.join(sorted([a_n, p1['umaban'], p2['umaban']], key=lambda x: int(x)))
            if key in raw:
                return int(bet * raw[key] / 100)

    elif bet_type == 'sanrentan':
        if len(partners) < 2: return 0
        p1, p2 = partners[0], partners[1]
        # アンカー1着 → 相手1が2着 → 相手2が3着
        if a_r == 1 and p1['finish_rank'] == 2 and p2['finish_rank'] == 3:
            key = f'{a_n}-{p1["umaban"]}-{p2["umaban"]}'
            if key in raw:
                return int(bet * raw[key] / 100)

    return 0


# ═══════════════════════════════════════════════════
# データ読み込み
# ═══════════════════════════════════════════════════
def load_data(data_dir=DATA_DIR):
    races = {}
    files = sorted(glob.glob(f'{data_dir}/raceresults_*.csv'))
    for fpath in files:
        m = re.search(r'raceresults_(\d{4})(\d{2})\.csv', fpath)
        ym = f'{m.group(1)}-{m.group(2)}' if m else None
        with open(fpath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                rid = row['race_id']
                if row['場コード'] not in JRA_VENUES:
                    continue
                if rid not in races:
                    races[rid] = {
                        'ym':             ym,
                        'grade':          row.get('grade', '').strip(),
                        'course':         row.get('コース', '').strip(),
                        'tansho_raw':     row.get('単勝払戻', ''),
                        'fukusho_raw':    row.get('複勝払戻', ''),
                        'umaren_raw':     row.get('馬連払戻', ''),
                        'umatan_raw':     row.get('馬単払戻', ''),
                        'wide_raw':       row.get('ワイド払戻', ''),
                        'sanrenpuku_raw': row.get('三連複払戻', ''),
                        'sanrentan_raw':  row.get('三連単払戻', ''),
                        'horses':         [],
                    }
                try:
                    races[rid]['horses'].append({
                        'name':        row['馬名'],
                        'umaban':      row['馬番'].strip(),
                        'finish_rank': int(row['着順']),
                        'odds':        float(row['単勝オッズ']),
                        'popularity':  int(row['人気']),
                        'f3':          row.get('上がり3F', '').strip(),
                        'corner':      row.get('通過順', '').strip(),
                        'margin':      parse_margin(row.get('着差', '')),
                    })
                except:
                    pass
    return races


def parse_payout(s):
    result = {}
    for part in (s or '').split('|'):
        part = part.strip()
        if ':' not in part:
            continue
        key, pay = part.rsplit(':', 1)
        try:
            result[key.strip()] = int(pay.strip())
        except:
            pass
    return result


# ═══════════════════════════════════════════════════
# 前走履歴
# ═══════════════════════════════════════════════════
def update_prev_history(race_id, info, prev_history):
    horses = info['horses']
    grade  = info.get('grade', '')
    course = info.get('course', '')
    valid  = [(float(h['f3']), h['name']) for h in horses if h['f3']]
    valid.sort()
    f3ranks = {name: rank + 1 for rank, (_, name) in enumerate(valid)}
    n = len(horses)
    for h in horses:
        try:
            nums = re.findall(r'\d+', h.get('corner', ''))
            prev_history[h['name']] = {
                'f3rank':      f3ranks.get(h['name'], n),
                'field_size':  n,
                'finish_rank': h['finish_rank'],
                'last_corner': int(nums[-1]) if nums else None,
                'margin':      h['margin'],
                'grade':       grade,
                'course':      course,
            }
        except:
            pass


# ═══════════════════════════════════════════════════
# 汎用セレクター（prev_history 型）
# ═══════════════════════════════════════════════════
def generic_prev_selector(races_m, prev_history, cond):
    """
    cond (dict) キー:
      f3rank_max, prev_finish_min, prev_finish_max,
      last_corner_min, last_corner_max, margin_max,
      prev_field_min,
      odds_min, odds_max, pop_min, pop_max, field_min
    """
    cands = []
    for race_id, info in races_m:
        horses = info['horses']
        if len(horses) < cond.get('field_min', 8):
            continue
        seen = False
        for h in horses:
            ph = prev_history.get(h['name'])
            if ph is None:
                continue
            # 前走上がり順位
            if ph['f3rank'] > cond.get('f3rank_max', 99):
                continue
            # 前走着順（下限）
            if ph['finish_rank'] < cond.get('prev_finish_min', 1):
                continue
            # 前走着順（上限）
            if ph['finish_rank'] > cond.get('prev_finish_max', 99):
                continue
            # 前走最終コーナー位置（後方から）
            lc = ph.get('last_corner')
            if cond.get('last_corner_min') is not None:
                if lc is None or lc < cond['last_corner_min']:
                    continue
            # 前走最終コーナー位置（前方から）
            if cond.get('last_corner_max') is not None:
                if lc is None or lc > cond['last_corner_max']:
                    continue
            # 前走着差
            if ph.get('margin', 0) > cond.get('margin_max', 999):
                continue
            # 前走フィールドサイズ下限
            if ph.get('field_size', 0) < cond.get('prev_field_min', 0):
                continue
            # 今走オッズ
            if not (cond.get('odds_min', 0) <= h['odds'] <= cond.get('odds_max', 999)):
                continue
            # 今走人気
            if not (cond.get('pop_min', 1) <= h['popularity'] <= cond.get('pop_max', 99)):
                continue
            if seen:
                continue
            seen = True
            cands.append((h, race_id, info))
    return cands


def generic_grade_selector(races_m, prev_history, cond):
    """race_grade 型: 前走 grade 条件"""
    prev_grades = set(cond.get('prev_grades', []))
    cur_grade_excl = cond.get('current_grade', None)  # ''なら今走は非重賞のみ
    cands = []
    for race_id, info in races_m:
        horses = info['horses']
        if len(horses) < cond.get('field_min', 8):
            continue
        cur_grade = info.get('grade', '')
        # 今走グレード除外
        if cur_grade_excl is not None and cur_grade == cur_grade_excl:
            # 今走も同グレード → 対象外（今走が空=条件戦のみ狙う場合、cur_grade_excl!=''なら弾く）
            if cur_grade in {'G1', 'G2', 'G3'}:
                continue
        seen = False
        for h in horses:
            ph = prev_history.get(h['name'])
            if ph is None or ph.get('grade', '') not in prev_grades:
                continue
            if not (cond.get('odds_min', 0) <= h['odds'] <= cond.get('odds_max', 999)):
                continue
            if not (cond.get('pop_min', 1) <= h['popularity'] <= cond.get('pop_max', 99)):
                continue
            if seen:
                continue
            seen = True
            cands.append((h, race_id, info))
    return cands


# ═══════════════════════════════════════════════════
# バックテストエンジン
# ═══════════════════════════════════════════════════
def run_strategy_backtest(races, strategy):
    """
    単一戦略のバックテスト。
    Returns: dict with total, hit, invest, ret, roi, monthly
    """
    if strategy.get('status') == 'implemented' and 'roi_actual' in strategy:
        return None  # 実装済みはスキップ

    by_month = defaultdict(list)
    for rid, info in races.items():
        by_month[info['ym']].append((rid, info))

    prev_history = {}
    total = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
    monthly = []

    stype          = strategy.get('type', 'prev_history')
    cond           = strategy.get('conditions', {})
    bet_pct        = strategy.get('bet_pct', 0.02)
    bet_max        = strategy.get('bet_max', 15000)
    bet_type       = strategy.get('bet_type', 'fukusho')
    n_partners     = PARTNERS_NEEDED.get(bet_type, 0)
    partner_method = strategy.get('partner_method', 'pop_top')
    p_odds_min     = strategy.get('partner_odds_min', None)
    p_odds_max     = strategy.get('partner_odds_max', None)
    is_box         = (partner_method == 'box_top3') and n_partners > 0
    capital        = 70000  # 固定初期資金

    for ym in sorted(by_month.keys()):
        races_m = by_month[ym]
        capital += 20000  # 月次補充

        if stype == 'prev_history':
            cands = generic_prev_selector(races_m, prev_history, cond)
        elif stype == 'race_grade':
            cands = generic_grade_selector(races_m, prev_history, cond)
        else:
            cands = []

        m = {'total': 0, 'hit': 0, 'invest': 0, 'ret': 0}
        for h, race_id, info in cands:
            bet = min(int(capital * bet_pct / 100) * 100, bet_max)
            if bet < 100:
                continue

            if n_partners == 0:
                # 単体券種（単勝・複勝）
                ret = calc_bet_return(h, [], bet_type, info, bet)
                invest = bet
            elif is_box:
                # D: ボックス（人気上位3頭と総当たり）
                box_horses = get_partners(h, info['horses'], 3,
                                          method='box_top3', prev_history=prev_history)
                if n_partners == 1:
                    combos = [[p] for p in box_horses]
                else:
                    combos = [[box_horses[i], box_horses[j]]
                               for i in range(len(box_horses))
                               for j in range(i+1, len(box_horses))
                               if len(box_horses) >= 2]
                if not combos:
                    continue
                per_bet = max(100, int(bet / len(combos) / 100) * 100)
                ret    = sum(calc_bet_return(h, ps, bet_type, info, per_bet) for ps in combos)
                invest = per_bet * len(combos)
            else:
                # B / C / デフォルト（通常流し）
                partners = get_partners(h, info['horses'], n_partners,
                                        method=partner_method,
                                        prev_history=prev_history,
                                        partner_odds_min=p_odds_min,
                                        partner_odds_max=p_odds_max)
                if len(partners) < n_partners:
                    continue
                ret    = calc_bet_return(h, partners, bet_type, info, bet)
                invest = bet

            for s in (total, m):
                s['total']  += 1
                s['invest'] += invest
                s['ret']    += ret
                if ret > 0:
                    s['hit'] += 1
            capital = capital - invest + ret

        for rid, info in races_m:
            update_prev_history(rid, info, prev_history)

        monthly.append({'ym': ym, **m})

    roi = total['ret'] / total['invest'] * 100 if total['invest'] > 0 else 0.0
    hit_rate = total['hit'] / total['total'] * 100 if total['total'] > 0 else 0.0

    return {
        'id':        strategy['id'],
        'name':      strategy['name'],
        'status':    strategy['status'],
        'type':      stype,
        'total':     total['total'],
        'hit':       total['hit'],
        'hit_rate':  round(hit_rate, 1),
        'invest':    total['invest'],
        'ret':       total['ret'],
        'roi':       round(roi, 1),
        'monthly':   monthly,
        'pass_130':  roi >= THRESHOLD_PCT and total['total'] >= MIN_BETS,
        'stack':     roi >= THRESHOLD_PCT and STACK_BETS <= total['total'] < MIN_BETS,
        'pass_msg':  (
            f'✅ ROI {roi:.1f}% ≥ {THRESHOLD_PCT}%' if roi >= THRESHOLD_PCT and total['total'] >= MIN_BETS else
            f'⏸  ROI {roi:.1f}% 件数不足({total["total"]}件) → スタック' if roi >= THRESHOLD_PCT and STACK_BETS <= total['total'] < MIN_BETS else
            f'⚠️  件数不足 {total["total"]}件' if roi >= THRESHOLD_PCT else
            f'❌ ROI {roi:.1f}%'
        ),
        'note':      strategy.get('note', ''),
    }


# ═══════════════════════════════════════════════════
# 読みやすい戦略名生成
# ═══════════════════════════════════════════════════
def make_readable_name(strategy):
    """条件と券種からパターン名ベースの戦略名を生成する"""
    cond = strategy.get('conditions', {})
    bt   = strategy.get('bet_type', 'fukusho')

    BET_JA = {
        'tansho': '単勝', 'fukusho': '複勝',
        'umaren': '馬連', 'umatan': '馬単',
        'wide':   'ワイド', 'sanrenpuku': '三連複', 'sanrentan': '三連単',
    }

    f3      = cond.get('f3rank_max')
    lc_min  = cond.get('last_corner_min')
    lc_max  = cond.get('last_corner_max')
    fin_min = cond.get('prev_finish_min')
    fin_max = cond.get('prev_finish_max')
    margin  = cond.get('margin_max')
    pf_min  = cond.get('prev_field_min')
    o_min   = cond.get('odds_min', 0)
    o_max   = cond.get('odds_max', 99)
    p_min   = cond.get('pop_min', 1)
    p_max   = cond.get('pop_max', 99)

    # ── パターン判定 ──
    if lc_min and f3 and f3 <= 3 and not lc_max:
        pattern = f'後方一気型（前走{lc_min}番手以降・上がり{f3}位以内）'
    elif lc_max and fin_min:
        pattern = f'先行惨敗型（前走{lc_max}番手以内で{fin_min}着以下）'
    elif f3 and f3 <= 2 and fin_min and fin_min >= 7:
        level = '最速' if f3 == 1 else f'{f3}位以内'
        pattern = f'隠れ末脚型（前走上がり{level}・{fin_min}着以下の大敗）'
    elif f3 and f3 <= 3 and fin_min and fin_min >= 8:
        pattern = f'大敗末脚型（前走上がり{f3}位以内・{fin_min}着以下）'
    elif f3 and f3 <= 2 and fin_min and fin_min >= 4:
        pattern = f'惜敗再起型（前走上がり{f3}位以内・{fin_min}着以下）'
    elif margin is not None and fin_min:
        pattern = f'惜敗型（前走{fin_min}着以下・着差{margin}馬身以内）'
    elif pf_min and fin_min:
        pattern = f'大舞台凡走型（前走{pf_min}頭立て以上で{fin_min}着以下）'
    elif f3:
        level = '最速' if f3 == 1 else f'{f3}位以内'
        pattern = f'上がり{level}型'
    elif fin_min:
        pattern = f'前走{fin_min}着以下型'
    else:
        pattern = '条件指定型'

    # ── オッズ・人気フィルター ──
    params = f'{o_min:.0f}〜{o_max:.0f}倍・{p_min}〜{p_max}番人気'

    bet_ja = BET_JA.get(bt, bt)
    return f'{pattern}【{bet_ja}】{params}'


# ═══════════════════════════════════════════════════
# 実装コード生成
# ═══════════════════════════════════════════════════
def generate_impl_code(strategy, result):
    """130%超戦略の backtest_csv.py 追加コードを生成"""
    sid  = strategy['id']
    cond = strategy.get('conditions', {})
    pct  = strategy.get('bet_pct', 0.02)
    mx   = strategy.get('bet_max', 15000)

    lines = [
        f'# ── {strategy["name"]} (ROI {result["roi"]}%) ──',
        f'# backtest_csv.py の定数セクションに追加:',
        f'{sid}_ODDS_MIN  = {cond.get("odds_min", 0)}',
        f'{sid}_ODDS_MAX  = {cond.get("odds_max", 99)}',
        f'{sid}_POP_MIN   = {cond.get("pop_min", 1)}',
        f'{sid}_POP_MAX   = {cond.get("pop_max", 99)}',
        f'{sid}_FIELD_MIN = {cond.get("field_min", 8)}',
        f'',
        f'def {sid.lower()}_bet(capital):',
        f'    return min(int(capital * {pct} / 100) * 100, {mx})',
        f'',
        f'# run_backtest() の月次ループ内に追加（ana_cands の後・prev_history更新の前）:',
        f'{sid.lower()}_selected = []',
        f'seen_{sid.lower()} = set()',
        f'for race_id, info in races_m:',
        f'    if len(info["horses"]) < {sid}_FIELD_MIN: continue',
        f'    for h in info["horses"]:',
        f'        ph = prev_history.get(h["name"])',
        f'        if ph is None: continue',
    ]
    if cond.get('f3rank_max'):
        lines.append(f'        if ph["f3rank"] > {cond["f3rank_max"]}: continue')
    if cond.get('prev_finish_min'):
        lines.append(f'        if ph["finish_rank"] < {cond["prev_finish_min"]}: continue')
    if cond.get('last_corner_min') is not None:
        lines.append(f'        if (ph.get("last_corner") or 0) < {cond["last_corner_min"]}: continue')
    lines += [
        f'        if not ({sid}_ODDS_MIN <= h["odds"] <= {sid}_ODDS_MAX): continue',
        f'        if not ({sid}_POP_MIN <= h["popularity"] <= {sid}_POP_MAX): continue',
        f'        if race_id in seen_{sid.lower()}: continue',
        f'        seen_{sid.lower()}.add(race_id)',
        f'        {sid.lower()}_selected.append((h, race_id, info))',
    ]
    return '\n'.join(lines)


# ═══════════════════════════════════════════════════
# 自動戦略考案（グリッドサーチ）
# ═══════════════════════════════════════════════════
def load_tested_ids():
    """これまでにテストしたIDをすべて返す（重複防止）"""
    tested = set(s['id'] for s in STRATEGY_CANDIDATES)
    try:
        with open(RESULTS_FILE, encoding='utf-8') as f:
            data = json.load(f)
            for r in data.get('results', []):
                tested.add(r['id'])
            for sid in data.get('all_tested_ids', []):
                tested.add(sid)
    except Exception:
        pass
    return tested


def generate_grid_candidates(tested_ids, n=BATCH_SIZE):
    """
    3軸 × 7券種のパラメータグリッドから未テストの組み合わせをランダムにn件生成。

    Axis A: FUKUSHO変形（f3rank + 前走着順 + オッズ + 人気）
    Axis B: CORNER変形（後方位置 + f3rank + オッズ + 人気）
    Axis C: 大敗末脚変形（f3rank + 大敗着順 + オッズ + 人気）
    × bet_type: tansho / fukusho / umaren / umatan / wide / sanrenpuku / sanrentan
    """
    # 券種ごとのベット設定（高配当券種はベット額を下げる）
    BET_SETTINGS = {
        'tansho':     (0.020, 15000),
        'fukusho':    (0.020, 15000),
        'umaren':     (0.015, 12000),
        'umatan':     (0.015, 12000),
        'wide':       (0.015, 12000),
        'sanrenpuku': (0.010, 8000),
        'sanrentan':  (0.010, 8000),
    }
    ALL_BET_TYPES = list(BET_SETTINGS.keys())
    MULTI_BET_TYPES = ['umaren', 'umatan', 'wide', 'sanrenpuku', 'sanrentan']

    # 相手選定メソッド（単体券種には不要）
    PARTNER_CONFIGS = [
        ('pop_top',    {},                          '人気1位'),
        ('odds_range', {'partner_odds_min': 3.0, 'partner_odds_max': 8.0},  'オッズ3-8倍'),
        ('odds_range', {'partner_odds_min': 5.0, 'partner_odds_max': 15.0}, 'オッズ5-15倍'),
        ('f3_best',    {},                          '上がり履歴最上位'),
        ('box_top3',   {},                          'ボックス上位3頭'),
    ]

    all_candidates = []

    # ── Axis A: FUKUSHO変形 ──
    for f3, fin, (omin, omax), (pmin, pmax), bt in itertools.product(
        [1, 2],
        [6, 7, 8, 9, 10],
        [(10,14),(12,16),(13,17),(14,18),(15,19),(16,20),
         (14,20),(12,18),(16,22),(18,26),(20,30),(12,22),(14,24)],
        [(4,9),(5,10),(6,11),(6,12),(7,12),(7,13),(8,14),(8,15),(9,16),(5,12),(4,10)],
        ALL_BET_TYPES,
    ):
        bp, bm = BET_SETTINGS[bt]
        partner_iter = PARTNER_CONFIGS if bt in MULTI_BET_TYPES else [('pop_top', {}, '')]
        for pm, pm_extra, pm_label in partner_iter:
            pm_tag = f'_{pm}' if pm != 'pop_top' else ''
            sid = f'F_f{f3}_r{fin}_o{omin}-{omax}_p{pmin}-{pmax}_{bt}{pm_tag}'
            if sid in tested_ids:
                continue
            entry = {
                'id': sid,
                'name': f'FUKUSHO変形[{bt}] f3≤{f3} 着≥{fin} {omin}-{omax}倍 {pmin}-{pmax}人気',
                'status': 'candidate', 'type': 'prev_history',
                'bet_type': bt, 'partner_method': pm,
                'conditions': {
                    'f3rank_max': f3, 'prev_finish_min': fin,
                    'odds_min': float(omin), 'odds_max': float(omax),
                    'pop_min': pmin, 'pop_max': pmax, 'field_min': 8,
                },
                'bet_pct': bp, 'bet_max': bm,
                'note': f'グリッド: FUKUSHO変形 [{bt}] 相手:{pm_label}',
            }
            entry.update(pm_extra)
            all_candidates.append(entry)

    # ── Axis B: CORNER変形（後方スタート）──
    for lc, f3, (omin, omax), (pmin, pmax), bt in itertools.product(
        [6, 7, 8, 9],
        [1, 2, 3],
        [(10,18),(12,20),(14,22),(16,25),(18,30)],
        [(4,10),(5,12),(6,14)],
        ALL_BET_TYPES,
    ):
        bp, bm = BET_SETTINGS[bt]
        partner_iter = PARTNER_CONFIGS if bt in MULTI_BET_TYPES else [('pop_top', {}, '')]
        for pm, pm_extra, pm_label in partner_iter:
            pm_tag = f'_{pm}' if pm != 'pop_top' else ''
            sid = f'C_lc{lc}_f{f3}_o{omin}-{omax}_p{pmin}-{pmax}_{bt}{pm_tag}'
            if sid in tested_ids:
                continue
            entry = {
                'id': sid,
                'name': f'CORNER変形[{bt}] コーナー≥{lc} f3≤{f3} {omin}-{omax}倍 {pmin}-{pmax}人気',
                'status': 'candidate', 'type': 'prev_history',
                'bet_type': bt, 'partner_method': pm,
                'conditions': {
                    'last_corner_min': lc, 'f3rank_max': f3,
                    'odds_min': float(omin), 'odds_max': float(omax),
                    'pop_min': pmin, 'pop_max': pmax, 'field_min': 8,
                },
                'bet_pct': bp, 'bet_max': bm,
                'note': f'グリッド: CORNER変形 [{bt}] 相手:{pm_label}',
            }
            entry.update(pm_extra)
            all_candidates.append(entry)

    # ── Axis C: 大敗末脚変形 ──
    for f3, fin, (omin, omax), (pmin, pmax), bt in itertools.product(
        [1, 2],
        [8, 9, 10, 11],
        [(12,22),(14,24),(15,28),(18,30),(12,18)],
        [(5,12),(6,14),(7,16),(8,16)],
        ALL_BET_TYPES,
    ):
        bp, bm = BET_SETTINGS[bt]
        partner_iter = PARTNER_CONFIGS if bt in MULTI_BET_TYPES else [('pop_top', {}, '')]
        for pm, pm_extra, pm_label in partner_iter:
            pm_tag = f'_{pm}' if pm != 'pop_top' else ''
            sid = f'H_f{f3}_r{fin}_o{omin}-{omax}_p{pmin}-{pmax}_{bt}{pm_tag}'
            if sid in tested_ids:
                continue
            entry = {
                'id': sid,
                'name': f'大敗末脚変形[{bt}] f3≤{f3} 着≥{fin} {omin}-{omax}倍 {pmin}-{pmax}人気',
                'status': 'candidate', 'type': 'prev_history',
                'bet_type': bt, 'partner_method': pm,
                'conditions': {
                    'f3rank_max': f3, 'prev_finish_min': fin,
                    'odds_min': float(omin), 'odds_max': float(omax),
                    'pop_min': pmin, 'pop_max': pmax, 'field_min': 8,
                },
                'bet_pct': bp, 'bet_max': bm,
                'note': f'グリッド: 大敗末脚変形 [{bt}] 相手:{pm_label}',
            }
            entry.update(pm_extra)
            all_candidates.append(entry)

    random.shuffle(all_candidates)
    total_remaining = len(all_candidates)
    return all_candidates[:n], total_remaining


# ═══════════════════════════════════════════════════
# JSON 保存
# ═══════════════════════════════════════════════════
def save_results(results, passed, skipped, stacked=None):
    stacked = stacked or []
    # 既存データを引き継いで累積
    prev_tested  = set()
    prev_stacked = []
    try:
        with open(RESULTS_FILE, encoding='utf-8') as f:
            prev_data = json.load(f)
            prev_tested  = set(prev_data.get('all_tested_ids', []))
            prev_stacked = prev_data.get('stacked', [])
    except Exception:
        pass

    new_ids = set(r['id'] for r in results)
    all_tested = sorted(prev_tested | new_ids)

    # スタック: 既存に同IDがなければ追加
    stacked_ids = set(s['id'] for s in prev_stacked)
    merged_stacked = prev_stacked + [
        {'id': r['id'], 'name': r['name'], 'roi': r['roi'], 'total': r['total'], 'note': r.get('note','')}
        for r in stacked if r['id'] not in stacked_ids
    ]

    data = {
        'run_date':       datetime.now().strftime('%Y-%m-%d %H:%M'),
        'threshold':      THRESHOLD_PCT,
        'min_bets':       MIN_BETS,
        'passed':         passed,
        'tested':         len(results),
        'all_tested_ids': all_tested,
        'stacked':        merged_stacked,
        'results':        results,
    }
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'\n結果保存: {RESULTS_FILE}  (累積テスト済み: {len(all_tested)}件 / スタック: {len(merged_stacked)}件)')


# ═══════════════════════════════════════════════════
# Notion 投稿
# ═══════════════════════════════════════════════════
def post_to_notion(results, passed_results):
    token = NOTION_TOKEN
    if not token:
        print('\n[Notion] NOTION_TOKEN が未設定のためスキップ')
        return

    today = datetime.now().strftime('%Y-%m-%d')
    title = f'戦略ループ実行結果 {today}'

    # テーブル行を構築
    table_rows = '| 戦略名 | 件数 | 的中率 | ROI | 判定 |\n| --- | --- | --- | --- | --- |\n'
    for r in results:
        table_rows += f'| {r["name"]} | {r["total"]}件 | {r["hit_rate"]}% | {r["roi"]}% | {r["pass_msg"]} |\n'

    impl_section = ''
    if passed_results:
        impl_section = '\n\n## 採用候補（ROI 130%超）\n\n'
        for r in passed_results:
            impl_section += f'### {r["name"]}  ROI {r["roi"]}% / {r["total"]}件\n\n'
            impl_section += f'{r.get("note","")}\n\n'

    content = f'''## 実行日時: {today}

**閾値**: ROI {THRESHOLD_PCT}%以上 かつ {MIN_BETS}件以上

## テスト結果サマリー

{table_rows}

採用: **{len(passed_results)}件** / テスト: **{len(results)}件**
{impl_section}
---
*strategy_loop.py により自動生成*
'''

    payload = {
        'parent': {'page_id': NOTION_PARENT_ID},
        'properties': {'title': {'title': [{'text': {'content': title}}]}},
        'children': [
            {
                'object': 'block',
                'type': 'paragraph',
                'paragraph': {'rich_text': [{'text': {'content': content}}]},
            }
        ],
    }

    headers = {
        'Authorization': f'Bearer {token}',
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
    }
    resp = requests.post(NOTION_API_URL, headers=headers, json=payload, timeout=15)
    if resp.status_code in (200, 201):
        data = resp.json()
        print(f'\n[Notion] 投稿成功: {data.get("url", "")}')
    else:
        print(f'\n[Notion] 投稿失敗: {resp.status_code} {resp.text[:200]}')


# ═══════════════════════════════════════════════════
# Discord 通知
# ═══════════════════════════════════════════════════
def post_to_discord(results, passed_results, stacked_results=None):
    stacked_results = stacked_results or []
    url = DISCORD_WEBHOOK_URL
    if not url:
        print('\n[Discord] DISCORD_WEBHOOK_URL が未設定のためスキップ')
        print('  設定方法: DISCORD_WEBHOOK_URL=<webhook_url> python strategy_loop.py --discord')
        return

    today = datetime.now().strftime('%Y-%m-%d %H:%M')

    if passed_results:
        # 採用確定 → 詳細通知
        messages = []
        for r in passed_results:
            cond = r.get('conditions', {})
            bt   = r.get('bet_type', 'fukusho')

            BET_TYPE_JA = {
                'tansho': '単勝', 'fukusho': '複勝',
                'umaren': '馬連', 'umatan': '馬単',
                'wide': 'ワイド', 'sanrenpuku': '三連複', 'sanrentan': '三連単',
            }
            COND_LABELS = {
                'f3rank_max':      ('前走上がり順位',  lambda v: f'{v}位以内'),
                'prev_finish_min': ('前走着順',        lambda v: f'{v}着以下'),
                'prev_finish_max': ('前走着順',        lambda v: f'{v}着以内'),
                'last_corner_min': ('前走最終コーナー',lambda v: f'{v}番手以降（後方）'),
                'last_corner_max': ('前走最終コーナー',lambda v: f'{v}番手以内（前方）'),
                'margin_max':      ('前走着差',        lambda v: f'{v}馬身以内'),
                'prev_field_min':  ('前走頭数',        lambda v: f'{v}頭以上'),
                'odds_min':        ('今走オッズ',      lambda v: f'{v}倍以上'),
                'odds_max':        ('今走オッズ',      lambda v: f'{v}倍以下'),
                'pop_min':         ('今走人気',        lambda v: f'{v}番人気以下'),
                'pop_max':         ('今走人気',        lambda v: f'{v}番人気以内'),
                'field_min':       ('頭数',            lambda v: f'{v}頭以上'),
            }

            # 条件を日本語で整形（odds_min/maxとpop_min/maxはまとめる）
            cond_lines = []
            if 'f3rank_max' in cond:
                cond_lines.append(f'  ・前走上がり3F: {cond["f3rank_max"]}位以内')
            if 'prev_finish_min' in cond and 'prev_finish_max' in cond:
                cond_lines.append(f'  ・前走着順: {cond["prev_finish_min"]}〜{cond["prev_finish_max"]}着')
            elif 'prev_finish_min' in cond:
                cond_lines.append(f'  ・前走着順: {cond["prev_finish_min"]}着以下')
            elif 'prev_finish_max' in cond:
                cond_lines.append(f'  ・前走着順: {cond["prev_finish_max"]}着以内')
            if 'last_corner_min' in cond:
                cond_lines.append(f'  ・前走最終コーナー: {cond["last_corner_min"]}番手以降（後方）')
            if 'last_corner_max' in cond:
                cond_lines.append(f'  ・前走最終コーナー: {cond["last_corner_max"]}番手以内（前方）')
            if 'margin_max' in cond:
                cond_lines.append(f'  ・前走着差: {cond["margin_max"]}馬身以内')
            if 'prev_field_min' in cond:
                cond_lines.append(f'  ・前走頭数: {cond["prev_field_min"]}頭以上')
            if 'odds_min' in cond or 'odds_max' in cond:
                cond_lines.append(f'  ・今走オッズ: {cond.get("odds_min","?")}〜{cond.get("odds_max","?")}倍')
            if 'pop_min' in cond or 'pop_max' in cond:
                cond_lines.append(f'  ・今走人気: {cond.get("pop_min","?")}〜{cond.get("pop_max","?")}番人気')
            if 'field_min' in cond:
                cond_lines.append(f'  ・頭数: {cond["field_min"]}頭以上')

            # 月別成績（上位5ヶ月）
            monthly = r.get('monthly', [])
            monthly_hit = [m for m in monthly if m['total'] > 0]
            monthly_str = ''
            if monthly_hit:
                monthly_str = '\n**月別成績（件数あり月）:**\n'
                for m in monthly_hit:
                    roi_m = m['ret'] / m['invest'] * 100 if m['invest'] else 0
                    mark = '✅' if roi_m >= 100 else '❌'
                    monthly_str += f'  {m["ym"]} {m["total"]}件 的中{m["hit"]}件 ROI{roi_m:.0f}% {mark}\n'

            readable = make_readable_name({'conditions': cond, 'bet_type': bt})
            block = '\n'.join([
                f'━━━━━━━━━━━━━━━━━━━━',
                f'🎯 **採用戦略確定！** ({today})',
                f'━━━━━━━━━━━━━━━━━━━━',
                f'**{readable}**',
                f'ID: `{r["id"]}`',
                f'',
                f'**成績:**',
                f'  ROI: **{r["roi"]:.1f}%**  |  件数: {r["total"]}件  |  的中率: {r["hit_rate"]:.1f}%',
                f'',
                f'**買い方:** {BET_TYPE_JA.get(bt, bt)}',
                f'  （多馬券の場合、相手馬は人気順上位から自動選定）',
                f'',
                f'**選定条件:**',
                *cond_lines,
                monthly_str,
                f'`python strategy_loop.py` で実装コードを確認',
            ])
            messages.append(block)

        content = '\n'.join(messages)
    else:
        # 採用なし → 簡潔なサマリー
        lines = [f'📊 戦略ループ実行完了 ({today}) — 採用なし']
        lines.append(f'テスト: {len(results)}件 | 閾値: {THRESHOLD_PCT}%')
        tested_total = len(results)
        try:
            with open(RESULTS_FILE, encoding='utf-8') as _f:
                _d = json.load(_f)
                tested_total = len(_d.get('all_tested_ids', []))
        except Exception:
            pass
        lines.append(f'累積テスト済み: {tested_total}件')
        if stacked_results:
            lines.append(f'⏸ スタック追加: {len(stacked_results)}件（10年データで再検証）')
            for r in stacked_results:
                lines.append(f'  {r["id"]} ROI{r["roi"]:.1f}% / {r["total"]}件')
        if results:
            non_stack = [r for r in results if not r.get('stack') and r['total'] >= 5]
            if non_stack:
                best = max(non_stack, key=lambda r: r['roi'])
                lines.append(f'今回最高ROI: {best["name"]} {best["roi"]:.1f}% ({best["total"]}件)')
        content = '\n'.join(lines)

    # Discord は 2000文字制限
    if len(content) > 1990:
        content = content[:1987] + '...'

    payload = {'content': content}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            print('\n[Discord] 通知送信成功')
        else:
            print(f'\n[Discord] 送信失敗: {resp.status_code} {resp.text[:100]}')
    except Exception as e:
        print(f'\n[Discord] エラー: {e}')


# ═══════════════════════════════════════════════════
# メイン
# ═══════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--notion',    action='store_true', help='Notion に結果を投稿')
    parser.add_argument('--discord',   action='store_true', help='Discord に結果を通知')
    parser.add_argument('--data',      default=DATA_DIR,   help='データディレクトリ')
    parser.add_argument('--threshold', type=float, default=THRESHOLD_PCT, help='採用ROI閾値')
    args = parser.parse_args()

    threshold = args.threshold

    print('=' * 65)
    print('=== Strategy Research Loop ===')
    print(f'  データ: {args.data}  閾値: {THRESHOLD_PCT}%  最低件数: {MIN_BETS}件')
    print('=' * 65)

    races = load_data(args.data)
    print(f'読み込み: {len(races)}レース\n')

    results      = []
    passed       = []
    stacked      = []
    skipped      = []

    def run_one(strategy):
        sid = strategy['id']
        if strategy.get('status') == 'implemented':
            roi_a = strategy.get('roi_actual', 0)
            bets  = strategy.get('bets_actual', 0)
            print(f'[{sid}] ✅ 実装済み  ROI {roi_a}% / {bets}件')
            skipped.append(sid)
            return
        if strategy.get('status') == 'pending_verification':
            roi_a = strategy.get('roi_actual', 0)
            bets  = strategy.get('bets_actual', 0)
            print(f'[{sid}] ⏸  検証待ち  ROI {roi_a}% / {bets}件（10年データ揃い次第）')
            skipped.append(sid)
            return
        if strategy.get('type') == 'race_grade':
            print(f'[{sid}] ⏳ grade データ未取得 → スキップ')
            skipped.append(sid)
            return

        print(f'[{sid}] テスト中…  ', end='', flush=True)
        result = run_strategy_backtest(races, strategy)
        if result is None:
            print('スキップ')
            skipped.append(sid)
            return

        print(f'{result["pass_msg"]}  ({result["total"]}件 的中率{result["hit_rate"]}%)')
        results.append(result)
        if result['pass_130']:
            passed.append(result)
            print(f'  → 採用候補！実装コードを生成します')
            result['impl_code'] = generate_impl_code(strategy, result)
        elif result.get('stack'):
            stacked.append(result)
            print(f'  → スタック済み（10年データで再検証）')

    # ── 固定候補をテスト ──
    for strategy in STRATEGY_CANDIDATES:
        run_one(strategy)

    # ── 自動生成候補をテスト ──
    tested_ids = load_tested_ids()
    new_candidates, remaining = generate_grid_candidates(tested_ids, n=BATCH_SIZE)
    print(f'\n--- 自動考案: {len(new_candidates)}件をテスト（未探索残り約{remaining}件）---')
    for strategy in new_candidates:
        run_one(strategy)

    # ── 結果表示 ──
    print()
    print('=' * 65)
    print(f'テスト完了  採用: {len(passed)}件 / スタック: {len(stacked)}件 / テスト: {len(results)}件')
    print('=' * 65)

    if passed:
        print('\n🎯 採用候補（ROI 130%超・50件以上）:')
        for r in passed:
            print(f'\n  ── {r["name"]}  ROI {r["roi"]:.1f}% / {r["total"]}件 ──')
            print(f'  {r["note"]}')
            if 'impl_code' in r:
                print('\n  【実装コード】')
                for line in r['impl_code'].split('\n'):
                    print(f'  {line}')
    else:
        print('\n  採用候補なし（ROI 130%超・50件以上なし）')

    if stacked:
        print(f'\n⏸  スタック（ROI 130%超・件数不足 → 10年データで再検証）:')
        for r in stacked:
            print(f'  [{r["id"]}] ROI {r["roi"]:.1f}% / {r["total"]}件')

    print()
    print('未採用戦略（記録済み）:')
    for r in results:
        if not r['pass_130'] and not r.get('stack'):
            print(f'  [{r["id"]}] {r["roi"]:.1f}% / {r["total"]}件')

    save_results(results, len(passed), len(skipped), stacked)

    if args.notion:
        post_to_notion(results, passed)

    if (args.discord or DISCORD_WEBHOOK_URL) and passed:
        post_to_discord(results, passed, stacked)

    return passed


if __name__ == '__main__':
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()

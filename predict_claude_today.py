# -*- coding: utf-8 -*-
"""
predict_claude_today.py  Claude展開推論 × Python確率モデル 統合予測
=====================================================================
Pythonで特徴量・スコアを計算し、Claude APIに渡して展開推論を加えた予測を出力。

実行:
  python predict_claude_today.py --date 20260412
  python predict_claude_today.py --date 20260412 --track_cond 重 --model claude-sonnet-4-6
"""
import os, sys, json, math, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

from collections import defaultdict

# .env からAPIキーを自動読み込み
def _load_dotenv():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    k = k.strip(); v = v.strip()
                    if k not in os.environ:
                        os.environ[k] = v
_load_dotenv()

import anthropic

# predict_prob_today の関数をそのまま再利用
from predict_prob_today import (
    load_oikiri, compute_oikiri_scores,
    load_races_json, build_race_info, score_race, compute_features_live,
    confidence_label, VENUE_NAME,
)
from analyze_axis_prob import logistic_fit, predict_prob, collect
from backtest_GEKIUMA_TENKAI import (
    build_jockey_style_db, build_course_pace_db, build_cond_pace_db,
    TUNE_START, TUNE_END,
)
from uscore_backtest import load_all_csv_races, _add_races_to_horse_db


# ══════════════════════════════════════════════════════
# Claude プロンプト構築
# ══════════════════════════════════════════════════════

def build_prompt(race_info, profiles, scores, adj_senko_map,
                 pace_label, pace_score, oikiri_scores,
                 p_win, p_place):
    """レースデータを構造化してClaudeへのプロンプトを作成"""

    top_names = sorted(scores, key=lambda n: scores[n], reverse=True)
    MARKS = ['◎', '○', '▲', '△', '×', '☆']

    # 脚質分布
    style_count = defaultdict(int)
    for n, p in profiles.items():
        if p:
            style_count[p.get('style', '不明')] += 1

    # 上位6頭の情報
    top_horses_lines = []
    for i, name in enumerate(top_names[:6]):
        mark = MARKS[i] if i < len(MARKS) else f'  {i+1}位'
        p = profiles.get(name)
        style = p.get('style', '?') if p else '?'
        adj_s = adj_senko_map.get(name, 0.5)

        ok = oikiri_scores.get(name, {})
        ok_str = ''
        if ok:
            ok_str = (f' [追切:{ok.get("eval","-")}'
                      f'/{ok.get("style","-")}]')

        spd = p.get('grade_adj_speed') or p.get('speed_mps') if p else None
        spd_str = f'{spd:.2f}m/s' if spd else '-'
        grade = p.get('grade_level', 0.35) if p else 0.35

        top_horses_lines.append(
            f'{mark} {name} '
            f'(score={scores[name]:.3f} / {style} / 速度:{spd_str} '
            f'/ 前走レベル:{grade:.2f}{ok_str})'
        )

    # 逃げ・先行馬の列挙
    senko_horses = [
        f'{n}(adj_senko={adj_senko_map.get(n,0.5):.2f})'
        for n, p in profiles.items()
        if p and p.get('style') in ('逃げ', '先行')
    ]

    prompt = f"""あなたは競馬の展開分析の専門家です。以下のレースデータを見て、展開・ペース・各馬の有利不利を分析してください。

【レース情報】
コース: {race_info['course']} {race_info['dist']}m  馬場: {race_info['track_cond']}
出走頭数: {race_info['n_field']}頭

【Pythonモデルの展開予測】
ペースラベル: {pace_label}  (スコア: {pace_score:.2f}  ※0=スロー, 1=ハイ)

【脚質分布】
逃げ: {style_count.get('逃げ',0)}頭  先行: {style_count.get('先行',0)}頭  差し: {style_count.get('差し',0)}頭  追い込み: {style_count.get('追い込み',0)}頭

【前に行く馬 (adj_senkoが低い順)】
{chr(10).join(senko_horses) if senko_horses else '(データなし)'}

【スコア上位6頭】
{chr(10).join(top_horses_lines)}

【Pythonモデルの確率推定】
◎ {top_names[0]} の P(1着)={p_win*100:.1f}%  P(3着内)={p_place*100:.1f}%

---
以下の観点で分析し、必ず **JSON形式のみ** で回答してください（前置き・後書き不要）:

1. pace_adjustment: 脚質分布を考慮したとき実際のペースは予測より速い/遅い/適切か (1文)
2. crowding_risk: 逃げ・先行馬が密集して消耗戦になるリスク (低/中/高)
3. leading_horses: 先頭集団（1〜3番手）を形成しそうな馬名をカンマ区切りで列挙 (adj_senkoが低い順)
4. pace_scenario: 逃げ→先行→差し→追い込みの位置取りと展開の流れを具体的に記述 (2〜3文)
5. flow_advantage: 展開で特に有利になる馬名をカンマ区切り (上位6頭から選択)
6. flow_disadvantage: 展開で不利になりそうな馬名をカンマ区切り (上位6頭から選択)
7. axis_eval: ◎馬への評価 (展開・競合面で有利か不利か、1〜2文)
8. key_point: このレースの展開上の最重要ポイント (1文)
9. recommend: 軸馬の信頼度調整の提案 (そのまま/やや下げ/やや上げ/大きく下げ)
10. pred_1: 1着予想馬名 (スコア上位6頭の中から展開・脚質を総合して選択)
11. pred_2: 2着予想馬名
12. pred_3: 3着予想馬名
13. pred_reason: 1〜3着予想の根拠 (1〜2文)

回答フォーマット:
{{"pace_adjustment": "...", "crowding_risk": "低|中|高", "leading_horses": "馬名,馬名", "pace_scenario": "...", "flow_advantage": "馬名,馬名", "flow_disadvantage": "馬名,馬名", "axis_eval": "...", "key_point": "...", "recommend": "そのまま|やや下げ|やや上げ|大きく下げ", "pred_1": "馬名", "pred_2": "馬名", "pred_3": "馬名", "pred_reason": "..."}}"""

    return prompt


# ══════════════════════════════════════════════════════
# Claude API呼び出し
# ══════════════════════════════════════════════════════

def call_claude(prompt: str, model: str = 'claude-sonnet-4-6') -> dict:
    """Claude APIを呼び出し、JSON解析して返す"""
    import re
    client = anthropic.Anthropic()
    try:
        msg = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{'role': 'user', 'content': prompt}]
        )
        raw = msg.content[0].text.strip()

        # ```json...``` ブロック優先で抽出
        m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass

        # フォールバック: 最初の { から最後の } まで
        start = raw.find('{')
        end = raw.rfind('}')
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start:end+1])
            except json.JSONDecodeError:
                pass

        return {'error': f'JSON parse failed: {raw[:200]}'}
    except Exception as e:
        return {'error': str(e)}


# ══════════════════════════════════════════════════════
# 結果表示
# ══════════════════════════════════════════════════════

RECOMMEND_SYMBOL = {
    'そのまま':   '→',
    'やや上げ':   '↑',
    'やや下げ':   '↓',
    '大きく下げ': '↓↓',
}
CROWDING_COLOR = {'低': '○', '中': '△', '高': '✗'}


def display_result(r, claude_result, actual=None):
    conf = confidence_label(r['p_place'])
    match_mark = '✓適合' if r['pace_style_match'] else '✗不一致'

    pace_adj    = claude_result.get('pace_adjustment', '-')
    crowding    = claude_result.get('crowding_risk', '-')
    leading     = claude_result.get('leading_horses', '-')
    scenario    = claude_result.get('pace_scenario', '-')
    flow_adv    = claude_result.get('flow_advantage', '-')
    flow_dis    = claude_result.get('flow_disadvantage', '-')
    axis_eval   = claude_result.get('axis_eval', '-')
    key_point   = claude_result.get('key_point', '-')
    recommend   = claude_result.get('recommend', 'そのまま')
    rec_sym     = RECOMMEND_SYMBOL.get(recommend, '→')
    crowd_sym   = CROWDING_COLOR.get(crowding, '?')

    print(f'┌─ {r["venue"]}{r["rnum"]:>2}R  {r["race_name"]}')
    print(f'│  {r["course"]} {r["dist"]}m  {r["n_field"]}頭  馬場:{r["track_cond"]}')
    print(f'│  展開予測: {r["pace_label"]}  {match_mark}')
    print(f'│')
    print(f'│  ◎ {r["axis"]} ({r["axis_pop"]}番人気 / {r["axis_style"]})')
    print(f'│     P(1着)  :  {r["p_win"]*100:>5.1f}%')
    print(f'│     P(3着内):  {r["p_place"]*100:>5.1f}%   {conf}')
    print(f'│')

    pred_1      = claude_result.get('pred_1', '-')
    pred_2      = claude_result.get('pred_2', '-')
    pred_3      = claude_result.get('pred_3', '-')
    pred_reason = claude_result.get('pred_reason', '')

    print(f'│  ── Claude展開分析 ──')
    print(f'│  ペース修正  : {pace_adj}')
    print(f'│  詰まりリスク: {crowd_sym} {crowding}')
    print(f'│  先頭集団    : {leading}')
    print(f'│  展開シナリオ: {scenario}')
    print(f'│  展開有利    : {flow_adv}')
    print(f'│  展開不利    : {flow_dis}')
    print(f'│  ◎評価     : {axis_eval}')
    print(f'│  Key Point  : {key_point}')
    print(f'│  信頼度提案 : {rec_sym} {recommend}')
    print(f'│')
    print(f'│  ── 1〜3着予想 ──')
    print(f'│  1着: {pred_1}')
    print(f'│  2着: {pred_2}')
    print(f'│  3着: {pred_3}')
    if pred_reason:
        print(f'│  根拠: {pred_reason}')

    # 結果照合
    if actual:
        a1 = actual.get('1', '-')
        a2 = actual.get('2', '-')
        a3 = actual.get('3', '-')
        top3_actual = {a1, a2, a3}
        top3_pred   = {pred_1, pred_2, pred_3}
        hit_count   = len(top3_pred & top3_actual)
        exact_win   = '✓' if pred_1 == a1 else '✗'
        print(f'│')
        print(f'│  ── 実際の結果 ──')
        print(f'│  1着: {a1} {exact_win}  2着: {a2}  3着: {a3}')
        print(f'│  的中: {hit_count}/3頭一致 (3着内予想との重複)')

    print(f'│')
    print(f'│  スコア上位5頭:')
    for i, h in enumerate(r['top5']):
        mark = ['◎', '○', '▲', '△', '×'][i]
        ok_str = ''
        if h['ok_score'] is not None:
            t_str = f"{h['ok_t1f']:.1f}秒" if h['ok_t1f'] else '-'
            ok_str = f"  [追切:{h['ok_eval'] or '-'}/{h['ok_style'] or '-'}/{t_str}]"
        print(f'│    {mark} {h["name"]} ({h["pop"]}番/{h["style"]}) '
              f'score={h["score"]:.4f}{ok_str}')
    print(f'└{"─"*60}\n')


# ══════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════

def load_actual_results(date_str: str, data_dir: str) -> dict:
    """
    実際のレース結果を読み込む。
    data/raceresults_{YYYYMM}.csv から対象日のレースを抽出。
    戻り値: { race_id: {'1': 馬名, '2': 馬名, '3': 馬名} }
    """
    import csv
    ym = date_str[:6]
    fpath = os.path.join(data_dir, f'raceresults_{ym}.csv')
    if not os.path.exists(fpath):
        return {}

    results = {}
    with open(fpath, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            rid = row.get('race_id', '').strip()
            if not rid.startswith(date_str[:8].replace('-', '')):
                continue
            rank_str = row.get('rank', '').strip()
            name     = row.get('horse_name', row.get('name', '')).strip()
            if rank_str in ('1', '2', '3') and name:
                if rid not in results:
                    results[rid] = {}
                results[rid][rank_str] = name
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',       required=True)
    parser.add_argument('--track_cond', default='良',
                        choices=['良', '稍重', '重', '不良'])
    parser.add_argument('--model',      default='claude-sonnet-4-6',
                        help='使用するClaudeモデル')
    parser.add_argument('--min_prob',   type=float, default=0.0)
    parser.add_argument('--data_dir',   default='data')
    parser.add_argument('--compare',    action='store_true',
                        help='実際の結果と照合する (raceresults_{YYYYMM}.csv が必要)')
    args = parser.parse_args()

    print('=' * 65)
    print(f'Claude×Python 統合予測  {args.date[:4]}/{args.date[4:6]}/{args.date[6:]}  馬場:{args.track_cond}')
    print(f'モデル: {args.model}')
    print('=' * 65)

    # ── 1. データ読み込み ──
    print('\n[1/3] 履歴データ読み込み中...')
    races = load_all_csv_races(args.data_dir)
    today_ym = args.date[:6]

    horse_db = defaultdict(list)
    _add_races_to_horse_db(horse_db, races, upto_ym=today_ym)
    for n in horse_db:
        horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
    print(f'  {len(races):,}R  horse_db: {len(horse_db):,}頭')

    jockey_style_db = build_jockey_style_db(horse_db)
    course_pace_db  = build_course_pace_db(races, today_ym)
    cond_pace_db    = build_cond_pace_db(races, today_ym)

    # ── 2. 確率モデル学習 ──
    print('\n[2/3] 確率モデル学習中...')
    feat_tune = collect(races, TUNE_START, TUNE_END, walkforward=False)
    w_p, b_p, mu_p, sd_p = logistic_fit(feat_tune, 'hit_place')
    w_w, b_w, mu_w, sd_w = logistic_fit(feat_tune, 'hit_win')
    print(f'  学習レース数: {len(feat_tune):,}')

    # ── 3. 本日レース ──
    print(f'\n[3/3] {args.date} のレース予測 + Claude分析...\n')
    today_races = load_races_json(args.date, args.data_dir)
    oikiri_all  = load_oikiri(args.date, args.data_dir)
    if oikiri_all:
        print(f'  追い切りデータ: {len(oikiri_all)}レース分')
    else:
        print(f'  追い切りデータなし')

    # 重複race_idを除去
    seen = set()
    unique_races = []
    for rj in today_races:
        if rj['race_id'] not in seen:
            seen.add(rj['race_id'])
            unique_races.append(rj)
    print(f'  ユニークレース: {len(unique_races)}R\n')

    results = []
    for race_json in unique_races:
        info = build_race_info(race_json, args.track_cond)
        rid  = info['race_id']
        venue_name = VENUE_NAME.get(info['venue_code'], info['venue_code'])
        rnum = race_json.get('race_num', race_json.get('rnum', '?'))

        oikiri_race   = oikiri_all.get(rid, {})
        oikiri_scores = compute_oikiri_scores(oikiri_race) if oikiri_race else {}

        profiles, scores, adj_senko_map, pace_label, pace_score = score_race(
            info, horse_db, jockey_style_db, course_pace_db, cond_pace_db,
            oikiri_scores=oikiri_scores)

        if scores is None:
            print(f'  {venue_name} {rnum}R [{rid}] → プロファイル不足スキップ')
            continue

        top_names = sorted(scores, key=lambda n: scores[n], reverse=True)
        axis  = top_names[0]
        feats = compute_features_live(
            info, profiles, scores, adj_senko_map, pace_score, pace_label)
        if feats is None:
            continue

        p_place = predict_prob(feats, w_p, b_p, mu_p, sd_p)
        p_win   = predict_prob(feats, w_w, b_w, mu_w, sd_w)

        pop_map  = {h['name']: h['pop'] for h in info['horse_list']}
        axis_pop = pop_map.get(axis, 99)

        top5_oikiri = []
        for n in top_names[:5]:
            ok = oikiri_scores.get(n, {})
            top5_oikiri.append({
                'name': n, 'score': scores[n],
                'pop': pop_map.get(n, 99),
                'style': (profiles[n] or {}).get('style', '?'),
                'ok_eval': ok.get('eval', '-'),
                'ok_style': ok.get('style', '-'),
                'ok_t1f': ok.get('time_1f'),
                'ok_score': ok.get('score'),
            })

        r = {
            'race_id': rid, 'venue': venue_name, 'rnum': rnum,
            'race_name': info['race_name'], 'course': info['course'],
            'dist': info['dist'], 'n_field': info['n_field'],
            'track_cond': args.track_cond,
            'axis': axis, 'axis_pop': axis_pop,
            'axis_style': (profiles[axis] or {}).get('style', '不明'),
            'axis_score': feats['axis_score'],
            'pace_label': pace_label, 'pace_score': pace_score,
            'pace_style_match': feats['pace_style_match'],
            'p_win': p_win, 'p_place': p_place,
            'top5': top5_oikiri,
            '_profiles': profiles,
            '_scores': scores,
            '_adj_senko': adj_senko_map,
            '_oikiri_scores': oikiri_scores,
            '_info': info,
        }

        if p_place >= args.min_prob:
            results.append(r)

    results.sort(key=lambda x: x['p_place'], reverse=True)

    # ── 実結果ロード (--compare 時) ──
    actual_db = {}
    if args.compare:
        actual_db = load_actual_results(args.date, args.data_dir)
        print(f'  実結果ロード: {len(actual_db)}R分')

    # ── Claude API呼び出し (各レース) ──
    print(f'{"=" * 65}')
    print(f'予測結果 ({len(results)}レース) + Claude展開分析')
    print(f'{"=" * 65}\n')

    claude_summaries = []
    for r in results:
        print(f'  [{r["venue"]} {r["rnum"]}R {r["race_name"]}] Claude分析中...', end=' ', flush=True)
        prompt = build_prompt(
            r['_info'], r['_profiles'], r['_scores'], r['_adj_senko'],
            r['pace_label'], r['pace_score'], r['_oikiri_scores'],
            r['p_win'], r['p_place']
        )
        claude_result = call_claude(prompt, args.model)
        print('完了' if 'error' not in claude_result else f'ERROR: {claude_result["error"][:50]}')

        actual = actual_db.get(r['race_id'])
        display_result(r, claude_result, actual=actual)
        claude_summaries.append((r, claude_result, actual))

    # ── サマリー ──
    print(f'{"=" * 65}')
    print('総合サマリー (P(3着内)降順)')
    print(f'{"=" * 65}')
    if args.compare:
        print(f'{"レース":<20} {"予想1着":^12} {"予想2着":^12} {"予想3着":^12} {"実1着":^10} {"的中":>4} {"P3内":>6} {"詰り":>4}')
        print('-' * 88)
        total_hit = 0
        total_r = 0
        for r, cr, actual in claude_summaries:
            crowd    = cr.get('crowding_risk', '-')
            crowd_sym = CROWDING_COLOR.get(crowd, '?')
            p1 = cr.get('pred_1', '-')[:10]
            p2 = cr.get('pred_2', '-')[:10]
            p3 = cr.get('pred_3', '-')[:10]
            label = f'{r["venue"]} {r["rnum"]}R {r["race_name"]}'[:20]
            if actual:
                a1 = actual.get('1', '-')
                top3_a = {actual.get('1',''), actual.get('2',''), actual.get('3','')}
                top3_p = {p1, p2, p3}
                hits = len(top3_p & top3_a)
                total_hit += hits
                total_r += 1
                win_mark = '✓' if p1 == a1 else ' '
                hit_str = f'{hits}/3'
                print(f'{label:<20} {p1:^12} {p2:^12} {p3:^12} {a1:^10} {win_mark}{hit_str:>3} '
                      f'{r["p_place"]*100:>5.1f}%  {crowd_sym}{crowd}')
            else:
                print(f'{label:<20} {p1:^12} {p2:^12} {p3:^12} {"(未定)":^10} {"  -":>4} '
                      f'{r["p_place"]*100:>5.1f}%  {crowd_sym}{crowd}')
        print(f'{"=" * 88}')
        if total_r > 0:
            print(f'照合済み: {total_r}R  3着内合計的中: {total_hit}/{total_r*3} ({total_hit/(total_r*3)*100:.1f}%)')
    else:
        print(f'{"レース":<20} {"1着":^12} {"2着":^12} {"3着":^12} {"P3内":>6} {"詰り":>4}')
        print('-' * 72)
        for r, cr, _ in claude_summaries:
            crowd    = cr.get('crowding_risk', '-')
            crowd_sym = CROWDING_COLOR.get(crowd, '?')
            p1 = cr.get('pred_1', '-')[:10]
            p2 = cr.get('pred_2', '-')[:10]
            p3 = cr.get('pred_3', '-')[:10]
            label = f'{r["venue"]} {r["rnum"]}R {r["race_name"]}'[:20]
            print(f'{label:<20} {p1:^12} {p2:^12} {p3:^12} '
                  f'{r["p_place"]*100:>5.1f}%  {crowd_sym}{crowd}')
        print(f'{"=" * 72}')


if __name__ == '__main__':
    main()

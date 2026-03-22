"""
agents/analyst.py  分析エージェント（スコア計算専任）
selector.pyからスコア計算ロジックのみを分離
- スコアリングモデル v4.0で各馬を評価
- 条件C/A'判定
- スコア上位3頭を選出
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from score_v4 import (
    load_models, calc_score,
    judge_condition, prev_rank_flag,
)
from pathlib import Path

DATA_DIR = Path('data')
_js = _dc = None


def _load():
    global _js, _dc
    if _js is None:
        _js, _dc = load_models(str(DATA_DIR))
    return _js, _dc


def analyze_race(race: dict) -> dict | None:
    """
    1レースを分析してスコア結果を返す
    Returns: {
        'race_id', 'race_name', 'course', 'dist', 'n_horses',
        'condition', 'gap', 'scores': [{name, score, ...}],
        'best': {name, jockey, odds, score}
    } or None
    """
    js, dc = _load()

    dist    = race.get('dist', 0)
    n       = race.get('n_horses', 99)
    course  = race.get('course', 'ダート')
    odds    = race.get('odds', [])
    horses  = race.get('horses', {})

    if not odds:
        return None

    results = []
    for o in odds:
        horse_id = o.get('horse_id')
        name     = o.get('name', '')
        jockey   = horses.get(horse_id, {}).get('jockey', '') if horse_id else ''
        history  = race.get('histories', {}).get(horse_id, [])

        score, detail = calc_score(
            name=name, jockey=jockey, odds_val=o['odds'],
            popularity=o['pop'], history=history,
            dist=dist, course=course, js=js, dc=dc
        )
        results.append({
            'name':    name,
            'jockey':  jockey,
            'odds':    o['odds'],
            'pop':     o['pop'],
            'score':   score,
            'detail':  detail,
            'flag':    prev_rank_flag(history),
        })

    results.sort(key=lambda x: x['score'], reverse=True)

    if len(results) < 3:
        return None

    gap  = round(results[0]['score'] - results[2]['score'], 1)
    cond = judge_condition(results[0]['odds'], gap)

    if cond is None:
        return None

    return {
        'race_id':   race['race_id'],
        'race_name': race.get('race_name', ''),
        'course':    course,
        'dist':      dist,
        'n_horses':  n,
        'condition': cond,
        'gap':       gap,
        'scores':    results,
        'best':      results[0],
        'start_time': race.get('start_time', ''),
    }


def analyze_races(races_data: dict) -> list[dict]:
    """複数レースをまとめて分析"""
    candidates = races_data.get('candidates', [])
    analyzed = []
    for race in candidates:
        result = analyze_race(race)
        if result:
            analyzed.append(result)
            print(f"[analyst] 分析完了: {result['race_name']} 条件{result['condition']} スコア差{result['gap']}pt")
    print(f"[analyst] 参加候補: {len(analyzed)}レース")
    return analyzed

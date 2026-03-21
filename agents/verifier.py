"""
agents/verifier.py  エージェント②（反証）
エージェント①の選出に対して批判的に評価し、
見落とし・リスクを指摘する
"""

import anthropic
import json


VERIFIER_SYSTEM = """あなたは競馬予想の反証専門エージェントです。
エージェント①の予想に対して、見落としやリスクを厳しく指摘してください。

以下の観点で必ず評価してください：
1. **脚質の懸念** - 展開が向かない馬はいないか（先行馬ばかり or 差し馬ばかり）
2. **前走ショック** - 前走大敗後で今走も危険な馬はないか
3. **距離適性** - 今回の距離が過去実績と大きく乖離していないか
4. **騎手リスク** - 乗り替わりや腕前に懸念はないか
5. **オッズの歪み** - 本命馬のオッズが前日から大きく動いている場合の警戒
6. **クラス変更** - 昇級馬・降格馬がいないか
7. **三連複の落とし穴** - スコア上位3頭が全員同じ脚質でハマリ展開になりうるか

出力フォーマット：
- 賭けてよい：理由を1〜2行で
- リスク事項：箇条書きで最大5点
- 最終評価：「推奨」「要注意」「見送り推奨」のいずれか"""


def run_verifier(selector_output: str, race_details: list[dict]) -> dict:
    """
    エージェント①の出力に対して反証を行う
    
    Args:
        selector_output: エージェント①のテキスト出力
        race_details: スコア計算結果のリスト
    
    Returns:
        {verdict: str, risks: list, final: str, raw: str}
    """
    client = anthropic.Anthropic()

    # レース詳細情報を構造化
    race_summary = []
    for r in race_details:
        scores_summary = [
            f"{h['pop']}人気 {h['name']}（{h['etype']}・上がりPt{h['agari_pt']}・スコア{h['score']}）"
            for h in r['scores'][:5]
        ]
        race_summary.append(
            f"【{r['race_name']}】{r['course']}{r['dist']}m {r['n_horses']}頭\n"
            f"本命: {r['best']['name']} {r['best']['odds']}倍 {r['best']['pop']}人気\n"
            f"スコア差: {r['gap']}pt\n"
            f"スコア上位5頭:\n" + "\n".join(f"  {s}" for s in scores_summary)
        )

    prompt = f"""以下はエージェント①による本日の競馬予想です。
反証・リスク分析をお願いします。

【エージェント①の予想】
{selector_output}

【スコア詳細】
{chr(10).join(race_summary)}

上記に対してシステムプロンプトの観点で反証してください。
最終的に「推奨」「要注意」「見送り推奨」のいずれかで締めてください。"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=VERIFIER_SYSTEM,
        messages=[{"role": "user", "content": prompt}]
    )

    raw_text = response.content[0].text

    # 最終評価を抽出
    final = "要注意"
    if "推奨" in raw_text and "見送り" not in raw_text:
        final = "推奨"
    elif "見送り推奨" in raw_text:
        final = "見送り推奨"

    return {
        "verdict": final,
        "raw":     raw_text,
    }


def format_verifier_output(result: dict) -> str:
    """Discord通知用のテキスト整形"""
    verdict_emoji = {
        "推奨":    "✅",
        "要注意":  "⚠️",
        "見送り推奨": "❌",
    }
    emoji = verdict_emoji.get(result['verdict'], "❓")
    return (
        f"### エージェント②（反証）の評価\n"
        f"最終評価: {emoji} **{result['verdict']}**\n\n"
        f"{result['raw']}"
    )


if __name__ == '__main__':
    # テスト
    test_selector = """中京9R フローラルウォーク賞
◎ ピエドゥラパン（横山武史）3.7倍
○ チュウワカーネギー（西村淳）4.0倍
▲ コルテオソレイユ（浜中俊）5.3倍
条件A' 判定"""

    test_races = [{
        'race_name': '中京9R フローラルウォーク賞',
        'course': '芝', 'dist': 2000, 'n_horses': 10,
        'best': {'name': 'ピエドゥラパン', 'odds': 3.7, 'pop': 1},
        'gap': 3.1,
        'scores': [
            {'pop':1,'name':'ピエドゥラパン','etype':'好位','agari_pt':6.8,'score':48.9},
            {'pop':2,'name':'チュウワカーネギー','etype':'先行','agari_pt':6.3,'score':45.8},
            {'pop':3,'name':'コルテオソレイユ','etype':'好位','agari_pt':7.9,'score':44.9},
            {'pop':7,'name':'アイガーリー','etype':'好位','agari_pt':6.3,'score':34.3},
            {'pop':4,'name':'ネッタイヤライ','etype':'差し','agari_pt':9.2,'score':37.5},
        ]
    }]

    result = run_verifier(test_selector, test_races)
    print(format_verifier_output(result))

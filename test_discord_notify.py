import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import requests

DISCORD_WEBHOOK_URL = (
    'https://discord.com/api/webhooks/1484909461188640915/'
    'W9fEb0xSlVFbh7k-7trQ45nFrZTKo2Cb1P8DKZMbsIZxeHdLPt8HK65yWdPGyc3cyZ_q'
)

BET_TYPE_JA = {
    'tansho': '単勝', 'fukusho': '複勝',
    'umaren': '馬連', 'umatan': '馬単',
    'wide':   'ワイド', 'sanrenpuku': '三連複', 'sanrentan': '三連単',
}
BET_COND = {
    'tansho':     '1着で的中',
    'fukusho':    '3着以内で的中',
    'umaren':     'アンカー+1番人気が1〜2着（順不同）',
    'umatan':     'アンカー1着・1番人気2着',
    'wide':       'アンカー+1番人気が3着以内に2頭',
    'sanrenpuku': 'アンカー+1〜2番人気が1〜3着（順不同）',
    'sanrentan':  'アンカー1着・1番人気2着・2番人気3着',
}

r = {
    'name': 'FUKUSHO変形[fukusho] f3<=1 着>=7 14-18倍 6-12人気',
    'roi': 187.3, 'total': 54, 'hit': 22, 'hit_rate': 40.7,
    'bet_type': 'fukusho',
    'conditions': {
        'f3rank_max': 1, 'prev_finish_min': 7,
        'odds_min': 14.0, 'odds_max': 18.0,
        'pop_min': 6, 'pop_max': 12, 'field_min': 8,
    },
    'monthly': [
        {'ym': '2024-03', 'total': 2, 'hit': 1, 'invest': 2000, 'ret': 3200},
        {'ym': '2024-08', 'total': 2, 'hit': 0, 'invest': 2000, 'ret': 0},
        {'ym': '2025-01', 'total': 4, 'hit': 2, 'invest': 4000, 'ret': 9800},
        {'ym': '2025-06', 'total': 3, 'hit': 1, 'invest': 3000, 'ret': 4100},
    ],
}

cond = r['conditions']
bt   = r['bet_type']

cond_lines = []
if 'f3rank_max' in cond:
    cond_lines.append(f'  前走上がり3F順位: {cond["f3rank_max"]}位以内（最速クラス）')
if 'prev_finish_min' in cond and 'prev_finish_max' in cond:
    cond_lines.append(f'  前走着順: {cond["prev_finish_min"]}〜{cond["prev_finish_max"]}着')
elif 'prev_finish_min' in cond:
    cond_lines.append(f'  前走着順: {cond["prev_finish_min"]}着以下（大敗）')
elif 'prev_finish_max' in cond:
    cond_lines.append(f'  前走着順: {cond["prev_finish_max"]}着以内')
if 'last_corner_min' in cond:
    cond_lines.append(f'  前走最終コーナー: {cond["last_corner_min"]}番手以降（後方待機）')
if 'last_corner_max' in cond:
    cond_lines.append(f'  前走最終コーナー: {cond["last_corner_max"]}番手以内（先行）')
if 'margin_max' in cond:
    cond_lines.append(f'  前走着差: {cond["margin_max"]}馬身以内')
if 'prev_field_min' in cond:
    cond_lines.append(f'  前走頭数: {cond["prev_field_min"]}頭以上の大レース')
if 'odds_min' in cond or 'odds_max' in cond:
    cond_lines.append(f'  今走オッズ: {cond.get("odds_min","?")}〜{cond.get("odds_max","?")}倍')
if 'pop_min' in cond or 'pop_max' in cond:
    cond_lines.append(f'  今走人気: {cond.get("pop_min","?")}〜{cond.get("pop_max","?")}番人気')
if 'field_min' in cond:
    cond_lines.append(f'  頭数: {cond["field_min"]}頭立て以上')

monthly_lines = ['月別成績:']
for m in r['monthly']:
    roi_m = m['ret'] / m['invest'] * 100 if m['invest'] else 0
    mark = 'OK' if roi_m >= 100 else '--'
    monthly_lines.append(
        f'  {m["ym"]}  {m["total"]}件  的中{m["hit"]}件  ROI{roi_m:.0f}%  [{mark}]'
    )

import sys
sys.path.insert(0, '.')
from strategy_loop import make_readable_name
readable = make_readable_name({'conditions': cond, 'bet_type': bt})

lines = [
    '━━━━━━━━━━━━━━━━━━━━',
    '🎯 **採用戦略確定！**（テスト送信）',
    '━━━━━━━━━━━━━━━━━━━━',
    f'**{readable}**',
    f'ID: `{r["name"]}`',
    '',
    '**バックテスト成績（23ヶ月）:**',
    f'  ROI: **{r["roi"]:.1f}%**  /  {r["total"]}件  /  的中率 {r["hit_rate"]:.1f}%',
    '',
    f'**買い方:** {BET_TYPE_JA.get(bt, bt)}',
    f'  条件: {BET_COND.get(bt, "")}',
    '',
    '**選定条件（前走データ → 今走フィルター）:**',
]
lines += cond_lines
lines += ['']
lines += monthly_lines
lines += ['', '`python strategy_loop.py` で実装コードを確認してください']

content = '\n'.join(lines)
if len(content) > 1990:
    content = content[:1987] + '...'

resp = requests.post(DISCORD_WEBHOOK_URL, json={'content': content}, timeout=10)
print(f'送信結果: {resp.status_code}')
print(f'文字数: {len(content)}')

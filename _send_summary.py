import sys, requests
sys.stdout.reconfigure(encoding='utf-8')

WEBHOOK = 'https://discord.com/api/webhooks/1484909461188640915/W9fEb0xSlVFbh7k-7trQ45nFrZTKo2Cb1P8DKZMbsIZxeHdLPt8HK65yWdPGyc3cyZ_q'

msg = {
  'embeds': [{
    'title': '📋 今日やったこと・次回の続きメモ (2026-04-19)',
    'color': 0x3498DB,
    'fields': [
      {
        'name': '🏇 今日わかったこと',
        'value': (
          '**三連単 vs 三連複、どっちがいいか比べた**\n'
          '→ 三連複のtop4ボックス買い（4点）が一番安定して稼げる\n'
          '→ 三連単は当たっても点数・投資額が多くて結局効率が悪い\n'
          '→ 今後も三連複top4ボックスで行く方針で確定\n\n'
          '**地方競馬でも同じモデルを使えるか試した**\n'
          '→ ほぼ機能しなかった（340レース中5件しか賭け対象にならない）\n'
          '→ 理由: 地方の馬はJRAの出走歴がないのでモデルがスコアをつけられない\n'
          '→ 地方の馬は地方の成績を見るように修正したが、それでも改善が少ない\n'
          '→ 根本的には「地方競馬専用のモデル」を作る必要がある'
        ),
        'inline': False
      },
      {
        'name': '⏳ 今夜バックグラウンドで動いていること',
        'value': (
          'GitHubで地方競馬の過去データ（2020〜2025年）を自動収集中\n'
          '→ 現在2020〜2025年それぞれ5月分まで取得済み\n'
          '→ 全部終わるまであと2〜3時間かかる予定\n'
          '→ 年ごとに完了したらDiscordに通知が来る\n'
          '進捗確認: https://github.com/Kenseimk/keiba-agent/actions/runs/24628216758'
        ),
        'inline': False
      },
      {
        'name': '▶️ 次回セッションでやること',
        'value': (
          '① Discordに「NAR 202X年 取得完了」が届いているか確認\n'
          '② 地方競馬の過去データでモデルを学習させる\n'
          '③ 改めて地方競馬のシミュレーションを走らせて結果を見る\n'
          '④ 良さそうなら馬連戦略も地方で試す'
        ),
        'inline': False
      }
    ],
    'footer': {'text': 'keiba-agent 開発メモ'}
  }]
}

r = requests.post(WEBHOOK, json=msg, timeout=10)
print('Discord送信:', r.status_code, '(204=成功)')

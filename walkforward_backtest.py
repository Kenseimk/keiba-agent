# -*- coding: utf-8 -*-
"""
walkforward_backtest.py  ウォークフォワード バックテスト
=========================================================
各テスト期間のモデルはその期間より前のデータのみで学習。
テストデータは完全にブラインド（未使用）。

期間設定（3年学習 / 1年バリデーション / 1年テスト）:
  Period 1: TRAIN=2019-2021, VAL=2022, TEST=2023
  Period 2: TRAIN=2020-2022, VAL=2023, TEST=2024
  Period 3: TRAIN=2021-2023, VAL=2024, TEST=2025
  Period 4: TRAIN=2022-2024, VAL=2025, TEST=2026/01-03

実行:
  python walkforward_backtest.py
  python walkforward_backtest.py --skip_train  (学習済みモデルを再利用)
"""
import os, sys, subprocess, shutil, argparse
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from collections import defaultdict

# ── ウォークフォワード期間定義 ────────────────────────
PERIODS = [
    {
        'label':       'Period1 (TEST=2023)',
        'train_years': [2019, 2020, 2021],
        'val_years':   [2022],
        'test_start':  '202301',
        'test_end':    '202312',
        'model_dir':   'models/wf_2023',
    },
    {
        'label':       'Period2 (TEST=2024)',
        'train_years': [2020, 2021, 2022],
        'val_years':   [2023],
        'test_start':  '202401',
        'test_end':    '202412',
        'model_dir':   'models/wf_2024',
    },
    {
        'label':       'Period3 (TEST=2025)',
        'train_years': [2021, 2022, 2023],
        'val_years':   [2024],
        'test_start':  '202501',
        'test_end':    '202512',
        'model_dir':   'models/wf_2025',
    },
    {
        'label':       'Period4 (TEST=2026)',
        'train_years': [2022, 2023, 2024],
        'val_years':   [2025],
        'test_start':  '202601',
        'test_end':    '202603',
        'model_dir':   'models/wf_2026',
    },
]

MODEL_FILES = (
    ['ml_model_win.txt', 'ml_model_win_bin.txt', 'ml_model_place.txt',
     'ml_model_win_lambda.txt', 'ml_model_place_lambda.txt'] +
    [f'ml_model_win_e{i}.txt'           for i in range(3)] +
    [f'ml_model_place_e{i}.txt'         for i in range(3)] +
    [f'ml_model_win_lambda_e{i}.txt'    for i in range(3)] +
    [f'ml_model_place_lambda_e{i}.txt'  for i in range(3)] +
    [f'ml_model_win_turf_e{i}.txt'      for i in range(3)] +
    [f'ml_model_place_turf_e{i}.txt'    for i in range(3)] +
    [f'ml_model_win_dirt_e{i}.txt'      for i in range(3)] +
    [f'ml_model_place_dirt_e{i}.txt'    for i in range(3)] +
    ['ml_model_win_turf.txt', 'ml_model_place_turf.txt',
     'ml_model_win_dirt.txt', 'ml_model_place_dirt.txt']
)


def run_training(period, skip=False):
    model_dir = period['model_dir']
    os.makedirs(model_dir, exist_ok=True)

    if skip and all(os.path.exists(os.path.join(model_dir, f)) for f in MODEL_FILES[:4]):
        print(f'  [SKIP] 学習済みモデルを使用: {model_dir}')
        return True

    print(f'\n{"="*60}')
    print(f'【学習】{period["label"]}')
    print(f'  TRAIN: {period["train_years"]}  VAL: {period["val_years"]}')
    print(f'  保存先: {model_dir}')
    print(f'{"="*60}')

    train_args = ' '.join(str(y) for y in period['train_years'])
    val_args   = ' '.join(str(y) for y in period['val_years'])

    cmd = [sys.executable, 'train_ml_model.py',
           '--train_years'] + [str(y) for y in period['train_years']] + \
          ['--val_years']   + [str(y) for y in period['val_years']]

    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        print(f'  [ERROR] 学習失敗')
        return False

    # モデルファイルをperiod固有ディレクトリにコピー
    copied = 0
    for fname in MODEL_FILES:
        if os.path.exists(fname):
            shutil.copy2(fname, os.path.join(model_dir, fname))
            copied += 1
    print(f'\n  → {copied}モデルファイルを {model_dir} にコピー完了')
    return True


def run_backtest(period):
    print(f'\n{"="*60}')
    print(f'【バックテスト】{period["label"]}')
    print(f'  期間: {period["test_start"]} 〜 {period["test_end"]}')
    print(f'  モデル: {period["model_dir"]}')
    print(f'{"="*60}')

    cmd = [sys.executable, 'backtest_SANRENPUKU_ml.py',
           '--start', period['test_start'],
           '--end',   period['test_end'],
           '--lambda', 'course',
           '--aite_mode', 'place',
           '--dynamic_aite',
           '--model_dir', period['model_dir']]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    output = result.stdout
    print(output)
    return parse_summary(output)


def parse_summary(output):
    """バックテスト出力から合計行をパース"""
    for line in output.splitlines():
        if line.strip().startswith('合計'):
            parts = line.split()
            try:
                races = int(parts[1].replace(',', ''))
                hits  = int(parts[2].replace(',', ''))
                cost  = int(parts[4].replace(',', ''))
                ret   = int(parts[5].replace(',', ''))
                return {'races': races, 'hits': hits, 'cost': cost, 'ret': ret}
            except:
                pass
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip_train', action='store_true',
                        help='学習済みモデルがあればスキップ')
    args = parser.parse_args()

    print('\n' + '='*60)
    print('=== ウォークフォワード バックテスト ===')
    print('過学習なし: 各テスト期間は完全ブラインド')
    print('='*60)

    all_results = []

    for period in PERIODS:
        # 1. 学習
        ok = run_training(period, skip=args.skip_train)
        if not ok:
            print(f'  [SKIP] {period["label"]} のバックテストをスキップ')
            continue

        # 2. バックテスト
        summary = run_backtest(period)
        if summary:
            summary['label'] = period['label']
            summary['test_start'] = period['test_start']
            summary['test_end']   = period['test_end']
            all_results.append(summary)

    # ── 全期間集計 ──────────────────────────────────
    if not all_results:
        print('\n結果なし')
        return

    print('\n' + '='*60)
    print('=== ウォークフォワード 全期間集計 ===')
    print('(各期間モデルは完全ブラインド)')
    print('='*60)
    print(f'{"期間":<28}  {"R数":>5}  {"的中":>4}  {"的中率":>6}  {"ROI":>7}  {"収支":>10}')
    print('-' * 70)

    total = defaultdict(int)
    for r in all_results:
        n = r['races']; h = r['hits']; c = r['cost']; ret = r['ret']
        roi = ret / c * 100 if c else 0
        mark = '✓' if roi >= 100 else '✗'
        print(f'{r["label"]:<28}  {n:>5}  {h:>4}  {h/n*100:>5.1f}%  {roi:>6.1f}% {mark}  {ret-c:>+10,}')
        for k in ['races', 'hits', 'cost', 'ret']:
            total[k] += r[k]

    print('-' * 70)
    n = total['races']; h = total['hits']; c = total['cost']; ret = total['ret']
    roi = ret / c * 100 if c else 0
    print(f'{"全期間合計":<28}  {n:>5}  {h:>4}  {h/n*100:>5.1f}%  {roi:>6.1f}%    {ret-c:>+10,}')
    print()
    black = sum(1 for r in all_results if r['ret'] > r['cost'])
    print(f'黒字期間: {black}/{len(all_results)}')
    print(f'月平均投資: {c//((n//40) or 1):,}円  総収支: {ret-c:+,}円')


if __name__ == '__main__':
    main()

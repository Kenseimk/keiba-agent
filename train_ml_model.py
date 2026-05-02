# -*- coding: utf-8 -*-
"""
train_ml_model.py  LightGBM 馬券予測モデル v4
=============================================
ラップタイム(区間速度)特徴量を追加した最終版

特徴量 (106次元):
  - 26因子スコア
  - 10 レース/馬歴特徴量
  - 18 追加特徴量 (速度指数・ペース・ラップ)
  - 52 相対特徴量 (z-score + 相対順位 for 26因子)

学習: 2015-2023  バリデーション: 2024  テスト: 2025-2026
"""
import os, sys
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

import numpy as np
import lightgbm as lgb
import optuna
optuna.logging.set_verbosity(optuna.logging.WARNING)
from collections import defaultdict

from uscore_backtest import load_all_csv_races, _add_races_to_horse_db
from uscore import (
    build_trainer_stats, build_jockey_stats, should_exclude_uscore,
)
from ml_features import (
    FACTOR_KEYS, EXTRA_NAMES, extract_features, add_relative_features, REL_FEAT_IDX,
)

RNUM_MIN  = 8
RNUM_MAX  = 11
MAX_FIELD = 14

print('全データ読み込み中...', flush=True)
races = load_all_csv_races('data')

# ── ウォークフォワード用 CLI引数 ──────────────────────
import argparse as _ap
_wf = _ap.ArgumentParser(add_help=False)
_wf.add_argument('--train_years', nargs='+', type=int, default=None)
_wf.add_argument('--val_years',   nargs='+', type=int, default=None)
_wf_args, _ = _wf.parse_known_args()

TRAIN_YEARS = _wf_args.train_years or list(range(2015, 2024))   # 学習: 2015-2023
VAL_YEARS   = _wf_args.val_years   or [2024]                    # バリデーション: 2024
TEST_YEARS  = [2025, 2026]                                       # テスト: 2025-2026


def collect_data(years, course_filter=None):
    """
    course_filter: None=全コース, 'turf'=芝のみ, 'dirt'=ダートのみ
    """
    X, y_win, y_place, y_rank, meta = [], [], [], [], []
    for year in years:
        ym_start = f'{year}01'
        ym_end   = f'{year}12'
        print(f'  {year}年 特徴量抽出中...', flush=True)

        horse_db = defaultdict(list)
        _add_races_to_horse_db(horse_db, races, upto_ym=ym_start)
        for n in horse_db:
            horse_db[n].sort(key=lambda r: (r['race_ym'], r['race_id']), reverse=True)
        trainer_stats = build_trainer_stats(horse_db)
        jockey_stats  = build_jockey_stats(horse_db)

        test_rids = sorted(rid for rid, info in races.items()
                           if ym_start <= info['file_ym'] <= ym_end)

        for race_id in test_rids:
            info = races[race_id]
            if should_exclude_uscore(info['race_name']): continue
            if all(h['odds'] == 0.0 for h in info['horse_list']): continue
            rnum = int(race_id[-2:])
            if not (RNUM_MIN <= rnum <= RNUM_MAX): continue
            if info['n_field'] > MAX_FIELD: continue

            # コースフィルター
            if course_filter is not None:
                is_dirt = 'ダ' in str(info.get('course', ''))
                if course_filter == 'turf' and is_dirt:    continue
                if course_filter == 'dirt' and not is_dirt: continue

            race_feats = []
            race_meta  = []
            for h in info['horse_list']:
                name = h['name']
                feat = extract_features(name, h, info, horse_db, trainer_stats, jockey_stats)
                if feat is None: continue
                rank = h['rank']
                if rank <= 0: continue
                race_feats.append(feat)
                race_meta.append({
                    'race_id': race_id, 'name': name,
                    'rank': rank, 'odds': h['odds'], 'pop': h['pop'],
                })

            if len(race_feats) < 4: continue

            race_feats_rel = add_relative_features(race_feats)

            for feat, m in zip(race_feats_rel, race_meta):
                X.append(feat)
                y_win.append(1 if m['rank'] == 1 else 0)
                y_place.append(1 if m['rank'] <= 2 else 0)
                # LambdaRank 用グレードラベル: 1着=3, 2着=2, 3着=1, else=0
                y_rank.append(3 if m['rank'] == 1 else (2 if m['rank'] == 2 else
                               (1 if m['rank'] == 3 else 0)))
                meta.append(m)

    return (np.array(X, dtype=np.float32),
            np.array(y_win,   dtype=np.int8),
            np.array(y_place, dtype=np.int8),
            np.array(y_rank,  dtype=np.int32),
            meta)


def make_lambda_dataset(X, y_rank, meta, feat_names, reference=None):
    """race_id でソートして LambdaRank 用 Dataset を作る"""
    from itertools import groupby as _gb
    idx = sorted(range(len(meta)), key=lambda i: meta[i]['race_id'])
    X_s   = X[idx]
    ys    = y_rank[idx]
    ms    = [meta[i] for i in idx]
    grps  = [len(list(g)) for _, g in _gb(ms, key=lambda m: m['race_id'])]
    ds = lgb.Dataset(X_s, label=ys, group=grps, feature_name=feat_names,
                     free_raw_data=False)
    if reference is not None:
        ds.reference = reference
    return ds, ms


print('\n【学習データ抽出 (2015-2021)】', flush=True)
X_tr, y_win_tr, y_place_tr, y_rank_tr, meta_tr = collect_data(TRAIN_YEARS)
print(f'学習データ: {len(X_tr):,}頭  1着割合: {y_win_tr.mean()*100:.1f}%')

print('\n【バリデーションデータ抽出 (2022-2023)】', flush=True)
X_va, y_win_va, y_place_va, y_rank_va, meta_va = collect_data(VAL_YEARS)
print(f'バリデーション: {len(X_va):,}頭  1着割合: {y_win_va.mean()*100:.1f}%')

print('\n【テストデータ抽出 (2024-2025)】', flush=True)
X_te, y_win_te, y_place_te, y_rank_te, meta_te = collect_data(TEST_YEARS)
print(f'テストデータ: {len(X_te):,}頭  1着割合: {y_win_te.mean()*100:.1f}%')

# ── 特徴量名 ────────────────────────────────────
_base_names = (FACTOR_KEYS +
               ['n_field', 'dist_km', 'gate_num', 'n_races', 'course_code', 'cond_code',
                'prev_grade', 'best_grade', 'prev_rank', 'prev_field'] +
               EXTRA_NAMES)
_rel_z    = [f'z_{FACTOR_KEYS[i]}'   for i in REL_FEAT_IDX]
_rel_rank = [f'rr_{FACTOR_KEYS[i]}'  for i in REL_FEAT_IDX]
feat_names = _base_names + _rel_z + _rel_rank

print(f'\n特徴量次元数: {len(feat_names)}  ({len(_base_names)}ベース + {len(_rel_z)+len(_rel_rank)}相対)')

# ── Optuna ハイパーパラメータ最適化 ──────────────────────
N_TRIALS = 300
print(f'\n【Optuna ハイパーパラメータ探索 ({N_TRIALS} trials)】', flush=True)

dtrain_win_base = lgb.Dataset(X_tr, label=y_win_tr, feature_name=feat_names, free_raw_data=False)
dval_win_base   = lgb.Dataset(X_va, label=y_win_va, reference=dtrain_win_base, free_raw_data=False)


def objective(trial):
    params = {
        'objective':         'binary',
        'metric':            'auc',
        'verbose':           -1,
        'random_state':      42,
        'learning_rate':     trial.suggest_float('learning_rate', 0.02, 0.15, log=True),
        'num_leaves':        trial.suggest_int('num_leaves', 15, 127),
        'min_child_samples': trial.suggest_int('min_child_samples', 20, 200),
        'max_depth':         trial.suggest_int('max_depth', 4, 10),
        'subsample':         trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree':  trial.suggest_float('colsample_bytree', 0.4, 1.0),
        'reg_alpha':         trial.suggest_float('reg_alpha', 0.0, 10.0),
        'reg_lambda':        trial.suggest_float('reg_lambda', 0.0, 10.0),
    }
    model = lgb.train(
        params, dtrain_win_base,
        num_boost_round=1000,
        valid_sets=[dval_win_base],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)],
    )
    return model.best_score['valid_0']['auc']


sampler = optuna.samplers.TPESampler(seed=42)
study = optuna.create_study(direction='maximize', sampler=sampler)
study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=False)
best_params = study.best_params
best_auc    = study.best_value
print(f'  最良 val AUC (win): {best_auc:.4f}')
print(f'  最良パラメータ: {best_params}')

lgb_params = {
    'objective':    'binary',
    'metric':       'auc',
    'verbose':      -1,
    'random_state': 42,
    **best_params,
}

# ── 1着モデル ────────────────────────────────────
print('\n【LightGBM 学習: 1着予測 (最良パラメータ)】', flush=True)
dtrain_win = lgb.Dataset(X_tr, label=y_win_tr, feature_name=feat_names)
dval_win   = lgb.Dataset(X_va, label=y_win_va, reference=dtrain_win)
model_win  = lgb.train(
    lgb_params, dtrain_win,
    num_boost_round=2000,
    valid_sets=[dval_win],
    callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)],
)
print(f'  最良ラウンド: {model_win.best_iteration}  val AUC: {model_win.best_score["valid_0"]["auc"]:.4f}')

# ── 連対モデル ────────────────────────────────────
print('\n【LightGBM 学習: 連対予測 (最良パラメータ)】', flush=True)
dtrain_pl  = lgb.Dataset(X_tr, label=y_place_tr, feature_name=feat_names)
dval_pl    = lgb.Dataset(X_va, label=y_place_va, reference=dtrain_pl)
model_place = lgb.train(
    lgb_params, dtrain_pl,
    num_boost_round=2000,
    valid_sets=[dval_pl],
    callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)],
)
print(f'  最良ラウンド: {model_place.best_iteration}  val AUC: {model_place.best_score["valid_0"]["auc"]:.4f}')

# ── 精度評価 ──────────────────────────────────────
def eval_accuracy(X, meta, model_w, model_p, label):
    scores_w = model_w.predict(X)
    scores_p = model_p.predict(X)
    groups = defaultdict(list)
    for i, m in enumerate(meta):
        groups[m['race_id']].append({**m, 'sw': scores_w[i], 'sp': scores_p[i]})

    n = h_win = h_top2 = h_top3 = r_top2 = both_top2 = 0
    for rid, horses in groups.items():
        if len(horses) < 2: continue
        sorted_w = sorted(horses, key=lambda h: -h['sw'])
        sorted_p = sorted(horses, key=lambda h: -h['sp'])
        hn = sorted_w[0]
        rn = next((h for h in sorted_p if h['name'] != hn['name']), None)
        if rn is None: continue
        hr = hn['rank']; rr = rn['rank']
        n += 1
        if hr == 1:             h_win  += 1
        if hr <= 2:             h_top2 += 1
        if hr <= 3:             h_top3 += 1
        if rr <= 2:             r_top2 += 1
        if hr <= 2 and rr <= 2: both_top2 += 1

    print(f'\n{label} ({n}R):')
    print(f'  ◎1着率:      {h_win/n*100:5.1f}%')
    print(f'  ◎連対率:     {h_top2/n*100:5.1f}%')
    print(f'  ◎3着内率:    {h_top3/n*100:5.1f}%')
    print(f'  ○連対率:     {r_top2/n*100:5.1f}%')
    print(f'  ◎○両連対率: {both_top2/n*100:5.1f}%')
    return n, h_win, both_top2


print('\n' + '='*60)
print('=== モデル精度評価 ===')
print('='*60)

print('\n--- 学習期間 2015-2021 ---')
eval_accuracy(X_tr, meta_tr, model_win, model_place, '学習期間')

print('\n--- バリデーション 2022-2023 ---')
eval_accuracy(X_va, meta_va, model_win, model_place, 'バリデーション')

print('\n--- テスト期間 2024-2025 ---')
eval_accuracy(X_te, meta_te, model_win, model_place, 'テスト期間')

# ── 特徴量重要度 ──────────────────────────────────
print('\n' + '='*60)
print('=== 特徴量重要度 Top20 ===')
print('='*60)
imp = model_win.feature_importance(importance_type='gain')
imp_pairs = sorted(zip(feat_names, imp), key=lambda x: -x[1])
max_imp = max(imp) or 1
for fname, score in imp_pairs[:20]:
    bar = '#' * int(score / max_imp * 30)
    tag = ' *' if fname in EXTRA_NAMES else ''
    print(f'  {fname:<22} {score:8.0f}  {bar}{tag}')

print('\n=== ラップ特徴量の重要度 ===')
lap_feats = ['late_rank_avg', 'front_rank_avg', 'pace_gap_avg', 'pace_gap_std',
             'late_speed_trend', 'front_speed_avg']
lap_pairs = [(f, s) for f, s in zip(feat_names, imp) if f in lap_feats]
lap_pairs.sort(key=lambda x: -x[1])
for fname, score in lap_pairs:
    rank = next(i for i, (f, s) in enumerate(imp_pairs, 1) if f == fname)
    print(f'  {fname:<22} {score:8.0f}  (全体{rank}位)')

# ── モデル保存 ────────────────────────────────────
model_win.save_model('ml_model_win.txt')
model_win.save_model('ml_model_win_bin.txt')   # 後方互換性のため両方保存
model_place.save_model('ml_model_place.txt')
print('\nモデル保存完了: ml_model_win.txt / ml_model_win_bin.txt / ml_model_place.txt')
print(f'特徴量次元: {len(feat_names)}')

# ── 芝/ダート別モデル学習 ─────────────────────────────────
ENSEMBLE_SEEDS = [7, 123, 456]
print('\n【芝/ダート別モデル学習】', flush=True)

def train_course_model(course_tag, course_filter):
    print(f'\n  --- {course_tag} モデル ---', flush=True)
    print(f'  学習データ抽出中...', flush=True)
    Xc_tr, yw_tr, yp_tr, _, mc_tr = collect_data(TRAIN_YEARS, course_filter)
    Xc_va, yw_va, yp_va, _, mc_va = collect_data(VAL_YEARS,   course_filter)
    Xc_te, yw_te, yp_te, _, mc_te = collect_data(TEST_YEARS,  course_filter)
    print(f'  学習:{len(Xc_tr):,}頭  val:{len(Xc_va):,}頭  test:{len(Xc_te):,}頭')

    # Optuna (試行数を減らして高速化)
    N_COURSE_TRIALS = 100
    dtrc_w = lgb.Dataset(Xc_tr, label=yw_tr, feature_name=feat_names, free_raw_data=False)
    dvac_w = lgb.Dataset(Xc_va, label=yw_va, reference=dtrc_w, free_raw_data=False)

    def obj_c(trial):
        p = {
            'objective': 'binary', 'metric': 'auc', 'verbose': -1,
            'random_state': 42,
            'learning_rate':     trial.suggest_float('learning_rate', 0.02, 0.15, log=True),
            'num_leaves':        trial.suggest_int('num_leaves', 15, 127),
            'min_child_samples': trial.suggest_int('min_child_samples', 20, 200),
            'max_depth':         trial.suggest_int('max_depth', 4, 10),
            'subsample':         trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree':  trial.suggest_float('colsample_bytree', 0.4, 1.0),
            'reg_alpha':         trial.suggest_float('reg_alpha', 0.0, 10.0),
            'reg_lambda':        trial.suggest_float('reg_lambda', 0.0, 10.0),
        }
        m = lgb.train(p, dtrc_w, num_boost_round=1000, valid_sets=[dvac_w],
                      callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)])
        return m.best_score['valid_0']['auc']

    sampler_c = optuna.samplers.TPESampler(seed=42)
    study_c   = optuna.create_study(direction='maximize', sampler=sampler_c)
    study_c.optimize(obj_c, n_trials=N_COURSE_TRIALS, show_progress_bar=False)
    best_c = study_c.best_params
    print(f'  val AUC: {study_c.best_value:.4f}')

    params_c = {'objective': 'binary', 'metric': 'auc', 'verbose': -1,
                'random_state': 42, **best_c}

    # win モデル
    dtrc_w2 = lgb.Dataset(Xc_tr, label=yw_tr, feature_name=feat_names)
    dvac_w2 = lgb.Dataset(Xc_va, label=yw_va, reference=dtrc_w2)
    mw_c = lgb.train(params_c, dtrc_w2, num_boost_round=2000, valid_sets=[dvac_w2],
                     callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)])
    mw_c.save_model(f'ml_model_win_{course_tag}.txt')

    # place モデル
    dtrc_p = lgb.Dataset(Xc_tr, label=yp_tr, feature_name=feat_names)
    dvac_p = lgb.Dataset(Xc_va, label=yp_va, reference=dtrc_p)
    mp_c = lgb.train(params_c, dtrc_p, num_boost_round=2000, valid_sets=[dvac_p],
                     callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)])
    mp_c.save_model(f'ml_model_place_{course_tag}.txt')

    # テスト精度
    eval_accuracy(Xc_te, mc_te, mw_c, mp_c, f'{course_tag} テスト')

    # アンサンブル (train+val で3シード)
    Xc_trva = np.vstack([Xc_tr, Xc_va])
    yw_trva  = np.concatenate([yw_tr, yp_va if False else yw_va])
    yp_trva  = np.concatenate([yp_tr, yp_va])
    for i, seed in enumerate(ENSEMBLE_SEEDS):
        pe = {**params_c, 'random_state': seed}
        nw = mw_c.best_iteration + 100
        np_ = mp_c.best_iteration + 100
        dtw = lgb.Dataset(Xc_trva, label=yw_trva, feature_name=feat_names)
        mw_e = lgb.train(pe, dtw, num_boost_round=nw, callbacks=[lgb.log_evaluation(-1)])
        mw_e.save_model(f'ml_model_win_{course_tag}_e{i}.txt')
        dtp = lgb.Dataset(Xc_trva, label=yp_trva, feature_name=feat_names)
        mp_e = lgb.train(pe, dtp, num_boost_round=np_, callbacks=[lgb.log_evaluation(-1)])
        mp_e.save_model(f'ml_model_place_{course_tag}_e{i}.txt')
    print(f'  {course_tag} モデル保存完了 (base + e0〜e2)')

train_course_model('turf', 'turf')
train_course_model('dirt', 'dirt')
print('\n芝/ダート別モデル学習完了')

# ── LambdaRank 学習 (収益直接最適化) ─────────────────────
print('\n【LambdaRank 学習 (graded labels: 1着=3, 2着=2, 3着=1, else=0)】', flush=True)
dtrain_lam, _ = make_lambda_dataset(X_tr, y_rank_tr, meta_tr, feat_names)
dval_lam,   _ = make_lambda_dataset(X_va, y_rank_va, meta_va, feat_names)

lambda_params = {
    'objective':    'lambdarank',
    'metric':       'ndcg',
    'ndcg_eval_at': [1, 3],
    'label_gain':   [0, 1, 3, 7],   # label 0,1,2,3 の NDCG ゲイン
    'verbose':      -1,
    'random_state': 42,
    # LambdaRank 用に learning_rate を小さくして安定学習
    'learning_rate':     0.02,
    'num_leaves':        best_params['num_leaves'],
    'min_child_samples': best_params['min_child_samples'],
    'max_depth':         best_params['max_depth'],
    'subsample':         best_params['subsample'],
    'colsample_bytree':  best_params['colsample_bytree'],
    'reg_alpha':         best_params['reg_alpha'],
    'reg_lambda':        best_params['reg_lambda'],
}

# LambdaRank は early stopping せず固定ラウンドで学習
LAMBDA_ROUNDS = 800
model_lambda = lgb.train(
    lambda_params, dtrain_lam,
    num_boost_round=LAMBDA_ROUNDS,
    valid_sets=[dval_lam],
    callbacks=[lgb.log_evaluation(200)],
)
model_lambda.save_model('ml_model_win_lambda.txt')
lam_score = model_lambda.best_score.get('valid_0', {})
print(f'  ラウンド: {LAMBDA_ROUNDS}  '
      f'val NDCG@1: {lam_score.get("ndcg@1", 0):.4f}  '
      f'NDCG@3: {lam_score.get("ndcg@3", 0):.4f}')

# place モデルも同パラメータで学習 (graded top3 ランキング最適化)
dtrain_pl_lam, _ = make_lambda_dataset(X_tr, y_rank_tr, meta_tr, feat_names)
model_place_lam = lgb.train(
    lambda_params, dtrain_pl_lam,
    num_boost_round=LAMBDA_ROUNDS,
    callbacks=[lgb.log_evaluation(-1)],
)
model_place_lam.save_model('ml_model_place_lambda.txt')
print(f'  place_lambda ラウンド: {LAMBDA_ROUNDS}')

# LambdaRank モデルの精度評価
print('\n--- LambdaRank テスト期間 2024-2025 ---')
eval_accuracy(X_te, meta_te, model_lambda, model_place_lam, 'LambdaRank テスト期間')
print('\nLambdaRank 保存完了: ml_model_win_lambda.txt / ml_model_place_lambda.txt')

# ── アンサンブル学習 (シードを変えて3モデル追加) ──────────
print(f'\n【アンサンブル学習 ({len(ENSEMBLE_SEEDS)}モデル × 2)】', flush=True)

X_tr_va = np.vstack([X_tr, X_va])
y_win_tr_va   = np.concatenate([y_win_tr,   y_win_va])
y_place_tr_va = np.concatenate([y_place_tr, y_place_va])

for i, seed in enumerate(ENSEMBLE_SEEDS):
    params_e = {**lgb_params, 'random_state': seed}
    n_rounds_w = model_win.best_iteration   + 100
    n_rounds_p = model_place.best_iteration + 100

    # win モデル
    dtrain_w = lgb.Dataset(X_tr_va, label=y_win_tr_va,   feature_name=feat_names)
    m_w = lgb.train(params_e, dtrain_w,
                    num_boost_round=n_rounds_w,
                    callbacks=[lgb.log_evaluation(-1)])
    m_w.save_model(f'ml_model_win_e{i}.txt')

    # place モデル
    dtrain_p = lgb.Dataset(X_tr_va, label=y_place_tr_va, feature_name=feat_names)
    m_p = lgb.train(params_e, dtrain_p,
                    num_boost_round=n_rounds_p,
                    callbacks=[lgb.log_evaluation(-1)])
    m_p.save_model(f'ml_model_place_e{i}.txt')

    # テスト期間で確認
    sw = m_w.predict(X_te)
    sp = m_p.predict(X_te)
    from collections import defaultdict as _dd
    grps = _dd(list)
    for idx, m in enumerate(meta_te):
        grps[m['race_id']].append({**m, 'sw': sw[idx], 'sp': sp[idx]})
    n_t = h_win = 0
    for rid, horses in grps.items():
        if len(horses) < 2: continue
        sw_top = sorted(horses, key=lambda h: -h['sw'])
        sp_top = sorted(horses, key=lambda h: -h['sp'])
        hn = sw_top[0]; rn = next((h for h in sp_top if h['name'] != hn['name']), None)
        if rn is None: continue
        n_t += 1
        if hn['rank'] == 1: h_win += 1
    print(f'  seed={seed:>3}: win1着率={h_win/n_t*100:.1f}% ({n_t}R)  → ml_model_win_e{i}.txt')

print(f'\nアンサンブル完了: ml_model_win_e0〜e2 / ml_model_place_e0〜e2')

# ── LambdaRank アンサンブル ───────────────────────────
print(f'\n【LambdaRank アンサンブル ({len(ENSEMBLE_SEEDS)}モデル × 2)】', flush=True)
y_rank_tr_va = np.concatenate([y_rank_tr, y_rank_va])
meta_tr_va   = meta_tr + meta_va

for i, seed in enumerate(ENSEMBLE_SEEDS):
    params_lam_e = {**lambda_params, 'random_state': seed}
    n_rounds_lam = model_lambda.best_iteration + 100

    dtrain_lam_e, _ = make_lambda_dataset(X_tr_va, y_rank_tr_va, meta_tr_va, feat_names)
    m_lam = lgb.train(params_lam_e, dtrain_lam_e,
                      num_boost_round=LAMBDA_ROUNDS,
                      callbacks=[lgb.log_evaluation(-1)])
    m_lam.save_model(f'ml_model_win_lambda_e{i}.txt')

    dtrain_plam_e, _ = make_lambda_dataset(X_tr_va, y_rank_tr_va, meta_tr_va, feat_names)
    m_plam = lgb.train(params_lam_e, dtrain_plam_e,
                       num_boost_round=LAMBDA_ROUNDS,
                       callbacks=[lgb.log_evaluation(-1)])
    m_plam.save_model(f'ml_model_place_lambda_e{i}.txt')

    sw = m_lam.predict(X_te)
    grps_te = defaultdict(list)
    for idx, m in enumerate(meta_te):
        grps_te[m['race_id']].append({**m, 'sw': sw[idx]})
    n_t = h_win = 0
    for rid, horses in grps_te.items():
        if len(horses) < 2: continue
        sw_top = sorted(horses, key=lambda h: -h['sw'])
        n_t += 1
        if sw_top[0]['rank'] == 1: h_win += 1
    print(f'  seed={seed:>3}: win1着率={h_win/n_t*100:.1f}% ({n_t}R)  → ml_model_win_lambda_e{i}.txt')

print(f'\nLambdaRank アンサンブル完了: ml_model_win_lambda_e0〜e2 / ml_model_place_lambda_e0〜e2')

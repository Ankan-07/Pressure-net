import os
import sys
import time
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sklearn.metrics import (
    roc_auc_score, brier_score_loss, log_loss, accuracy_score,
)
from xgboost import XGBClassifier

from src.models.dataset import (
    load_features, match_split, to_flat, flat_feature_names,
)
from src.models.logistic import build_logistic_pipeline, top_coefficients


OUTPUT_MD = "baseline_results.md"
SEED = 42


def evaluate(name: str, y_true: np.ndarray, p_pred: np.ndarray) -> dict:
    auc = roc_auc_score(y_true, p_pred)
    brier = brier_score_loss(y_true, p_pred)
    ll = log_loss(y_true, p_pred, labels=[0, 1])
    acc = accuracy_score(y_true, (p_pred >= 0.5).astype(int))
    print(f"  {name:8s}  AUC={auc:.4f}  Brier={brier:.4f}  LogLoss={ll:.4f}  Acc={acc:.4f}")
    return {"auc": auc, "brier": brier, "log_loss": ll, "accuracy": acc, "n": int(len(y_true))}


def main():
    t0 = time.time()
    print("=" * 60)
    print("PressureNet — Week 3 baselines (LR + XGBoost)")
    print("=" * 60)

    df = load_features()
    train_df, val_df, test_df = match_split(df, test_size=0.15, val_size=0.15, seed=SEED)

    print(f"\nMatches:  train={train_df['match_id'].nunique()}  "
          f"val={val_df['match_id'].nunique()}  test={test_df['match_id'].nunique()}")
    print(f"Events:   train={len(train_df)}  val={len(val_df)}  test={len(test_df)}")
    print(f"Train pos rate: {train_df['label'].mean():.3f}")

    X_train, y_train = to_flat(train_df), train_df["label"].to_numpy()
    X_val, y_val = to_flat(val_df), val_df["label"].to_numpy()
    X_test, y_test = to_flat(test_df), test_df["label"].to_numpy()
    feat_names = flat_feature_names()

                                   
    print("\n[1/2] Training Logistic Regression...")
    t = time.time()
    lr = build_logistic_pipeline(C=1.0, seed=SEED)
    lr.fit(X_train, y_train)
    lr_train_time = time.time() - t
    print(f"      fit in {lr_train_time:.1f}s")

    lr_val_proba = lr.predict_proba(X_val)[:, 1]
    lr_test_proba = lr.predict_proba(X_test)[:, 1]
    print("  Validation:")
    lr_val = evaluate("LR", y_val, lr_val_proba)
    print("  Test:")
    lr_test = evaluate("LR", y_test, lr_test_proba)

    lr_top = top_coefficients(lr, feat_names, k=10)
    print("  Top-10 |coef|:")
    for name, c in lr_top:
        print(f"    {name:35s}  {c:+.4f}")

                       
    print("\n[2/2] Training XGBoost...")
    n_neg = int((y_train == 0).sum())
    n_pos = int((y_train == 1).sum())
    spw = n_neg / n_pos
    xgb = XGBClassifier(
        n_estimators=500,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        min_child_weight=5,
        reg_lambda=1.0,
        objective="binary:logistic",
        eval_metric="auc",
        scale_pos_weight=spw,
        tree_method="hist",
        n_jobs=-1,
        random_state=SEED,
        early_stopping_rounds=25,
    )
    t = time.time()
    xgb.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    xgb_train_time = time.time() - t
    print(f"      fit in {xgb_train_time:.1f}s  best_iter={xgb.best_iteration}")

    xgb_val_proba = xgb.predict_proba(X_val)[:, 1]
    xgb_test_proba = xgb.predict_proba(X_test)[:, 1]
    print("  Validation:")
    xgb_val = evaluate("XGB", y_val, xgb_val_proba)
    print("  Test:")
    xgb_test = evaluate("XGB", y_test, xgb_test_proba)

    importances = xgb.feature_importances_
    order = np.argsort(importances)[::-1][:10]
    xgb_top = [(feat_names[i], float(importances[i])) for i in order]
    print("  Top-10 gain importance:")
    for name, imp in xgb_top:
        print(f"    {name:35s}  {imp:.4f}")

                           
    LR_GATE, XGB_GATE = 0.63, 0.68
    lr_pass = lr_test["auc"] > LR_GATE
    xgb_pass = xgb_test["auc"] > XGB_GATE
    print("\nGate checks (on TEST):")
    print(f"  LR  AUC {lr_test['auc']:.4f} {'PASS' if lr_pass else 'FAIL'} (target > {LR_GATE})")
    print(f"  XGB AUC {xgb_test['auc']:.4f} {'PASS' if xgb_pass else 'FAIL'} (target > {XGB_GATE})")

                                         
    write_results_md(
        path=OUTPUT_MD,
        n_train=len(train_df), n_val=len(val_df), n_test=len(test_df),
        n_matches=(train_df["match_id"].nunique(),
                   val_df["match_id"].nunique(),
                   test_df["match_id"].nunique()),
        train_pos_rate=float(train_df["label"].mean()),
        lr_val=lr_val, lr_test=lr_test, lr_top=lr_top, lr_time=lr_train_time,
        xgb_val=xgb_val, xgb_test=xgb_test, xgb_top=xgb_top, xgb_time=xgb_train_time,
        xgb_best_iter=int(xgb.best_iteration),
        lr_pass=lr_pass, xgb_pass=xgb_pass,
    )
    print(f"\nWrote {OUTPUT_MD}")
    print(f"Total time: {time.time() - t0:.1f}s")


def write_results_md(path, n_train, n_val, n_test, n_matches, train_pos_rate,
                     lr_val, lr_test, lr_top, lr_time,
                     xgb_val, xgb_test, xgb_top, xgb_time, xgb_best_iter,
                     lr_pass, xgb_pass):
    m_train, m_val, m_test = n_matches

    def row(name, m):
        return f"| {name} | {m['auc']:.4f} | {m['brier']:.4f} | {m['log_loss']:.4f} | {m['accuracy']:.4f} | {m['n']} |"

    lines = []
    lines.append("# Week 3 — Baseline Results\n")
    lines.append("Linear and tree baselines on the flattened (N, 45) feature matrix. "
                 "These set the bar the LSTM and Transformer must clear to justify "
                 "their architectural complexity.\n")
    lines.append("## Data Split\n")
    lines.append("Split by `match_id` (not by event row) to prevent match-context leakage.\n")
    lines.append("| Split | Matches | Events |")
    lines.append("|-------|---------|--------|")
    lines.append(f"| Train | {m_train} | {n_train} |")
    lines.append(f"| Val   | {m_val} | {n_val} |")
    lines.append(f"| Test  | {m_test} | {n_test} |")
    lines.append(f"\nTrain positive (success) rate: **{train_pos_rate:.3f}**. "
                 f"Mild imbalance handled via `class_weight='balanced'` (LR) and "
                 f"`scale_pos_weight` (XGBoost).\n")

    lines.append("## Gate Results\n")
    lines.append("| Model | Test AUC | Target | Result |")
    lines.append("|-------|----------|--------|--------|")
    lines.append(f"| Logistic Regression | {lr_test['auc']:.4f} | > 0.63 | "
                 f"{'PASS' if lr_pass else 'FAIL'} |")
    lines.append(f"| XGBoost             | {xgb_test['auc']:.4f} | > 0.68 | "
                 f"{'PASS' if xgb_pass else 'FAIL'} |")
    lines.append("")

    lines.append("## Full Metrics\n")
    lines.append("### Validation\n")
    lines.append("| Model | AUC | Brier | Log Loss | Accuracy | N |")
    lines.append("|-------|-----|-------|----------|----------|---|")
    lines.append(row("Logistic Regression", lr_val))
    lines.append(row("XGBoost", xgb_val))
    lines.append("\n### Test\n")
    lines.append("| Model | AUC | Brier | Log Loss | Accuracy | N |")
    lines.append("|-------|-----|-------|----------|----------|---|")
    lines.append(row("Logistic Regression", lr_test))
    lines.append(row("XGBoost", xgb_test))
    lines.append("")

    lines.append("## Logistic Regression — Top Standardised Coefficients\n")
    lines.append("Coefficients are in standardised feature space (`StandardScaler` applied), "
                 "so magnitudes are directly comparable. Sign indicates direction: positive = "
                 "feature increases probability of success.\n")
    lines.append("| Feature | Coefficient |")
    lines.append("|---------|-------------|")
    for name, c in lr_top:
        lines.append(f"| `{name}` | {c:+.4f} |")
    lines.append(f"\nTrained in {lr_time:.1f}s.\n")

    lines.append("## XGBoost — Top Feature Importances (gain)\n")
    lines.append("| Feature | Importance |")
    lines.append("|---------|------------|")
    for name, imp in xgb_top:
        lines.append(f"| `{name}` | {imp:.4f} |")
    lines.append(f"\nTrained in {xgb_time:.1f}s, best iteration = {xgb_best_iter}.\n")

    lines.append("## Notes for the Deep Models\n")
    lines.append("- LSTM target: AUC > 0.67 (must beat LR by ≥0.04 to justify recurrence).")
    lines.append("- Transformer target: AUC > 0.70 (must beat XGBoost to justify self-attention).")
    lines.append("- Use the **identical** match-id split (`seed=42`, 70/15/15) — see "
                 "`src/models/dataset.py::match_split`.")
    lines.append("- If a deep model fails to beat XGBoost, the temporal/attention machinery "
                 "is not adding signal — investigate before tuning further.\n")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()

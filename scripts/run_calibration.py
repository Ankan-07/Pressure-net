from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import torch
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    accuracy_score, brier_score_loss, log_loss, roc_auc_score,
)
from xgboost import XGBClassifier

from src.models.calibrate import PlattCalibrator, probs_to_logits
from src.models.dataset import (
    PressureDataset, fit_scaler, load_features, match_split,
    pos_weight_from_labels, to_flat,
)
from src.models.logistic import build_logistic_pipeline
from src.models.lstm import PressureLSTM
from src.models.train import predict_logits
from src.models.transformer import PressureTransformer


SEED = 42
RESULTS_DIR = Path("results")
CALIB_DIR = RESULTS_DIR / "calibration"
EVAL_MD = RESULTS_DIR / "eval_results.md"
EVAL_JSON = RESULTS_DIR / "eval_metrics.json"
LSTM_CKPT = "data/checkpoints/lstm.pt"
TRANSFORMER_CKPT = "data/checkpoints/transformer.pt"


def metrics(probs: np.ndarray, labels: np.ndarray) -> dict:
    return {
        "auc": float(roc_auc_score(labels, probs)),
        "brier": float(brier_score_loss(labels, probs)),
        "log_loss": float(log_loss(labels, probs, labels=[0, 1])),
        "accuracy": float(accuracy_score(labels, (probs >= 0.5).astype(int))),
        "n": int(len(labels)),
    }


def fmt(name, m):
    return (f"  {name:18s}  AUC={m['auc']:.4f}  Brier={m['brier']:.4f}  "
            f"LogLoss={m['log_loss']:.4f}  Acc={m['accuracy']:.4f}")


def plot_calibration(name: str, raw: np.ndarray, cal: np.ndarray,
                     labels: np.ndarray, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5.2, 5.2))
    for probs, label, marker, color in [
        (raw, "raw", "o", "#888"),
        (cal, "Platt", "s", "#c0392b"),
    ]:
        frac, mean_pred = calibration_curve(labels, probs, n_bins=10, strategy="uniform")
        ax.plot(mean_pred, frac, marker=marker, color=color, label=label, linewidth=1.4)
    ax.plot([0, 1], [0, 1], "--", color="#333", linewidth=0.8, label="perfect")
    ax.set_xlabel("Predicted probability")
    ax.set_ylabel("Empirical frequency")
    ax.set_title(f"{name} — reliability (TEST, 10 bins)")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.legend(loc="upper left")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def deep_logits(model_cls, ckpt_path: str, val_ds, test_ds, forward_fn,
                **model_kwargs) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    model = model_cls(**model_kwargs)
    state = torch.load(ckpt_path, map_location="cpu", weights_only=True)
    model.load_state_dict(state["state_dict"])
    val_logits, val_labels = predict_logits(model, val_ds, forward_fn)
    test_logits, test_labels = predict_logits(model, test_ds, forward_fn)
    return val_logits, val_labels, test_logits, test_labels


def lstm_forward(m, b): return m(b["x"], b["num_real"])
def transformer_forward(m, b): return m(b["x"], b["pad_mask"])


def calibrate_one(name: str, val_scores: np.ndarray, val_labels: np.ndarray,
                  test_scores: np.ndarray, test_labels: np.ndarray,
                  score_type: str) -> dict:
    raw_probs = (test_scores if score_type == "prob"
                 else 1.0 / (1.0 + np.exp(-test_scores)))

    cal = PlattCalibrator().fit(val_scores, val_labels, score_type=score_type)
    cal_probs = cal.predict_proba(test_scores, score_type=score_type)

    raw_m = metrics(raw_probs, test_labels)
    cal_m = metrics(cal_probs, test_labels)

    print(fmt(f"{name} raw",   raw_m))
    print(fmt(f"{name} Platt", cal_m))

    CALIB_DIR.mkdir(parents=True, exist_ok=True)
    plot_calibration(name, raw_probs, cal_probs, test_labels,
                     CALIB_DIR / f"{name.lower().replace(' ', '_')}.png")
    with open(CALIB_DIR / f"{name.lower().replace(' ', '_')}.json", "w") as f:
        json.dump({"name": name, "platt": cal.to_dict(),
                   "raw": raw_m, "calibrated": cal_m,
                   "score_type": score_type}, f, indent=2)
    return {"name": name, "raw": raw_m, "calibrated": cal_m,
            "platt": cal.to_dict(), "score_type": score_type}


def main():
    t0 = time.time()
    print("=" * 64)
    print("PressureNet — Week 5 calibration (all four models)")
    print("=" * 64)

    df = load_features()
    train_df, val_df, test_df = match_split(df, seed=SEED)
    print(f"matches  train={train_df['match_id'].nunique()} "
          f"val={val_df['match_id'].nunique()} test={test_df['match_id'].nunique()}")
    print(f"events   train={len(train_df)} val={len(val_df)} test={len(test_df)}")

                                              
    X_train, y_train = to_flat(train_df), train_df["label"].to_numpy()
    X_val,   y_val   = to_flat(val_df),   val_df["label"].to_numpy()
    X_test,  y_test  = to_flat(test_df),  test_df["label"].to_numpy()

                                              
    scaler  = fit_scaler(train_df)
    val_ds  = PressureDataset(val_df, scaler)
    test_ds = PressureDataset(test_df, scaler)

    summary = []

                                      
    print("\n[1/4] Logistic Regression — refit + Platt")
    lr = build_logistic_pipeline(C=1.0, seed=SEED)
    lr.fit(X_train, y_train)
    lr_val_p  = lr.predict_proba(X_val)[:, 1]
    lr_test_p = lr.predict_proba(X_test)[:, 1]
    summary.append(calibrate_one(
        "LR", lr_val_p, y_val, lr_test_p, y_test, score_type="prob"
    ))

                          
    print("\n[2/4] XGBoost — refit + Platt")
    spw = float((y_train == 0).sum()) / max(int((y_train == 1).sum()), 1)
    xgb = XGBClassifier(
        n_estimators=500, max_depth=5, learning_rate=0.05,
        subsample=0.9, colsample_bytree=0.9, min_child_weight=5,
        reg_lambda=1.0, objective="binary:logistic", eval_metric="auc",
        scale_pos_weight=spw, tree_method="hist", n_jobs=-1,
        random_state=SEED, early_stopping_rounds=25,
    )
    xgb.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    xgb_val_p  = xgb.predict_proba(X_val)[:, 1]
    xgb_test_p = xgb.predict_proba(X_test)[:, 1]
    summary.append(calibrate_one(
        "XGBoost", xgb_val_p, y_val, xgb_test_p, y_test, score_type="prob"
    ))

                       
    print("\n[3/4] LSTM — load checkpoint + Platt")
    v_logits, v_labels, t_logits, t_labels = deep_logits(
        PressureLSTM, LSTM_CKPT, val_ds, test_ds, lstm_forward,
        n_features=15, proj_dim=32, hidden_size=64, dropout=0.2,
    )
    summary.append(calibrate_one(
        "LSTM", v_logits, v_labels, t_logits, t_labels, score_type="logit"
    ))

                              
    print("\n[4/4] Transformer — load checkpoint + Platt")
    v_logits, v_labels, t_logits, t_labels = deep_logits(
        PressureTransformer, TRANSFORMER_CKPT, val_ds, test_ds, transformer_forward,
        n_features=15, seq_len=3, d_model=64, nhead=4, dim_ff=128,
        num_layers=2, attn_dropout=0.1, head_dropout=0.2,
    )
    summary.append(calibrate_one(
        "Transformer", v_logits, v_labels, t_logits, t_labels, score_type="logit"
    ))

                                     
    EVAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(EVAL_JSON, "w") as f:
        json.dump({"summary": summary,
                   "split": {
                       "train_matches": train_df["match_id"].nunique(),
                       "val_matches": val_df["match_id"].nunique(),
                       "test_matches": test_df["match_id"].nunique(),
                       "train_events": int(len(train_df)),
                       "val_events": int(len(val_df)),
                       "test_events": int(len(test_df)),
                   }}, f, indent=2)
    write_md(summary, train_df, val_df, test_df)
    print(f"\nWrote {EVAL_MD}")
    print(f"Wrote {EVAL_JSON}")
    print(f"Total time: {time.time() - t0:.1f}s")


def write_md(summary: list[dict], train_df, val_df, test_df) -> None:
    def row_metrics(m):
        return (f"{m['auc']:.4f} | {m['brier']:.4f} | "
                f"{m['log_loss']:.4f} | {m['accuracy']:.4f}")

    lines = []
    lines.append("# PressureNet — Week 5 Evaluation & Calibration\n")
    lines.append("Platt scaling fit on the **val** split, evaluated on the **test** "
                 "split (held out from both base training and calibration). Reliability "
                 "diagrams in [`results/calibration/`](results/calibration).\n")

    lines.append("## Split (identical to Weeks 3 + 4)\n")
    lines.append("| Split | Matches | Events |")
    lines.append("|-------|---------|--------|")
    lines.append(f"| Train | {train_df['match_id'].nunique()} | {len(train_df)} |")
    lines.append(f"| Val   | {val_df['match_id'].nunique()} | {len(val_df)} |")
    lines.append(f"| Test  | {test_df['match_id'].nunique()} | {len(test_df)} |\n")

    lines.append("## Test Metrics — Raw vs Platt-Calibrated\n")
    lines.append("Ranking (AUC, accuracy) is unaffected by Platt scaling — it's a "
                 "monotonic transform. The interesting columns are **Brier** and "
                 "**Log Loss**: lower = better-calibrated probabilities.\n")
    lines.append("| Model | Variant | AUC | Brier | Log Loss | Accuracy |")
    lines.append("|-------|---------|----:|------:|---------:|---------:|")
    for s in summary:
        lines.append(f"| {s['name']} | raw   | {row_metrics(s['raw'])} |")
        lines.append(f"| {s['name']} | Platt | {row_metrics(s['calibrated'])} |")
    lines.append("")

    lines.append("## Brier Δ (raw − calibrated, positive = calibration helped)\n")
    lines.append("| Model | Raw Brier | Cal Brier | Δ |")
    lines.append("|-------|----------:|----------:|--:|")
    for s in summary:
        d = s["raw"]["brier"] - s["calibrated"]["brier"]
        sign = "+" if d >= 0 else "−"
        lines.append(f"| {s['name']} | {s['raw']['brier']:.4f} | "
                     f"{s['calibrated']['brier']:.4f} | {sign}{abs(d):.4f} |")
    lines.append("")

    lines.append("## Fitted Platt Parameters (a · logit + b)\n")
    lines.append("`a` close to 1.0 and `b` close to 0.0 means the raw scores were "
                 "already nearly calibrated; large deviations indicate the base "
                 "training distorted the probability scale.\n")
    lines.append("| Model | a | b | N (val) |")
    lines.append("|-------|--:|--:|--------:|")
    for s in summary:
        p = s["platt"]
        lines.append(f"| {s['name']} | {p['a']:.4f} | {p['b']:+.4f} | {p['n_fit']} |")
    lines.append("")

    lines.append("## Targets (from CLAUDE.md)\n")
    lines.append("| Model | AUC target | Brier target | Log-loss target |")
    lines.append("|-------|-----------:|-------------:|----------------:|")
    lines.append("| Logistic Regression | > 0.63 | < 0.24 | < 0.62 |")
    lines.append("| LSTM        | > 0.67 | < 0.23 | < 0.61 |")
    lines.append("| Transformer | > 0.70 | < 0.22 | < 0.60 |\n")

    lines.append("## Reliability Curves\n")
    for s in summary:
        slug = s["name"].lower().replace(" ", "_")
        lines.append(f"- **{s['name']}** — [{slug}.png](calibration/{slug}.png)")
    lines.append("")

    lines.append("## Reproduce\n")
    lines.append("```powershell")
    lines.append("uv run python scripts/run_calibration.py")
    lines.append("```\n")

    EVAL_MD.parent.mkdir(parents=True, exist_ok=True)
    with open(EVAL_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()

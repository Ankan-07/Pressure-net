"""
Week 5 ablation studies (from CLAUDE.md).

Four runs, each compared to the existing Week 4 baseline (results/{lstm,
transformer}.json). Goal: confirm that the design decisions in CLAUDE.md
are load-bearing — temporal context matters, hidden size 64 is well-
chosen, closing-speed features carry real signal.

| # | Variant                     | Tests                                                |
|---|-----------------------------|------------------------------------------------------|
| 1 | LSTM T=1                    | Sequence modelling is needed (not just t=0 features) |
| 2 | LSTM hidden=32              | Hidden=64 capacity is justified                      |
| 3 | Transformer T=1             | Self-attention benefits from multi-timestep context  |
| 4 | Transformer no-closing-speed| Features 3,4 (closing_speed_*) add real signal       |

Each ablation re-trains from scratch with the same split (seed=42),
same optimizer, same early-stopping policy, then evaluates on the test
split. We mutate the standardised dataset in-place AFTER fitting the
scaler on the unmodified train data — so the only thing that changes
is what the model can see during training and inference.

Run from project root:
    uv run python scripts/run_ablations.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.dataset import (
    PressureDataset, fit_scaler, load_features, match_split,
    pos_weight_from_labels,
)
from src.models.lstm import PressureLSTM
from src.models.train import (
    TrainConfig, compute_metrics, fit, predict_logits,
)
from src.models.transformer import PressureTransformer


SEED = 42
RESULTS_DIR = Path("results")
ABL_DIR = RESULTS_DIR / "ablations"
ABL_JSON = ABL_DIR / "summary.json"
BASELINE_LSTM = RESULTS_DIR / "lstm.json"
BASELINE_TRANS = RESULTS_DIR / "transformer.json"


def lstm_forward(m, b): return m(b["x"], b["num_real"])
def transformer_forward(m, b): return m(b["x"], b["pad_mask"])


# ---------------------------------------------------------------------------
# In-place dataset mutations
# ---------------------------------------------------------------------------
# Tensor layout (see src/models/dataset.py): X[:, t, f] with t=0 chronologically
# earliest (= t-2) and t=2 chronologically latest (= the action moment).
# masks[:, t] is True where that timestep is padded; num_real ∈ {1, 2, 3} counts
# real timesteps starting from the END of the window.

CLOSING_SPEED_IDX = [3, 4]   # closing_speed_nearest, closing_speed_2nd


def mutate_t1_only(ds: PressureDataset) -> None:
    """Keep only the t=0 (action-moment) row. Zero the past, mask it, num_real=1."""
    ds.X[:, [0, 1], :] = 0.0
    ds.masks[:, [0, 1]] = True
    ds.num_real[:] = 1


def mutate_drop_features(ds: PressureDataset, idxs: list[int]) -> None:
    """Zero the given feature columns at every timestep (post-standardisation)."""
    ds.X[:, :, idxs] = 0.0


# ---------------------------------------------------------------------------
# Per-ablation runner
# ---------------------------------------------------------------------------

def evaluate_split(model, ds, forward_fn) -> dict:
    logits, labels = predict_logits(model, ds, forward_fn)
    return compute_metrics(logits, labels)


def run_one(name: str, train_df, val_df, test_df, scaler,
            mutate_fn, model_factory, forward_fn,
            config_overrides: dict | None = None,
            checkpoint: str | None = None) -> dict:
    print("\n" + "=" * 64)
    print(f"Ablation: {name}")
    print("=" * 64)

    train_ds = PressureDataset(train_df, scaler)
    val_ds   = PressureDataset(val_df, scaler)
    test_ds  = PressureDataset(test_df, scaler)

    if mutate_fn is not None:
        # get_masks returns a non-writable numpy view of the parquet buffer;
        # we own the dataset now so promote to writable copies before mutating.
        for d in (train_ds, val_ds, test_ds):
            d.X = np.array(d.X, copy=True)
            d.masks = np.array(d.masks, copy=True)
            d.num_real = np.array(d.num_real, copy=True)
        mutate_fn(train_ds); mutate_fn(val_ds); mutate_fn(test_ds)

    model = model_factory()
    n_params = sum(p.numel() for p in model.parameters())

    config = TrainConfig(
        lr=1e-3, weight_decay=1e-2, batch_size=128,
        epochs=50, patience=7, t_max=50,
        pos_weight=pos_weight_from_labels(train_ds.labels),
        seed=SEED,
    )
    if config_overrides:
        for k, v in config_overrides.items():
            setattr(config, k, v)

    print(f"params={n_params:,}  config={config}")

    ckpt = checkpoint or f"data/checkpoints/ablation_{name.lower().replace(' ', '_')}.pt"
    train_info = fit(model, train_ds, val_ds, forward_fn, config, ckpt)
    val_m  = evaluate_split(model, val_ds,  forward_fn)
    test_m = evaluate_split(model, test_ds, forward_fn)

    print(f"\n  VAL   AUC={val_m['auc']:.4f}  Brier={val_m['brier']:.4f}  "
          f"LL={val_m['log_loss']:.4f}  Acc={val_m['accuracy']:.4f}")
    print(f"  TEST  AUC={test_m['auc']:.4f}  Brier={test_m['brier']:.4f}  "
          f"LL={test_m['log_loss']:.4f}  Acc={test_m['accuracy']:.4f}")

    # Strip history out of the config we serialise (already in train_info)
    cfg_serialisable = {k: v for k, v in asdict(config).items() if k != "history"}

    return {
        "name": name,
        "n_params": n_params,
        "best_epoch": train_info["best_epoch"],
        "best_val_loss": train_info["best_val_loss"],
        "val": val_m,
        "test": test_m,
        "config": cfg_serialisable,
        "checkpoint": ckpt,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def baseline_aucs() -> dict[str, dict]:
    with open(BASELINE_LSTM) as f: lstm_base = json.load(f)
    with open(BASELINE_TRANS) as f: trans_base = json.load(f)
    return {
        "LSTM (T=3, baseline)": {
            "n_params": lstm_base["n_params"],
            "val": lstm_base["val"], "test": lstm_base["test"],
            "best_epoch": lstm_base["best_epoch"],
        },
        "Transformer (T=3, baseline)": {
            "n_params": trans_base["n_params"],
            "val": trans_base["val"], "test": trans_base["test"],
            "best_epoch": trans_base["best_epoch"],
        },
    }


def main():
    t0 = time.time()
    print("PressureNet — Week 5 ablations")

    df = load_features()
    train_df, val_df, test_df = match_split(df, seed=SEED)
    scaler = fit_scaler(train_df)
    print(f"matches  train={train_df['match_id'].nunique()} "
          f"val={val_df['match_id'].nunique()} test={test_df['match_id'].nunique()}")
    print(f"events   train={len(train_df)} val={len(val_df)} test={len(test_df)}")

    runs: list[dict] = []

    # 1. LSTM T=1
    runs.append(run_one(
        "LSTM T=1",
        train_df, val_df, test_df, scaler,
        mutate_fn=mutate_t1_only,
        model_factory=lambda: PressureLSTM(
            n_features=15, proj_dim=32, hidden_size=64, dropout=0.2),
        forward_fn=lstm_forward,
    ))

    # 2. LSTM hidden=32 (T=3 retained)
    runs.append(run_one(
        "LSTM hidden=32",
        train_df, val_df, test_df, scaler,
        mutate_fn=None,
        model_factory=lambda: PressureLSTM(
            n_features=15, proj_dim=32, hidden_size=32, dropout=0.2),
        forward_fn=lstm_forward,
    ))

    # 3. Transformer T=1
    runs.append(run_one(
        "Transformer T=1",
        train_df, val_df, test_df, scaler,
        mutate_fn=mutate_t1_only,
        model_factory=lambda: PressureTransformer(
            n_features=15, seq_len=3, d_model=64, nhead=4, dim_ff=128,
            num_layers=2, attn_dropout=0.1, head_dropout=0.2),
        forward_fn=transformer_forward,
        # Match the Transformer baseline LR (3e-4) for a fair comparison
        config_overrides={"lr": 3e-4},
    ))

    # 4. Transformer without closing_speed features
    runs.append(run_one(
        "Transformer no-closing-speed",
        train_df, val_df, test_df, scaler,
        mutate_fn=lambda ds: mutate_drop_features(ds, CLOSING_SPEED_IDX),
        model_factory=lambda: PressureTransformer(
            n_features=15, seq_len=3, d_model=64, nhead=4, dim_ff=128,
            num_layers=2, attn_dropout=0.1, head_dropout=0.2),
        forward_fn=transformer_forward,
        config_overrides={"lr": 3e-4},
    ))

    baselines = baseline_aucs()

    ABL_DIR.mkdir(parents=True, exist_ok=True)
    with open(ABL_JSON, "w") as f:
        json.dump({"baselines": baselines, "ablations": runs}, f, indent=2)
    write_md(baselines, runs)

    print(f"\nWrote {ABL_JSON}")
    print(f"Total time: {(time.time() - t0)/60:.1f} min")


def write_md(baselines: dict, runs: list[dict]) -> None:
    md = RESULTS_DIR / "ablations.md"

    def fmt(m):
        return (f"{m['auc']:.4f} | {m['brier']:.4f} | "
                f"{m['log_loss']:.4f} | {m['accuracy']:.4f}")

    lines = []
    lines.append("# PressureNet — Week 5 Ablations\n")
    lines.append("Each ablation re-trains from scratch on the same match-id split "
                 "(`seed=42`, 70/15/15) with identical optimiser / early-stopping "
                 "policy. The deltas are vs the Week 4 baseline rows.\n")

    lines.append("## Baselines (from Week 4)\n")
    lines.append("| Model | Test AUC | Test Brier | Test LogLoss | Params |")
    lines.append("|-------|---------:|-----------:|-------------:|-------:|")
    for name, b in baselines.items():
        m = b["test"]
        lines.append(f"| {name} | {m['auc']:.4f} | {m['brier']:.4f} | "
                     f"{m['log_loss']:.4f} | {b['n_params']:,} |")
    lines.append("")

    lines.append("## Ablation Results (test split)\n")
    lines.append("| Ablation | AUC | Brier | LogLoss | Acc | Params | Best ep |")
    lines.append("|----------|----:|------:|--------:|----:|-------:|--------:|")
    for r in runs:
        lines.append(f"| {r['name']} | {fmt(r['test'])} | "
                     f"{r['n_params']:,} | {r['best_epoch']} |")
    lines.append("")

    lines.append("## Deltas vs Baseline (test AUC)\n")
    lines.append("Negative = ablation hurt; ~0 = the design decision is not load-bearing.\n")
    lines.append("| Ablation | Baseline AUC | Ablation AUC | Δ |")
    lines.append("|----------|-------------:|-------------:|--:|")
    base_lstm  = baselines["LSTM (T=3, baseline)"]["test"]["auc"]
    base_trans = baselines["Transformer (T=3, baseline)"]["test"]["auc"]
    name_to_base = {
        "LSTM T=1":                     base_lstm,
        "LSTM hidden=32":               base_lstm,
        "Transformer T=1":              base_trans,
        "Transformer no-closing-speed": base_trans,
    }
    for r in runs:
        base = name_to_base[r["name"]]
        d = r["test"]["auc"] - base
        sign = "+" if d >= 0 else "−"
        lines.append(f"| {r['name']} | {base:.4f} | {r['test']['auc']:.4f} | "
                     f"{sign}{abs(d):.4f} |")
    lines.append("")

    lines.append("## Reproduce\n")
    lines.append("```powershell")
    lines.append("uv run python scripts/run_ablations.py")
    lines.append("```\n")

    with open(md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Wrote {md}")


if __name__ == "__main__":
    main()

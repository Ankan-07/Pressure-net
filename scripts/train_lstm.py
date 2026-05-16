"""
Train the bidirectional LSTM baseline.

Uses the same match-id split (seed=42) as scripts/train_baselines.py so
metrics are directly comparable to LR and XGBoost.

Run from project root:
    uv run python scripts/train_lstm.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.dataset import (
    PressureDataset, fit_scaler, load_features, match_split, pos_weight_from_labels,
)
from src.models.lstm import PressureLSTM
from src.models.train import (
    TrainConfig, compute_metrics, fit, predict_logits,
)


CHECKPOINT = "data/checkpoints/lstm.pt"
RESULTS = "results/lstm.json"


def lstm_forward(model, batch):
    return model(batch["x"], batch["num_real"])


def main():
    t0 = time.time()
    print("=" * 60)
    print("PressureNet - LSTM training")
    print("=" * 60)

    df = load_features()
    train_df, val_df, test_df = match_split(df, seed=42)
    scaler = fit_scaler(train_df)
    train_ds = PressureDataset(train_df, scaler)
    val_ds = PressureDataset(val_df, scaler)
    test_ds = PressureDataset(test_df, scaler)

    print(f"matches  train={train_df['match_id'].nunique()} val={val_df['match_id'].nunique()} "
          f"test={test_df['match_id'].nunique()}")
    print(f"events   train={len(train_ds)} val={len(val_ds)} test={len(test_ds)}")

    model = PressureLSTM(n_features=15, proj_dim=32, hidden_size=64, dropout=0.2)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"model    PressureLSTM  params={n_params:,}")

    config = TrainConfig(
        lr=1e-3, weight_decay=1e-2, batch_size=128,
        epochs=50, patience=7, t_max=50,
        pos_weight=pos_weight_from_labels(train_ds.labels),
        seed=42,
    )
    print(f"config   {config}")

    print("\nTraining...")
    train_info = fit(model, train_ds, val_ds, lstm_forward, config, CHECKPOINT)

    print("\nFinal evaluation (best checkpoint):")
    val_logits, val_labels = predict_logits(model, val_ds, lstm_forward)
    test_logits, test_labels = predict_logits(model, test_ds, lstm_forward)
    val_metrics = compute_metrics(val_logits, val_labels)
    test_metrics = compute_metrics(test_logits, test_labels)

    def fmt(name, m):
        print(f"  {name:5s}  AUC={m['auc']:.4f}  Brier={m['brier']:.4f}  "
              f"LogLoss={m['log_loss']:.4f}  Acc={m['accuracy']:.4f}")
    fmt("VAL",  val_metrics)
    fmt("TEST", test_metrics)

    gate = 0.67
    passed = test_metrics["auc"] > gate
    print(f"\nGate: LSTM test AUC {test_metrics['auc']:.4f} "
          f"{'PASS' if passed else 'FAIL'} (target > {gate})")

    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    with open(RESULTS, "w") as f:
        json.dump({
            "model": "PressureLSTM",
            "n_params": n_params,
            "best_epoch": train_info["best_epoch"],
            "best_val_loss": train_info["best_val_loss"],
            "val": val_metrics,
            "test": test_metrics,
            "gate_target": gate, "gate_pass": passed,
            "config": config.__dict__,
            "history": train_info["history"],
        }, f, indent=2)
    print(f"\nWrote {RESULTS}")
    print(f"Total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()

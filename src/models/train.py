"""
Shared training utilities for the LSTM and Transformer.

Provides the training loop, evaluation function, early stopping, and
checkpoint handling. Per-model scripts in scripts/ supply only the model
instance, the forward adapter (each model has a different forward signature),
and the hyperparameter config.

Why the forward adapter
-----------------------
PressureLSTM expects (x, num_real); PressureTransformer expects (x, pad_mask).
Rather than make the training loop know about both, each script passes a tiny
callable `forward_fn(model, batch) -> logits` so train.py stays model-agnostic.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import (
    accuracy_score, brier_score_loss, log_loss, roc_auc_score,
)
from torch.utils.data import DataLoader, Dataset


ForwardFn = Callable[[nn.Module, dict], torch.Tensor]


@dataclass
class TrainConfig:
    lr: float = 1e-3
    weight_decay: float = 1e-2
    batch_size: int = 128
    epochs: int = 50
    patience: int = 7              # early stop on val loss
    t_max: int = 50                # CosineAnnealingLR T_max
    pos_weight: float = 1.0        # n_neg / n_pos from train labels
    grad_clip: float = 1.0
    seed: int = 42
    num_workers: int = 0
    history: list = field(default_factory=list)


def pick_device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def set_seed(seed: int) -> None:
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def _move_batch(batch: dict, device: torch.device) -> dict:
    return {k: v.to(device, non_blocking=True) for k, v in batch.items()}


def _epoch(model: nn.Module, loader: DataLoader, criterion: nn.Module,
           device: torch.device, forward_fn: ForwardFn,
           optimizer: torch.optim.Optimizer | None = None,
           grad_clip: float | None = None) -> tuple[float, np.ndarray, np.ndarray]:
    """Run a single epoch. If optimizer is None we are in eval mode."""
    train = optimizer is not None
    model.train(train)

    total_loss, total_n = 0.0, 0
    all_logits, all_labels = [], []

    ctx = torch.enable_grad() if train else torch.no_grad()
    with ctx:
        for batch in loader:
            batch = _move_batch(batch, device)
            logits = forward_fn(model, batch)
            loss = criterion(logits, batch["label"])

            if train:
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                if grad_clip is not None:
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                optimizer.step()

            n = batch["label"].size(0)
            total_loss += loss.item() * n
            total_n += n
            all_logits.append(logits.detach().cpu().numpy())
            all_labels.append(batch["label"].detach().cpu().numpy())

    return (
        total_loss / total_n,
        np.concatenate(all_logits),
        np.concatenate(all_labels),
    )


def compute_metrics(logits: np.ndarray, labels: np.ndarray) -> dict:
    probs = 1.0 / (1.0 + np.exp(-logits))
    return {
        "auc": float(roc_auc_score(labels, probs)),
        "brier": float(brier_score_loss(labels, probs)),
        "log_loss": float(log_loss(labels, probs, labels=[0, 1])),
        "accuracy": float(accuracy_score(labels, (probs >= 0.5).astype(int))),
        "n": int(len(labels)),
    }


def fit(model: nn.Module, train_ds: Dataset, val_ds: Dataset,
        forward_fn: ForwardFn, config: TrainConfig,
        checkpoint_path: str | Path) -> dict:
    """
    Train with early stopping on val loss. Saves best checkpoint to
    `checkpoint_path` and returns final history + best epoch metrics.
    """
    set_seed(config.seed)
    device = pick_device()
    print(f"[device] {device}")
    model.to(device)

    train_loader = DataLoader(
        train_ds, batch_size=config.batch_size, shuffle=True,
        num_workers=config.num_workers, drop_last=False,
    )
    val_loader = DataLoader(
        val_ds, batch_size=config.batch_size, shuffle=False,
        num_workers=config.num_workers,
    )

    criterion = nn.BCEWithLogitsLoss(
        pos_weight=torch.tensor(config.pos_weight, device=device)
    )
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=config.lr, weight_decay=config.weight_decay
    )
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=config.t_max)

    best_val_loss = float("inf")
    best_state: dict | None = None
    best_epoch = -1
    epochs_since_improve = 0

    ckpt_path = Path(checkpoint_path)
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)

    for epoch in range(1, config.epochs + 1):
        t = time.time()
        tr_loss, tr_logits, tr_labels = _epoch(
            model, train_loader, criterion, device, forward_fn,
            optimizer=optimizer, grad_clip=config.grad_clip,
        )
        val_loss, val_logits, val_labels = _epoch(
            model, val_loader, criterion, device, forward_fn
        )
        scheduler.step()

        tr_auc = roc_auc_score(tr_labels, 1.0 / (1.0 + np.exp(-tr_logits)))
        val_metrics = compute_metrics(val_logits, val_labels)
        dt = time.time() - t

        improved = val_loss < best_val_loss - 1e-6
        if improved:
            best_val_loss = val_loss
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            best_epoch = epoch
            epochs_since_improve = 0
        else:
            epochs_since_improve += 1

        config.history.append({
            "epoch": epoch,
            "train_loss": tr_loss, "train_auc": float(tr_auc),
            "val_loss": val_loss, **{f"val_{k}": v for k, v in val_metrics.items()},
            "lr": optimizer.param_groups[0]["lr"],
            "improved": improved,
        })

        print(f"  ep {epoch:3d}  tr_loss={tr_loss:.4f} tr_auc={tr_auc:.4f}  "
              f"val_loss={val_loss:.4f} val_auc={val_metrics['auc']:.4f}  "
              f"lr={optimizer.param_groups[0]['lr']:.2e}  {dt:.1f}s"
              f"{'  *' if improved else ''}")

        if epochs_since_improve >= config.patience:
            print(f"  early stop at epoch {epoch} (no improvement for {config.patience} epochs)")
            break

    assert best_state is not None, "training never produced a checkpoint"
    torch.save({"state_dict": best_state, "epoch": best_epoch}, ckpt_path)
    model.load_state_dict(best_state)
    print(f"  best epoch = {best_epoch}, val_loss = {best_val_loss:.4f}  ->  saved to {ckpt_path}")

    return {"best_epoch": best_epoch, "best_val_loss": float(best_val_loss),
            "history": config.history}


@torch.no_grad()
def predict_logits(model: nn.Module, ds: Dataset, forward_fn: ForwardFn,
                   batch_size: int = 256) -> tuple[np.ndarray, np.ndarray]:
    """Return (logits, labels) for a dataset using the current model weights."""
    device = pick_device()
    model.to(device).eval()
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False)
    logits, labels = [], []
    for batch in loader:
        batch = _move_batch(batch, device)
        logits.append(forward_fn(model, batch).cpu().numpy())
        labels.append(batch["label"].cpu().numpy())
    return np.concatenate(logits), np.concatenate(labels)

"""
Shared data loading + match-level splitting for PressureNet models.

All three baselines (LR, XGBoost) and the deep models (LSTM, Transformer)
consume the same (T=3, F=15) tensor. This module is the single entry point
so the split is identical across every model — critical for fair comparison.

Split is by match_id (NOT by event row) to prevent leakage: events from the
same match would otherwise leak match-specific context into validation/test.
"""
import numpy as np
import pandas as pd
import torch
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from torch.utils.data import Dataset

MASK_COLS = ["mask_t0", "mask_t1", "mask_t2"]


FEATURE_PATH = "data/features/features.parquet"
N_TIMESTEPS = 3
N_FEATURES = 15
N_FLAT = N_TIMESTEPS * N_FEATURES  # 45

FEATURE_NAMES = [
    "dist_to_nearest_defender",
    "dist_to_2nd_defender",
    "dist_to_3rd_defender",
    "closing_speed_nearest",
    "closing_speed_2nd",
    "ball_carrier_x",
    "ball_carrier_y",
    "voronoi_area",
    "pitch_zone",
    "time_since_last_touch",
    "n_defenders_within_5m",
    "pass_lane_density",
    "action_type",
    "body_orientation_sin",
    "body_orientation_cos",
]


def load_features(path: str = FEATURE_PATH) -> pd.DataFrame:
    df = pd.read_parquet(path)
    feat_cols = [c for c in df.columns if c.startswith("feat_")]
    assert len(feat_cols) == N_FLAT, f"expected {N_FLAT} feature cols, got {len(feat_cols)}"
    return df


def to_tensor_3d(df: pd.DataFrame) -> np.ndarray:
    """Return (N, T=3, F=15) float32 array ordered t0, t1, t2."""
    cols = [f"feat_t{t}_{f}" for t in range(N_TIMESTEPS) for f in range(N_FEATURES)]
    return df[cols].to_numpy(dtype=np.float32).reshape(-1, N_TIMESTEPS, N_FEATURES)


def to_flat(df: pd.DataFrame) -> np.ndarray:
    """Return (N, 45) float32 array — flatten timestep+feature for sklearn/xgb."""
    cols = [f"feat_t{t}_{f}" for t in range(N_TIMESTEPS) for f in range(N_FEATURES)]
    return df[cols].to_numpy(dtype=np.float32)


def flat_feature_names() -> list[str]:
    """Column names matching to_flat() order: t{0,1,2}_{feature_name}."""
    return [f"t{t}_{name}" for t in range(N_TIMESTEPS) for name in FEATURE_NAMES]


def match_split(df: pd.DataFrame, test_size: float = 0.15, val_size: float = 0.15,
                seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split events into train/val/test by match_id.

    Returns (train_df, val_df, test_df) as DataFrame views — no events from
    the same match appear in more than one split.
    """
    match_ids = np.array(df["match_id"].unique(), dtype=object)
    trainval_ids, test_ids = train_test_split(match_ids, test_size=test_size, random_state=seed)
    # val_size is relative to original — scale up for the trainval residual
    rel_val = val_size / (1.0 - test_size)
    train_ids, val_ids = train_test_split(trainval_ids, test_size=rel_val, random_state=seed)

    train_df = df[df["match_id"].isin(train_ids)].reset_index(drop=True)
    val_df = df[df["match_id"].isin(val_ids)].reset_index(drop=True)
    test_df = df[df["match_id"].isin(test_ids)].reset_index(drop=True)
    return train_df, val_df, test_df


# ---------------------------------------------------------------------------
# Deep-model helpers: standardisation + torch Dataset
# ---------------------------------------------------------------------------

def get_masks(df: pd.DataFrame) -> np.ndarray:
    """Return (N, T=3) bool array. True = that timestep is zero-padded."""
    return df[MASK_COLS].to_numpy(dtype=bool)


def fit_scaler(train_df: pd.DataFrame) -> StandardScaler:
    """
    Fit a per-feature StandardScaler on the TRAIN split only.

    Padded timesteps are excluded from the fit so their zeros do not bias
    the per-feature mean/std. Same scaler is later applied to val/test.
    """
    X3d = to_tensor_3d(train_df)              # (N, T, F)
    masks = get_masks(train_df)               # (N, T) — True = padded
    valid_rows = X3d[~masks]                  # (N_valid, F)
    return StandardScaler().fit(valid_rows)


def apply_scaler(scaler: StandardScaler, X3d: np.ndarray, masks: np.ndarray) -> np.ndarray:
    """Standardise (N, T, F) using fitted scaler; re-zero padded timesteps."""
    N, T, F = X3d.shape
    out = scaler.transform(X3d.reshape(-1, F)).reshape(N, T, F).astype(np.float32)
    out[masks] = 0.0    # keep padded positions as true zeros after scaling
    return out


class PressureDataset(Dataset):
    """
    Torch Dataset wrapping a pre-standardised (N, T=3, F=15) array.

    __getitem__ returns a dict so model code can pull only what it needs:
        x          (T, F) float32   — standardised features
        pad_mask   (T,)   bool      — True where padded (for Transformer key mask)
        num_real   ()     long      — number of real timesteps (1..3, for LSTM pack)
        label      ()     float32   — binary target
    """

    def __init__(self, df: pd.DataFrame, scaler: StandardScaler):
        X3d = to_tensor_3d(df)
        masks = get_masks(df)
        self.X = apply_scaler(scaler, X3d, masks)
        self.masks = masks
        self.labels = df["label"].to_numpy(dtype=np.float32)
        # num_real ∈ {1, 2, 3}; padding is contiguous from the start, so this is well-defined
        self.num_real = (~masks).sum(axis=1).astype(np.int64)

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, i: int) -> dict:
        return {
            "x": torch.from_numpy(self.X[i]),
            "pad_mask": torch.from_numpy(self.masks[i].copy()),
            "num_real": torch.tensor(self.num_real[i], dtype=torch.long),
            "label": torch.tensor(self.labels[i], dtype=torch.float32),
        }


def pos_weight_from_labels(labels: np.ndarray) -> float:
    """Compute n_neg/n_pos for BCEWithLogitsLoss(pos_weight=...) on a label array."""
    n_pos = int((labels == 1).sum())
    n_neg = int((labels == 0).sum())
    return n_neg / max(n_pos, 1)

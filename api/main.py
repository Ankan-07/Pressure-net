from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from src.models.calibrate import PlattCalibrator
from src.models.dataset import (
    FEATURE_NAMES, N_FEATURES, N_TIMESTEPS,
    PressureDataset, apply_scaler, fit_scaler, load_features, match_split,
)
from src.models.train import predict_logits
from src.models.transformer import PressureTransformer
from src.ranking.explainer import capture_cls_attention


MODEL_VERSION = "pressurenet-v1.0"
SEED = 42

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHECKPOINT = PROJECT_ROOT / "data" / "checkpoints" / "transformer.pt"
PLATT_JSON = PROJECT_ROOT / "results" / "calibration" / "transformer.json"
RATINGS_CSV = PROJECT_ROOT / "outputs" / "pressure_ratings.csv"
ATTN_NPZ = PROJECT_ROOT / "outputs" / "attention" / "all.npz"


class State:
    model: PressureTransformer
    scaler: object                                
    calibrator: PlattCalibrator
    ratings: pd.DataFrame
    attn_player_ids: np.ndarray
    attn_heatmaps: np.ndarray
    train_difficulties: np.ndarray                                            
    n_training_events: int


state = State()


def _load_state() -> None:
    print("[startup] loading features + recreating W4 split...")
    df = load_features(str(PROJECT_ROOT / "data" / "features" / "features.parquet"))
    train_df, _, _ = match_split(df, seed=SEED)
    state.scaler = fit_scaler(train_df)
    state.n_training_events = int(len(train_df))

    print("[startup] loading transformer checkpoint...")
    model = PressureTransformer(
        n_features=15, seq_len=3, d_model=64, nhead=4, dim_ff=128,
        num_layers=2, attn_dropout=0.1, head_dropout=0.2,
    )
    ckpt = torch.load(CHECKPOINT, map_location="cpu", weights_only=True)
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    state.model = model

    with open(PLATT_JSON) as f:
        platt = json.load(f)
    state.calibrator = PlattCalibrator.from_dict(platt["platt"])
    print(f"[startup] calibrator a={state.calibrator.a:.4f} b={state.calibrator.b:+.4f}")

    state.ratings = pd.read_csv(RATINGS_CSV)
    print(f"[startup] {len(state.ratings)} qualifying players in ratings")

    npz = np.load(ATTN_NPZ, allow_pickle=True)
    state.attn_player_ids = npz["player_ids"].astype(np.int64)
    state.attn_heatmaps = npz["heatmaps"].astype(np.float32)
    print(f"[startup] attention heatmaps: {state.attn_heatmaps.shape}")

                                                                          
    print("[startup] computing train-event difficulty distribution...")
    t = time.time()
    train_ds = PressureDataset(train_df, state.scaler)
    def fwd(m, b): return m(b["x"], b["pad_mask"])
    logits, _ = predict_logits(model, train_ds, fwd)
    probs = state.calibrator.predict_proba(logits, score_type="logit")
    state.train_difficulties = np.sort((1.0 - probs).astype(np.float32))
    print(f"[startup] train difficulty dist ready ({len(state.train_difficulties)} events, "
          f"{time.time() - t:.1f}s)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_state()
    yield


app = FastAPI(
    title="PressureNet API",
    version="1.0.0",
    description="Context-adjusted football performance scoring under pressure.",
    lifespan=lifespan,
)


@app.get("/api/v1/health")
def health() -> dict:
    return {
        "status": "ok",
        "model_version": MODEL_VERSION,
        "calibrated": True,
        "n_training_events": state.n_training_events,
    }


class EvaluateEventRequest(BaseModel):
    features: list[list[float]] = Field(
        ...,
        description="Three rows = [t-2, t-1, t=0]; each row = 15 features in index order",
    )


def _top_features(row: np.ndarray, k: int = 3) -> dict[str, float]:
    idx = np.argsort(-row)[:k]
    return {FEATURE_NAMES[i]: float(row[i]) for i in idx if row[i] > 0}


def _percentile_in_train(value: float, sorted_ref: np.ndarray) -> float:
    rank = float(np.searchsorted(sorted_ref, value, side="right"))
    return round(100.0 * rank / len(sorted_ref), 1)


@app.post("/api/v1/evaluate-event")
def evaluate_event(req: EvaluateEventRequest) -> dict:
    arr = np.asarray(req.features, dtype=np.float32)
    if arr.shape != (N_TIMESTEPS, N_FEATURES):
        raise HTTPException(
            status_code=422,
            detail=f"features must have shape ({N_TIMESTEPS}, {N_FEATURES}), got {arr.shape}",
        )

                                                                          
    arr_dataset_order = arr[::-1].copy()

                                                     
    pad_mask = np.zeros((1, N_TIMESTEPS), dtype=bool)
    x_std = apply_scaler(state.scaler, arr_dataset_order[None, ...], pad_mask)
    x = torch.from_numpy(x_std)
    m = torch.from_numpy(pad_mask)

                                              
    with capture_cls_attention(state.model, layer_idx=0) as captured:
        with torch.no_grad():
            logit = state.model(x, m).item()

    weights = captured[0]                                                  
    cls_row = weights[0, :, 0, 1:].mean(dim=0)                                            
    cls_row_np = cls_row.numpy()                                                   

                                                                                  
    x_abs = np.abs(x_std[0])                                              
    heat = cls_row_np[:, None] * x_abs                     

    probs = float(state.calibrator.predict_proba(np.array([logit]), score_type="logit")[0])
    difficulty = 1.0 - probs
    percentile = _percentile_in_train(difficulty, state.train_difficulties)

                                                        
    return {
        "success_probability": round(probs, 4),
        "difficulty_score": round(difficulty, 4),
        "percentile": percentile,
        "interpretation": (
            f"Top {round(100 - percentile, 1)}% hardest situations seen in training data"
            if percentile >= 50
            else f"Bottom {percentile}% of difficulty — soft pressing situation"
        ),
        "attention_weights": {
            "t0":  _top_features(heat[0]),                        
            "t-1": _top_features(heat[1]),
            "t-2": _top_features(heat[2]),
        },
    }


@app.get("/api/v1/player/{player_id}/rating")
def player_rating(
    player_id: int,
    competition: Optional[int] = Query(None),
    season: Optional[int] = Query(None),
) -> dict:
    df = state.ratings
    row = df[df["player_id"] == player_id]
    if competition is not None:
        row = row[row["competition_id"] == competition]
    if season is not None:
        row = row[row["season_id"] == season]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"player_id={player_id} not found")
    r = row.iloc[0]
    return {
        "player_id": int(r["player_id"]),
        "player_name": r["player_name"],
        "team": r["team_name"],
        "competition": r["competition_name"],
        "season": str(r["season_name"]),
        "pap_score": round(float(r["pap_score"]), 4),
        "pap_percentile": float(r["pap_percentile"]),
        "n_pressing_events": int(r["n_pressing_events"]),
        "success_rate": round(float(r["success_rate"]), 4),
        "expected_success": round(float(r["expected_success"]), 4),
        "avg_difficulty_faced": round(float(r["avg_difficulty"]), 4),
        "top_pressure_zone": r["top_pressure_zone"],
    }


@app.get("/api/v1/leaderboard")
def leaderboard(
    competition: Optional[int] = Query(None),
    season: Optional[int] = Query(None),
    min_events: int = Query(100, ge=1),
    top_n: int = Query(20, ge=1, le=500),
) -> dict:
    df = state.ratings
    sub = df[df["n_pressing_events"] >= min_events]
    if competition is not None:
        sub = sub[sub["competition_id"] == competition]
    if season is not None:
        sub = sub[sub["season_id"] == season]
    sub = sub.sort_values("pap_score", ascending=False).head(top_n).reset_index(drop=True)

    leaderboard_rows = []
    for i, r in sub.iterrows():
        leaderboard_rows.append({
            "rank": i + 1,
            "player_id": int(r["player_id"]),
            "player_name": r["player_name"],
            "team": r["team_name"],
            "pap_score": round(float(r["pap_score"]), 4),
            "pap_percentile": float(r["pap_percentile"]),
            "n_pressing_events": int(r["n_pressing_events"]),
        })

    total_pool = df[df["n_pressing_events"] >= min_events]
    if competition is not None:
        total_pool = total_pool[total_pool["competition_id"] == competition]
    if season is not None:
        total_pool = total_pool[total_pool["season_id"] == season]

    return {
        "leaderboard": leaderboard_rows,
        "total_qualifying_players": int(len(total_pool)),
        "competition": (sub["competition_name"].iloc[0] if not sub.empty else None),
        "season": (str(sub["season_name"].iloc[0]) if not sub.empty else None),
    }


@app.get("/api/v1/player/{player_id}/attention")
def player_attention(player_id: int) -> dict:
    idx = np.where(state.attn_player_ids == player_id)[0]
    if len(idx) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"no attention heatmap for player_id={player_id} (not in qualifying set)",
        )
    heat = state.attn_heatmaps[int(idx[0])]                 
    return {
        "player_id": player_id,
        "timesteps": ["t=0", "t-1", "t-2"],
        "feature_names": list(FEATURE_NAMES),
        "heatmap": heat.tolist(),
    }

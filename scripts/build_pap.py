from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.calibrate import PlattCalibrator
from src.models.dataset import (
    PressureDataset, fit_scaler, load_features, match_split,
)
from src.models.transformer import PressureTransformer
from src.ranking.explainer import aggregate_by_player, extract_attention_heatmaps
from src.ranking.pap_score import (
    attach_metadata, compute_pap_table,
    load_event_player_map, load_match_competition_map,
    QUALIFY_MIN_EVENTS,
)


SEED = 42
CHECKPOINT = "data/checkpoints/transformer.pt"
PLATT_JSON = "results/calibration/transformer.json"

OUT_DIR = Path("outputs")
CSV_OUT = OUT_DIR / "pressure_ratings.csv"
ATTN_OUT = OUT_DIR / "attention" / "all.npz"
SUMMARY_MD = Path("results") / "pap_summary.md"


def load_calibrator(path: str) -> PlattCalibrator:
    with open(path) as f:
        d = json.load(f)
    return PlattCalibrator.from_dict(d["platt"])


def write_summary(table: pd.DataFrame, n_total: int, n_qualified: int,
                  per_event_corr: float, label_rates: dict) -> None:
    top = table.head(20)
    bot = table.tail(20).iloc[::-1]                     

    def row(r):
        return (f"| {int(r['rank'])} | {r['player_name']} | {r['team_name']} | "
                f"{int(r['n_pressing_events'])} | "
                f"{r['pap_score']:+.4f} | {r['success_rate']:.3f} | "
                f"{r['expected_success']:.3f} | {r['avg_difficulty']:.3f} | "
                f"{r['top_pressure_zone']} |")

    header = (
        "| Rank | Player | Team | N | PAP | Actual | Expected | Difficulty | Top zone |\n"
        "|----:|--------|------|--:|----:|------:|---------:|-----------:|----------|"
    )

    lines = []
    lines.append("# PressureNet — Week 6 PAP Leaderboard\n")
    lines.append("PAP (Pressure-Adjusted Performance) is the mean per-event gap "
                 "between a player's actual outcome and the Platt-calibrated "
                 "Transformer probability that an *average* player would have "
                 "succeeded in the same pressing situation:\n")
    lines.append("> `PAP_p = mean_e (label_e - p_cal(e))`\n")
    lines.append("Positive PAP = outperformed expectation under pressure. Units "
                 "are probability points (e.g. +0.05 = 5 pp above baseline).\n")

    lines.append("## Methodology\n")
    lines.append(f"- Model: PressureTransformer (W4 checkpoint, Test AUC 0.8446)")
    lines.append(f"- Calibration: Platt scaling fit on val (W5)")
    lines.append(f"- Scope: all {n_total} pressing events with a player_id join")
    lines.append(f"- Qualifying: >= {QUALIFY_MIN_EVENTS} pressing events ({n_qualified} players qualify)")
    lines.append(f"- Sanity: Pearson(label, p_cal) across all events = {per_event_corr:.4f}\n")

    lines.append("## Label rate sanity check\n")
    lines.append("Calibrated probability buckets should track empirical success rate:\n")
    lines.append("| p_cal bucket | empirical success | n events |")
    lines.append("|--------------|------------------:|---------:|")
    for bucket, (rate, n) in label_rates.items():
        lines.append(f"| {bucket} | {rate:.3f} | {n} |")
    lines.append("")

    lines.append(f"## Top 20 (best PAP, n_events >= {QUALIFY_MIN_EVENTS})\n")
    lines.append(header)
    for _, r in top.iterrows():
        lines.append(row(r))
    lines.append("")

    lines.append(f"## Bottom 20 (worst PAP)\n")
    lines.append(header)
    for _, r in bot.iterrows():
        lines.append(row(r))
    lines.append("")

    lines.append("## Reproduce\n")
    lines.append("```powershell")
    lines.append("uv run python scripts/build_pap.py")
    lines.append("```\n")

    SUMMARY_MD.parent.mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    t0 = time.time()
    print("=" * 64)
    print("PressureNet — Week 6 PAP score + attention heatmaps")
    print("=" * 64)

                                       
    df = load_features()
    event_player = load_event_player_map()
    match_comp = load_match_competition_map()
    df = attach_metadata(df, event_player, match_comp)
    print(f"events with metadata: {len(df)}")
    print(f"unique players: {df['player_id'].nunique()}")
    print(f"competitions: {df['competition_name'].unique().tolist()}")

                                                                         
    train_df, val_df, test_df = match_split(df, seed=SEED)
    scaler = fit_scaler(train_df)
    all_ds = PressureDataset(df, scaler)
    print(f"split  train={len(train_df)} val={len(val_df)} test={len(test_df)}  all={len(df)}")

                                                
    model = PressureTransformer(
        n_features=15, seq_len=3, d_model=64, nhead=4, dim_ff=128,
        num_layers=2, attn_dropout=0.1, head_dropout=0.2,
    )
    state = torch.load(CHECKPOINT, map_location="cpu", weights_only=True)
    model.load_state_dict(state["state_dict"])
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device).eval()
    print(f"[device] {device}")

    calibrator = load_calibrator(PLATT_JSON)
    print(f"calibrator: a={calibrator.a:.4f}  b={calibrator.b:+.4f}  n_fit={calibrator.n_fit}")

                                                                        
    print("running inference + attention extraction...")
    t_inf = time.time()
    heatmaps = extract_attention_heatmaps(model, all_ds, batch_size=256)
    print(f"  attention heatmaps: shape={heatmaps.shape}  time={time.time() - t_inf:.1f}s")

                                                                    
    from src.models.train import predict_logits
    def transformer_forward(m, b): return m(b["x"], b["pad_mask"])
    logits, labels = predict_logits(model, all_ds, transformer_forward)
    assert len(logits) == len(df), f"{len(logits)} != {len(df)}"

                                           
    probs_cal = calibrator.predict_proba(logits, score_type="logit")

                                   
    per_event_corr = float(np.corrcoef(labels.astype(np.float64), probs_cal)[0, 1])
    print(f"  Pearson(label, p_cal) = {per_event_corr:.4f}")

                                 
    bucket_edges = [0.0, 0.2, 0.4, 0.6, 0.8, 1.001]
    bucket_labels = ["0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"]
    buckets = pd.cut(probs_cal, bins=bucket_edges, labels=bucket_labels, include_lowest=True)
    label_rates = {
        b: (float(df.loc[buckets == b, "label"].mean()) if (buckets == b).any() else float("nan"),
            int((buckets == b).sum()))
        for b in bucket_labels
    }

                            
    pap_df = compute_pap_table(df, probs_cal, min_events=QUALIFY_MIN_EVENTS)
    print(f"qualifying players (>= {QUALIFY_MIN_EVENTS} events): {len(pap_df)}")

                                                      
    player_ids = df["player_id"].to_numpy(dtype=np.int64)
    qualified_ids = set(pap_df["player_id"].astype(int).tolist())
    qualified_mask = np.array([pid in qualified_ids for pid in player_ids])
    unique_ids, per_player_heat = aggregate_by_player(
        heatmaps[qualified_mask], player_ids[qualified_mask]
    )
    print(f"attention heatmaps per qualifying player: shape={per_player_heat.shape}")

                                
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pap_df.to_csv(CSV_OUT, index=False, encoding="utf-8")
    print(f"wrote {CSV_OUT}")

    ATTN_OUT.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        ATTN_OUT,
        player_ids=unique_ids,
        heatmaps=per_player_heat,
        feature_names=np.array([
            "dist_to_nearest_defender", "dist_to_2nd_defender", "dist_to_3rd_defender",
            "closing_speed_nearest", "closing_speed_2nd",
            "ball_carrier_x", "ball_carrier_y", "voronoi_area",
            "pitch_zone", "time_since_last_touch", "n_defenders_within_5m",
            "pass_lane_density", "action_type",
            "body_orientation_sin", "body_orientation_cos",
        ]),
        timestep_names=np.array(["t=0", "t-1", "t-2"]),
    )
    print(f"wrote {ATTN_OUT}")

    write_summary(
        pap_df, n_total=len(df), n_qualified=len(pap_df),
        per_event_corr=per_event_corr, label_rates=label_rates,
    )
    print(f"wrote {SUMMARY_MD}")
    print(f"total time: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

QUALIFY_MIN_EVENTS = 100

ZONE_NAMES = {
    0: "own_box",
    1: "def_third",
    2: "left_half_space",
    3: "right_half_space",
    4: "final_third",
    5: "opp_box",
}


def load_event_player_map(events_parquet: str = "data/processed/events.parquet") -> pd.DataFrame:
    ev = pd.read_parquet(events_parquet, columns=["id", "player", "team"])
    ev = ev[ev["player"].notna()].copy()
    ev["player_id"] = ev["player"].map(lambda d: int(d["id"]))
    ev["player_name"] = ev["player"].map(lambda d: d.get("name"))
    ev["team_id"] = ev["team"].map(lambda d: int(d["id"]))
    ev["team_name"] = ev["team"].map(lambda d: d.get("name"))
    return ev.rename(columns={"id": "event_id"})[
        ["event_id", "player_id", "player_name", "team_id", "team_name"]
    ]


def load_match_competition_map(matches_glob: str = "data/raw/matches/*.json") -> pd.DataFrame:
    import glob
    rows = []
    for path in glob.glob(matches_glob):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for m in data:
            rows.append({
                "match_id": str(m["match_id"]),
                "competition_id": int(m["competition"]["competition_id"]),
                "season_id": int(m["season"]["season_id"]),
                "competition_name": m["competition"]["competition_name"],
                "season_name": m["season"]["season_name"],
            })
    return pd.DataFrame(rows).drop_duplicates(subset=["match_id"])


def attach_metadata(features_df: pd.DataFrame,
                    event_player: pd.DataFrame,
                    match_comp: pd.DataFrame) -> pd.DataFrame:
    out = features_df.merge(event_player, on="event_id", how="left")
    out = out.merge(match_comp, on="match_id", how="left")
    n_drop = out["player_id"].isna().sum()
    if n_drop:
        print(f"[pap] dropping {n_drop} events with no player_id join")
        out = out[out["player_id"].notna()].copy()
    out["player_id"] = out["player_id"].astype(int)
    return out


def compute_pap_table(events_df: pd.DataFrame, probs: np.ndarray,
                      min_events: int = QUALIFY_MIN_EVENTS) -> pd.DataFrame:
    df = events_df.copy()
    df["p_cal"] = probs.astype(np.float64)
    df["pap_event"] = df["label"].astype(np.float64) - df["p_cal"]

                                                                             
    def _mode_or_first(s: pd.Series):
        m = s.mode()
        return m.iloc[0] if len(m) else s.iloc[0]

    grouped = df.groupby("player_id", as_index=False).agg(
        player_name=("player_name", _mode_or_first),
        team_name=("team_name", _mode_or_first),
        competition_id=("competition_id", _mode_or_first),
        competition_name=("competition_name", _mode_or_first),
        season_id=("season_id", _mode_or_first),
        season_name=("season_name", _mode_or_first),
        n_pressing_events=("event_id", "count"),
        pap_score=("pap_event", "mean"),
        success_rate=("label", "mean"),
        expected_success=("p_cal", "mean"),
        top_pressure_zone_int=("feat_t0_8", _mode_or_first),
    )

    grouped["avg_difficulty"] = 1.0 - grouped["expected_success"]
    grouped["top_pressure_zone"] = grouped["top_pressure_zone_int"].astype(int).map(ZONE_NAMES)
    grouped = grouped.drop(columns=["top_pressure_zone_int"])

    qualified = grouped[grouped["n_pressing_events"] >= min_events].copy()
    qualified = qualified.sort_values("pap_score", ascending=False).reset_index(drop=True)
    qualified.insert(0, "rank", np.arange(1, len(qualified) + 1))
    qualified["pap_percentile"] = (
        qualified["pap_score"].rank(pct=True) * 100
    ).round(1)

    cols = [
        "rank", "player_id", "player_name", "team_name",
        "competition_name", "season_name",
        "competition_id", "season_id",
        "n_pressing_events", "pap_score", "pap_percentile",
        "success_rate", "expected_success", "avg_difficulty",
        "top_pressure_zone",
    ]
    return qualified[cols]

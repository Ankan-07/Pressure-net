from __future__ import annotations

import os
from typing import Optional

import numpy as np
import pandas as pd
import requests
import streamlit as st

API_BASE = os.environ.get("PRESSURENET_API", "http://127.0.0.1:8000/api/v1")

FEATURE_NAMES = [
    "dist_to_nearest_defender", "dist_to_2nd_defender", "dist_to_3rd_defender",
    "closing_speed_nearest", "closing_speed_2nd",
    "ball_carrier_x", "ball_carrier_y", "voronoi_area",
    "pitch_zone", "time_since_last_touch", "n_defenders_within_5m",
    "pass_lane_density", "action_type",
    "body_orientation_sin", "body_orientation_cos",
]
TIMESTEPS = ["t-2", "t-1", "t=0"]

COMPETITIONS = {
    "All": (None, None),
    "EURO 2024 (55/282)": (55, 282),
    "EURO 2020 (55/43)":  (55, 43),
    "World Cup 2022 (43/106)": (43, 106),
}


@st.cache_data(ttl=60)
def api_get(path: str, params: Optional[dict] = None) -> dict:
    r = requests.get(f"{API_BASE}{path}", params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def api_post(path: str, body: dict) -> dict:
    r = requests.post(f"{API_BASE}{path}", json=body, timeout=10)
    r.raise_for_status()
    return r.json()


def health_banner() -> None:
    try:
        h = api_get("/health")
        st.sidebar.success(
            f"API ok · {h['model_version']} · {h['n_training_events']:,} train events"
        )
    except Exception as e:
        st.sidebar.error(f"API unreachable: {e}\nTry: `uv run uvicorn api.main:app --port 8000`")


def page_leaderboard() -> None:
    st.title("PAP Leaderboard")
    st.write("Pressure-Adjusted Performance — how many percentage points "
             "above (or below) the average player each athlete scored, "
             "averaged over their pressing events.")

    cols = st.columns([2, 1, 1])
    comp_label = cols[0].selectbox("Competition", list(COMPETITIONS.keys()))
    min_events = cols[1].number_input("Min events", min_value=20, max_value=500, value=100, step=10)
    top_n = cols[2].number_input("Top N", min_value=5, max_value=200, value=25, step=5)

    comp_id, season_id = COMPETITIONS[comp_label]
    params = {"min_events": int(min_events), "top_n": int(top_n)}
    if comp_id is not None: params["competition"] = comp_id
    if season_id is not None: params["season"] = season_id

    data = api_get("/leaderboard", params=params)
    df = pd.DataFrame(data["leaderboard"])
    if df.empty:
        st.warning("No qualifying players for these filters.")
        return

    st.caption(f"{data['total_qualifying_players']} qualifying players in this pool")

    df_display = df.copy()
    df_display["pap_score"] = df_display["pap_score"].map(lambda x: f"{x:+.4f}")
    df_display["pap_percentile"] = df_display["pap_percentile"].map(lambda x: f"{x:.1f}")
    st.dataframe(
        df_display[["rank", "player_name", "team", "n_pressing_events",
                    "pap_score", "pap_percentile"]],
        use_container_width=True, hide_index=True,
    )

    st.bar_chart(df.set_index("player_name")["pap_score"], height=400)


def page_player_profile() -> None:
    st.title("Player Profile")

                                                                       
    pool = api_get("/leaderboard", params={"min_events": 100, "top_n": 500})
    options = pd.DataFrame(pool["leaderboard"])
    if options.empty:
        st.error("No qualifying players available.")
        return

    label_to_id = {f"{r['player_name']} ({r['team']})": int(r["player_id"])
                   for _, r in options.iterrows()}
    pick = st.selectbox("Player", list(label_to_id.keys()))
    pid = label_to_id[pick]

    rating = api_get(f"/player/{pid}/rating")

    c = st.columns(4)
    c[0].metric("PAP", f"{rating['pap_score']:+.4f}", f"P{rating['pap_percentile']:.0f}")
    c[1].metric("Pressing events", rating["n_pressing_events"])
    c[2].metric("Actual success", f"{rating['success_rate']:.1%}")
    c[3].metric("Expected", f"{rating['expected_success']:.1%}")

    st.write(f"**Team:** {rating['team']}  |  **Competition:** "
             f"{rating['competition']} {rating['season']}  |  "
             f"**Top pressure zone:** `{rating['top_pressure_zone']}`  |  "
             f"**Avg difficulty faced:** {rating['avg_difficulty_faced']:.3f}")

    st.subheader("Attention heatmap")
    st.caption("Which (timestep, feature) pairs the Transformer leans on when "
               "evaluating this player's pressing events. Higher = more "
               "weight in the model's decision.")
    try:
        att = api_get(f"/player/{pid}/attention")
        heat = np.array(att["heatmap"])                                                   
        heat_chrono = heat[::-1]                                     
        df_heat = pd.DataFrame(heat_chrono, index=TIMESTEPS, columns=att["feature_names"])
        st.dataframe(
            df_heat.style.format("{:.3f}").background_gradient(cmap="Reds", axis=None),
            use_container_width=True,
        )
    except requests.HTTPError as e:
        st.info(f"No attention heatmap for this player ({e}).")


def page_event_playground() -> None:
    st.title("Evaluate Single Event")
    st.write("Set the 15 features at each timestep, then evaluate. "
             "The model returns calibrated success probability + difficulty "
             "percentile vs the training distribution.")

    defaults = [
        [0.2,  0.4,  0.6,  1.2, 0.8, 0.55, 0.42, 18.5, 4, 1.2, 2, 0.30, 0, 0.50, 0.87],
        [0.3,  0.5,  0.7,  1.5, 0.9, 0.54, 0.43, 17.2, 4, 0.8, 2, 0.35, 0, 0.50, 0.87],
        [0.15, 0.35, 0.55, 2.1, 1.1, 0.53, 0.44, 14.1, 4, 0.3, 3, 0.40, 0, 0.50, 0.87],
    ]

    st.caption("Rows are chronological: t-2 (oldest), t-1, t=0 (now)")
    rows = []
    for i, ts in enumerate(TIMESTEPS):
        with st.expander(f"{ts} — features", expanded=(ts == "t=0")):
            cols = st.columns(5)
            row = []
            for j, fname in enumerate(FEATURE_NAMES):
                with cols[j % 5]:
                    val = st.number_input(
                        f"{fname}", key=f"{ts}-{fname}",
                        value=float(defaults[i][j]), format="%.3f",
                    )
                    row.append(val)
            rows.append(row)

    if st.button("Evaluate", type="primary"):
        result = api_post("/evaluate-event", {"features": rows})
        c = st.columns(3)
        c[0].metric("Success probability", f"{result['success_probability']:.3f}")
        c[1].metric("Difficulty", f"{result['difficulty_score']:.3f}")
        c[2].metric("Percentile (train)", f"{result['percentile']:.1f}")
        st.info(result["interpretation"])

        st.subheader("Top attended features per timestep")
        for ts_key, label in [("t-2", "t-2"), ("t-1", "t-1"), ("t0", "t=0")]:
            weights = result["attention_weights"].get(ts_key, {})
            if not weights:
                continue
            df = pd.DataFrame(
                sorted(weights.items(), key=lambda kv: -kv[1]),
                columns=["feature", "weight"],
            )
            with st.expander(f"{label} — top features", expanded=True):
                st.dataframe(df.style.format({"weight": "{:.3f}"}),
                             use_container_width=True, hide_index=True)


def page_methodology() -> None:
    st.title("Methodology")
    st.markdown(r"""
### What is PAP?

**Pressure-Adjusted Performance** measures how much better (or worse) a player
performs under pressure compared to what a model predicts for an average
player in the same situation:

$$
\mathrm{PAP}_p = \frac{1}{|E_p|}\sum_{e \in E_p}\bigl(\,\mathrm{label}_e - p_{\mathrm{cal}}(e)\,\bigr)
$$

- $\mathrm{label}_e \in \{0, 1\}$: did the player retain & progress the ball?
- $p_{\mathrm{cal}}(e)$: Platt-calibrated Transformer probability that an
  average player would have succeeded.
- Units are **probability points**. PAP = +0.05 means ~5 pp above baseline.

### Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Features | 15 per timestep, T=3 sequence | Spatial / pressure / temporal context |
| Model    | Transformer encoder (d=64, 2 layers) | Self-attention → interpretability |
| Calibration | Platt scaling (val-fit) | Raw logits are rankings, not probabilities |
| Split    | Match-level 70/15/15 | Prevents same-match leakage |

Test AUC 0.8446. Calibration buckets within ~3 pp of empirical success rate.

### Why an attention heatmap?

The Transformer's first-layer [CLS] attention tells us which timesteps it
weighted most. Multiplied by feature magnitudes, this yields a 3×15
fingerprint per player — a readable view of *what* drove their grade.
""")


def main() -> None:
    st.set_page_config(page_title="PressureNet", layout="wide")
    st.sidebar.title("PressureNet")
    page = st.sidebar.radio(
        "Page",
        ["Leaderboard", "Player Profile", "Evaluate Event", "Methodology"],
    )
    health_banner()

    if page == "Leaderboard":
        page_leaderboard()
    elif page == "Player Profile":
        page_player_profile()
    elif page == "Evaluate Event":
        page_event_playground()
    else:
        page_methodology()


if __name__ == "__main__":
    main()

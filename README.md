# PressureNet

**Context-adjusted football performance metric powered by StatsBomb 360 freeze-frame data.**

PressureNet learns how hard a given on-ball moment is — given the defender geometry, closing speeds, and field position around the ball-carrier — and then scores each player on how much better or worse they performed compared to the model's expectation for an average player in the same situation. The result is the **PAP (Pressure-Adjusted Performance) score**.

Three model architectures are trained and benchmarked on the same temporal feature tensor of shape `(T=3, F=15)`:

| Model | Test ROC-AUC | Params | Role |
|---|---|---|---|
| Logistic Regression | 0.7966 | 46 | Linear baseline — sanity-checks feature engineering |
| XGBoost | 0.8475 | — | Tabular gradient-boosted baseline |
| Bi-LSTM | 0.8447 | 50,817 | Sequential recurrent model |
| Transformer Encoder | 0.8446 | 68,353 | Self-attention — used for explainability |

The Transformer is retained because its attention weights produce a per-player 3×15 pressure profile, even though XGBoost ties it on raw AUC.

---

## Table of Contents

1. [Project Goals](#project-goals)
2. [Architecture Overview](#architecture-overview)
3. [Data](#data)
4. [Feature Engineering](#feature-engineering)
5. [Model Architectures](#model-architectures)
6. [Installation](#installation)
7. [Usage](#usage)
8. [Repository Layout](#repository-layout)
9. [Evaluation & Results](#evaluation--results)
10. [API Documentation](#api-documentation)
11. [Dashboard](#dashboard)
12. [Roadmap](#roadmap)

---

## Project Goals

Traditional football statistics (pass completion %, dribble success %, xG) treat all actions as equal. A pass completed under no pressure in the center circle is counted the same as a pass completed with two defenders closing at 4 m/s in the final third. PressureNet corrects this:

1. **Quantify situational difficulty.** Train a model to predict P(success | pressure context) for every on-ball event.
2. **Compute PAP per player.** PAP = mean(actual_outcome − model_expected_outcome) across that player's pressing events.
3. **Explain *why*.** Use Transformer attention to produce per-player pressure profiles — does player X struggle against fast closers? Tight pre-reception space? Specific pitch zones?
4. **Serve it.** A FastAPI service exposes evaluation, player ratings, and leaderboards. A Streamlit dashboard wraps the API for non-technical users.

---

## Architecture Overview

```
StatsBomb GitHub (events + 360 + lineups)
        │
        ▼
src/ingestion/downloader.py  ──►  data/raw/ (JSON)
        │
        ▼
src/ingestion/loader.py      ──►  data/processed/{events,frames,lineups}.parquet
        │
        ▼
src/transformation/engineer.py
  - filter_pressing_events()
  - assign_labels()
  - build_temporal_window()  ──►  data/features/features.parquet  (N × 3 × 15)
        │
        ▼
src/models/{logistic,lstm,transformer}.py   ──►  data/checkpoints/*.pt
src/models/calibrate.py (Platt scaling)
        │
        ▼
src/ranking/pap_score.py     ──►  outputs/pressure_ratings.csv
src/ranking/explainer.py     ──►  outputs/attention/{player_id}.npy
        │
        ▼
api/main.py (FastAPI)  ←──────►  dashboard/app.py (Streamlit)
```

---

## Data

**Source:** [StatsBomb Open Data](https://github.com/statsbomb/open-data) — free, no API key required.

**Competitions used:**

| Competition | `competition_id` | `season_id` |
|---|---|---|
| EURO 2024 | 55 | 282 |
| EURO 2020 | 55 | 43 |
| World Cup 2022 | 43 | 106 |

**Volumes:**

- ~615,202 total events across competitions
- ~59,719 pressing events (Pass/Carry/Dribble/Shot under pressure with freeze frame)
- Class balance: ~62% success / ~38% failure (mild imbalance; `pos_weight` used in BCE loss)
- Match-id split: 116 train / 25 val / 25 test matches → 41,606 / 8,982 / 9,131 events

**Critical rule:** the train/val/test split is by `match_id` (seed=42), never by event row. Splitting by event leaks match context into the test set and inflates AUC by ~0.04–0.06.

---

## Feature Engineering

Each pressing event becomes a tensor of shape `(T=3, F=15)` — three timesteps (t-2, t-1, t=0) of fifteen features each. Missing predecessor events at match start are zero-padded with a boolean mask.

| # | Feature | Description |
|---|---|---|
| 0 | `dist_to_nearest_defender` | Euclidean distance to nearest opponent (m) |
| 1 | `dist_to_2nd_defender` | Distance to 2nd nearest opponent |
| 2 | `dist_to_3rd_defender` | Distance to 3rd nearest opponent |
| 3 | `closing_speed_nearest` | Δdistance / Δt for nearest defender (m/s) |
| 4 | `closing_speed_2nd` | Same for 2nd nearest |
| 5 | `ball_carrier_x` | Normalised x (0–1) |
| 6 | `ball_carrier_y` | Normalised y (0–1) |
| 7 | `voronoi_area` | Ball-carrier's Voronoi cell area (`scipy.spatial.Voronoi`) |
| 8 | `pitch_zone` | 0–5 zone encoding (own box → opp box) |
| 9 | `time_since_last_touch` | Seconds since previous on-ball event |
| 10 | `n_defenders_within_5m` | Count of opponents within 5 m |
| 11 | `pass_lane_density` | Mean opponent density in top-3 passing-angle bins |
| 12 | `action_type` | pass=0 / dribble=1 / carry=2 / shot=3 / cross=4 |
| 13 | `body_orientation_sin` | sin(orientation) |
| 14 | `body_orientation_cos` | cos(orientation) |

**Label.** `label=1` if the ball-carrier (a) retained possession AND (b) the action was progressive — completed pass, completed dribble, shot attempted, or carry advanced ≥ 5 m toward the opposition goal.

---

## Model Architectures

### Logistic Regression — `src/models/logistic.py`
Flatten (3, 15) → 45 features → linear → sigmoid. Tests whether features alone are predictive.

### Bidirectional LSTM — `src/models/lstm.py`
```
Linear(15 → 32) → BiLSTM(hidden=64, layers=1) → concat last hidden states (128)
                → Dropout(0.2) → Linear(128 → 1)
```
Optimizer: AdamW(lr=1e-3, wd=1e-2). Scheduler: CosineAnnealingLR(T_max=50). Batch 128, early stop patience 7.

### Transformer Encoder — `src/models/transformer.py`
```
Linear(15 → 64) → prepend learnable CLS → add learned positional embedding (1, 4, 64)
                → TransformerEncoder(layers=2, heads=4, ff=128, dropout=0.1, GELU)
                → extract CLS → Dropout(0.2) → Linear(64 → 1)
```
Optimizer: AdamW(lr=3e-4, wd=1e-2). Same scheduler / batch / patience as LSTM.

Loss for both deep models: `BCEWithLogitsLoss(pos_weight=n_neg/n_pos)`.

---

## Installation

PressureNet uses **Python 3.11** and the [`uv`](https://github.com/astral-sh/uv) package manager.

```bash
# Clone
git clone https://github.com/<your-user>/pressurenet.git
cd pressurenet

# Install dependencies (creates .venv automatically)
uv sync

# Activate (optional — uv run handles this implicitly)
.venv\Scripts\activate     # Windows
source .venv/bin/activate  # macOS / Linux
```

Do **not** use `pip install -r requirements.txt` for development — `uv sync` is authoritative. Use `uv add <package>` to add a new dependency.

---

## Usage

### 1. Download raw StatsBomb data

```bash
uv run python -m src.ingestion.downloader
```

Pulls events, 360 freeze frames, and lineups for the three configured competitions into `data/raw/`. Existing files are skipped — `data/raw/` is intentionally immutable.

### 2. Load JSON → Parquet

```bash
uv run python -m src.ingestion.loader
```

Produces `data/processed/events.parquet`, `frames.parquet`, `lineups.parquet`.

### 3. Build features

```bash
uv run python scripts/build_features.py
```

Runs the full pipeline (filter pressing events → assign labels → build temporal windows) and writes `data/features/features.parquet`.

### 4. Train models

```bash
uv run python scripts/train_baselines.py     # LR + XGBoost
uv run python scripts/train_lstm.py          # BiLSTM
uv run python scripts/train_transformer.py   # Transformer
```

Checkpoints land in `data/checkpoints/`; per-model metrics in `results/*.json`.

### 5. Compute PAP scores (Week 6 — pending)

```bash
uv run python scripts/build_pap.py
```

Writes `outputs/pressure_ratings.csv` with one row per qualifying player (≥100 pressing events).

### 6. Run the API and dashboard

```bash
uv run uvicorn api.main:app --reload --port 8000
uv run streamlit run dashboard/app.py --server.port 8501
```

---

## Repository Layout

```
pressurenet/
├── api/                    # FastAPI service
│   └── main.py
├── dashboard/              # Streamlit dashboard
│   └── app.py
├── data/
│   ├── raw/                # Immutable StatsBomb JSON
│   ├── processed/          # events.parquet, frames.parquet, lineups.parquet
│   ├── features/           # features.parquet (N × 3 × 15)
│   └── checkpoints/        # lstm.pt, transformer.pt
├── outputs/
│   ├── pressure_ratings.csv
│   └── attention/{player_id}.npy
├── results/                # model_results.md + per-model JSONs
├── scripts/
│   ├── build_features.py
│   ├── train_baselines.py
│   ├── train_lstm.py
│   ├── train_transformer.py
│   └── build_pap.py
├── src/
│   ├── ingestion/          # downloader.py, loader.py
│   ├── transformation/     # engineer.py
│   ├── models/             # logistic.py, lstm.py, transformer.py, dataset.py, train.py, calibrate.py
│   ├── ranking/            # pap_score.py, explainer.py
│   ├── evaluation/         # metrics.py
│   ├── viz/                # pitch_plots.py, attention_viz.py
│   ├── logger.py
│   └── exception.py
├── CLAUDE.md               # LLM development guide
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## Evaluation & Results

Full write-up: [`results/model_results.md`](results/model_results.md).

| Metric | LR | XGBoost | LSTM | Transformer | Target (Transformer) |
|---|---|---|---|---|---|
| ROC-AUC | 0.7966 | 0.8475 | 0.8447 | 0.8446 | > 0.70 ✅ |
| Brier Score | — | — | — | — | < 0.22 |
| Log Loss | — | — | — | — | < 0.60 |

Val ↔ test AUC drift ≤ 0.007 across all four models — no overfitting detected.

**Planned ablations (Week 5):** T=1 vs T=3, hidden=32 vs 64 (LSTM), d_model=128 vs 64 (Transformer), removing closing-speed features, sinusoidal vs learned PE.

---

## API Documentation

Base URL (local dev): `http://localhost:8000`

All endpoints return JSON. Errors follow the FastAPI default `{"detail": "..."}` shape with appropriate 4xx/5xx status codes.

### `GET /api/v1/health`

Liveness probe — useful for Docker healthchecks and uptime monitoring.

**Response 200**
```json
{
  "status": "ok",
  "model_version": "pressurenet-v1.0",
  "calibrated": true,
  "n_training_events": 52847
}
```

---

### `POST /api/v1/evaluate-event`

Score a single pressing situation. Returns the model's predicted difficulty (1 − P(success)), its percentile against the training distribution, and the Transformer's attention weights for explainability.

**Request body**

`features` must be a 3 × 15 array — rows ordered `[t-2, t-1, t=0]`, columns in the index order from the [Feature Engineering](#feature-engineering) table.

```json
{
  "features": [
    [0.20, 0.40, 0.60, 1.2, 0.8, 0.55, 0.42, 18.5, 4, 1.2, 2, 0.30, 0, 0.50, 0.87],
    [0.30, 0.50, 0.70, 1.5, 0.9, 0.54, 0.43, 17.2, 4, 0.8, 2, 0.35, 0, 0.50, 0.87],
    [0.15, 0.35, 0.55, 2.1, 1.1, 0.53, 0.44, 14.1, 4, 0.3, 3, 0.40, 0, 0.50, 0.87]
  ]
}
```

**Response 200**
```json
{
  "difficulty_score": 0.31,
  "percentile": 18,
  "interpretation": "Top 18% hardest situations seen in training data",
  "attention_weights": {
    "t0":  {"dist_to_nearest_defender": 0.41, "closing_speed_nearest": 0.28, "voronoi_area": 0.12},
    "t-1": {"dist_to_nearest_defender": 0.18, "closing_speed_nearest": 0.15, "voronoi_area": 0.09},
    "t-2": {"dist_to_nearest_defender": 0.09, "closing_speed_nearest": 0.08, "voronoi_area": 0.05}
  }
}
```

**Errors**
- `422 Unprocessable Entity` — `features` is not 3 × 15 or contains non-numeric values.

---

### `GET /api/v1/player/{player_id}/rating`

Retrieve the aggregated PAP score for one player in one competition + season.

**Path parameters**
- `player_id` *(int, required)* — StatsBomb player ID.

**Query parameters**
- `competition` *(int, required)* — StatsBomb `competition_id` (e.g. `55` for EURO).
- `season` *(int, required)* — StatsBomb `season_id` (e.g. `282` for EURO 2024).

**Example**
```
GET /api/v1/player/3009/rating?competition=55&season=282
```

**Response 200**
```json
{
  "player_name": "Kylian Mbappé",
  "team": "France",
  "competition": "EURO 2024",
  "season": "2024",
  "pap_score": 0.087,
  "pap_percentile": 91,
  "n_pressing_events": 247,
  "avg_difficulty_faced": 0.51,
  "top_pressure_zone": "final_third"
}
```

**Errors**
- `404 Not Found` — player did not feature in the requested competition/season, or has < 100 qualifying pressing events.

---

### `GET /api/v1/leaderboard`

Ranked PAP leaderboard for a competition + season.

**Query parameters**
- `competition` *(int, required)*
- `season` *(int, required)*
- `min_events` *(int, optional, default `100`)* — minimum qualifying pressing events.
- `top_n` *(int, optional, default `20`)* — number of players to return.

**Example**
```
GET /api/v1/leaderboard?competition=55&season=282&min_events=100&top_n=20
```

**Response 200**
```json
{
  "leaderboard": [
    {
      "rank": 1,
      "player_id": 3089,
      "player_name": "...",
      "team": "...",
      "pap_score": 0.112,
      "pap_percentile": 99,
      "n_pressing_events": 312
    }
  ],
  "total_qualifying_players": 184,
  "competition": "EURO 2024",
  "season": "2024"
}
```

---

### Interactive Docs

FastAPI auto-generates two browsable docs UIs:

- Swagger UI — `http://localhost:8000/docs`
- ReDoc — `http://localhost:8000/redoc`

---

## Dashboard

The Streamlit dashboard (`dashboard/app.py`) wraps the API and offers four pages:

1. **Leaderboard** — sortable PAP rankings per competition/season.
2. **Player profile** — PAP score, 3×15 attention heatmap, pitch-zone breakdown.
3. **Event evaluator** — interactive freeze-frame input → difficulty score.
4. **Model card** — metric summary, calibration curve, ablation table.

Run with:
```bash
uv run streamlit run dashboard/app.py
```

---

## Roadmap

| Week | Focus | Status |
|---|---|---|
| 1 | Environment + data download + EDA | ✅ Complete |
| 2 | Feature engineering pipeline | ✅ Complete |
| 3 | LR + XGBoost baselines, label validation | ✅ Complete |
| 4 | LSTM + Transformer training | ✅ Complete |
| 5 | Platt calibration + ablations + eval suite | ⏳ In progress |
| 6 | PAP scoring + attention explainability | ⏳ Pending |
| 7 | FastAPI + Streamlit dashboard | ⏳ Pending |
| 8 | Docker + deployment (Render / Streamlit Cloud) | ⏳ Pending |

---

## License & Acknowledgements

- Data: **StatsBomb Open Data** — used under their [terms of use](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf). Attribution to StatsBomb is required in any public-facing use.
- Code: see `LICENSE` (to be added).

Built with PyTorch, scikit-learn, XGBoost, FastAPI, Streamlit, and mplsoccer.

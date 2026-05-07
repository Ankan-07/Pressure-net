# PressureNet — Codebase Reference

Detailed map of every source file: what it does, its public API, and its current state.

---

## Directory Structure

```
c:/Pressure Net/
├── CLAUDE.md                    ← LLM context (read this first)
├── README.md                    ← Public-facing project overview
├── codebase.md                  ← This file
├── main.py                      ← Placeholder (not yet wired up)
├── test_loader.py               ← Integration test / smoke test for ingestion
├── pyproject.toml               ← Project metadata and dependencies
├── uv.lock                      ← Locked dependency versions
├── .python-version              ← 3.11
├── .gitignore                   ← Excludes data/, logs/, .venv/
├── data/
│   ├── raw/
│   │   ├── events/              ← {match_id}.json per match
│   │   ├── three-sixty/         ← {match_id}.json per match
│   │   ├── lineups/             ← {match_id}.json per match
│   │   └── matches/             ← {competition_id}_{season_id}.json
│   └── processed/
│       ├── events.parquet       ← 64.7 MB
│       ├── frames.parquet       ← 131.8 MB
│       └── lineups.parquet      ← 181.7 KB
├── reference doc/
│   ├── PressureNet_Build_Plan.docx
│   └── PressureNet_Build_Plan_1.docx
└── src/
    ├── __init__.py
    ├── exception.py
    ├── logger.py
    ├── utils.py                 ← Empty
    ├── ingestion/
    │   ├── downloader.py
    │   ├── loader.py
    │   └── validator.py         ← Empty
    ├── transformation/
    │   └── engineer.py
    ├── evaluation/
    │   ├── __init__.py
    │   └── metrics.py           ← Empty
    ├── models/
    │   └── model_trainer.py     ← Empty
    └── notebooks/
        └── visualization.ipynb
```

---

## Module Reference

### `src/logger.py`

Configures Python's standard `logging` to write to `logs/{timestamp}.log`.

```python
import logging  # use this in all modules

logging.info("message")
logging.error("message")
```

Note: the module creates the log directory immediately on import. Importing it as a side effect is the current pattern — don't refactor unless intentional.

---

### `src/exception.py`

Custom exception wrapper that captures file name and line number from the traceback.

```python
from src.exception import CustomException
import sys

try:
    ...
except Exception as e:
    raise CustomException(e, sys)
```

`CustomException.__str__()` returns a formatted message:
```
Error occured in python script name [path/to/file.py] line number [42] error message[original error]
```

---

### `src/ingestion/downloader.py`

Downloads raw StatsBomb data from GitHub. Skips files that already exist (idempotent).

**Competitions hardcoded:**
```python
competitions = [
    (55, 282, "EURO 2024"),
    (55,  43, "EURO 2020"),
    (43, 106, "World Cup 2022"),
]
```

**Public API:**
```python
download_file(source: str, save_path: str) -> None
    # Downloads source URL to save_path. No-op if file exists.

get_match_ids(competition_id: int, season_id: int) -> list[int]
    # Downloads matches JSON and returns list of match_id integers.

main() -> None
    # Downloads all events, three-sixty, and lineups for all competitions.
```

**Run:**
```bash
python src/ingestion/downloader.py
```

**Output directory structure:**
```
data/raw/
  events/{match_id}.json
  three-sixty/{match_id}.json
  lineups/{match_id}.json
  matches/{competition_id}_{season_id}.json
```

---

### `src/ingestion/loader.py`

Loads raw JSON files into pandas DataFrames and saves processed Parquet files.

**Public API:**

```python
load_events(data_dir: str) -> pd.DataFrame
    # Reads all JSON in data_dir/events/, concatenates into one DataFrame.
    # Adds match_id column (string, derived from filename).
    # Returns: one row per event.

load_frames(data_dir: str) -> pd.DataFrame
    # Reads all JSON in data_dir/three-sixty/.
    # Returns: one row per event (freeze_frame is still a list column).

load_frames_exploaded(data_dir: str) -> pd.DataFrame
    # Calls load_frames() then explodes freeze_frame → one row per player per event.
    # Unpacks location dict into loc_x, loc_y columns.
    # Returns columns: event_uuid, match_id, teammate, actor, keeper, loc_x, loc_y

load_lineups(data_dir: str) -> pd.DataFrame
    # Reads all JSON in data_dir/lineups/.
    # Flattens team → player structure.
    # Adds team_id, team_name, match_id to each player row.

save_processed(events, frames, lineups, output_dir: str) -> None
    # Saves all three DataFrames as Parquet to output_dir/.

load_processed(output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
    # Loads events.parquet, frames.parquet, lineups.parquet from output_dir/.
    # Returns (events, frames, lineups).
```

**Usage pattern (from test_loader.py):**
```python
from src.ingestion.loader import load_events, load_frames_exploaded, load_lineups, save_processed

events  = load_events("data/raw")           # ~615,202 rows
frames  = load_frames_exploaded("data/raw") # ~millions of rows
lineups = load_lineups("data/raw")

save_processed(events, frames, lineups, "data/processed")
```

**Known quirk:** `load_frames_exploaded` has a docstring inside the try block and a typo in the name (should be `exploded`). Do not rename without updating all call sites.

---

### `src/transformation/engineer.py`

Feature engineering for pressing events. Partially implemented.

**Import issue:** this file currently uses bare imports (`from logger import logging`, `from exception import CustomException`) that only work when run from the `src/` directory. Should be `from src.logger import logging` etc. Fix when next modifying this file.

**Public API (implemented):**

```python
filter_pressing_events(events: pd.DataFrame, frames_exploded: pd.DataFrame) -> pd.DataFrame
    # Returns subset of events that are valid pressing situations.
    # Criteria:
    #   - event type is one of: Pass, Carry, Dribble, Shot
    #   - event has a matching freeze frame (event_uuid in frames_exploded)
    #   - at least one opponent visible (teammate == False in frames_exploded)
    #   - under_pressure == True
    #   - not a goalkeeper event (goalkeeper column is NaN)
    # Returns: filtered events DataFrame (same schema as input).

get_label(row: pd.Series) -> int
    # Computes binary label for a single pressing event row.
    # Returns 1 (success) if:
    #   Pass:    no outcome (completed pass has no outcome in StatsBomb schema)
    #   Dribble: outcome.name == "Complete"
    #   Shot:    outcome.name in [Goal, Saved, Blocked, Post, ...]
    #   Carry:   end_location[0] - location[0] >= 5 (5m+ progressive)
    # Returns 0 otherwise.

assign_labels(pressing_events: pd.DataFrame) -> pd.DataFrame
    # Applies get_label to each row, adds 'label' column.
    # Does not mutate input (uses .copy()).
```

**Public API (stub only — not yet implemented):**

```python
get_defender_distance(event_id, event_loc, frames_exploded)
    # Intended to compute distances from ball-carrier to all visible defenders.
    # Only the signature exists — body is not implemented.
```

**Next steps for this module:**
Complete the full 15-feature vector extraction per pressing event. See CLAUDE.md for the feature specification. The key missing functions are:
- `get_closing_speeds(frame_t, frame_t_minus_1, event_uuid)` — change in defender distances between consecutive frames
- `get_voronoi_area(freeze_frame_df, ball_carrier_loc)` — scipy Voronoi area for ball-carrier
- `get_pitch_zone(x, y)` — 6-zone integer encoding
- `build_temporal_window(event, events_df, frames_df)` — assembles T=3 feature tensor
- `build_feature_vector(event, freeze_frame)` — single timestep → numpy array of 15 features

---

### `src/notebooks/visualization.ipynb`

EDA notebook. Currently the most complete analytical artifact.

**What it demonstrates:**
- Loading processed Parquet files
- Applying `filter_pressing_events()` and `assign_labels()`
- Label distribution: ~59,719 pressing events from 615,202 total
- Success rates by event type: Pass 73%, Shot 57.5%, Dribble 53.8%, Carry 22%
- Pressing event distribution by pitch zone: Defensive third 41.4%, Middle 44.4%, Final 39.4%
- Scatter and KDE plots of pressing event locations using mplsoccer

**Run:** open with Jupyter Lab or `jupyter notebook`. Requires processed Parquet files in `data/processed/`.

---

### `test_loader.py`

Integration smoke test that runs the full ingestion pipeline and prints shapes.

```bash
python test_loader.py
```

Expected output:
```
Events:  (615202, ...)
Frames:  (..., 7)
Lineups: (..., ...)

Saving to parquet...
Done.
label
0    ...
1    ...
```

---

## Dependency Map

```
test_loader.py
  └── src.ingestion.loader (load_events, load_frames_exploaded, load_lineups, save_processed)
        └── src.exception.CustomException
        └── src.logger.logging
  └── src.transformation.engineer (filter_pressing_events, get_label, assign_labels)

src.ingestion.downloader
  └── src.exception.CustomException
  └── urllib.request (stdlib)

src.notebooks.visualization.ipynb
  └── src.ingestion.loader (load_processed)
  └── src.transformation.engineer
  └── mplsoccer, matplotlib, pandas
```

---

## Data Schemas

### events.parquet
Key columns (many more exist — StatsBomb events are wide):

| Column | Type | Notes |
|---|---|---|
| `id` | str | UUID, joins to `frames.event_uuid` |
| `index` | int | Event sequence number within match |
| `type` | dict | `{"id": ..., "name": "Pass"}` — use `.get("name")` |
| `period` | int | 1 or 2 (or extra time) |
| `timestamp` | str | HH:MM:SS.mmm |
| `minute` | int | |
| `second` | int | |
| `team` | dict | `{"id": ..., "name": "..."}` |
| `player` | dict | `{"id": ..., "name": "..."}` |
| `position` | dict | `{"id": ..., "name": "Center Forward"}` |
| `location` | list | `[x, y]` in StatsBomb coords (0-120 x, 0-80 y) |
| `under_pressure` | bool / NaN | NaN means False |
| `goalkeeper` | dict / NaN | Present only for GK events |
| `pass` | dict / NaN | Pass attributes including `outcome` |
| `carry` | dict / NaN | Carry attributes including `end_location` |
| `dribble` | dict / NaN | Dribble attributes including `outcome` |
| `shot` | dict / NaN | Shot attributes including `outcome` |
| `match_id` | str | Added by loader (derived from filename) |

### frames.parquet (exploded)

| Column | Type | Notes |
|---|---|---|
| `event_uuid` | str | Joins to `events.id` |
| `match_id` | str | |
| `teammate` | bool | True = same team as ball-carrier |
| `actor` | bool | True = this is the ball-carrier |
| `keeper` | bool | True = goalkeeper |
| `loc_x` | float | Player x position |
| `loc_y` | float | Player y position |

### lineups.parquet

| Column | Type | Notes |
|---|---|---|
| `player_id` | int | StatsBomb player ID |
| `player_name` | str | |
| `team_id` | int | |
| `team_name` | str | |
| `match_id` | str | |
| `positions` | list | List of position dicts |

### features.parquet (to be created — Week 2 deliverable)

Intermediate artifact produced by the completed `engineer.py`. Contains the engineered feature tensor for every pressing event.

**Logical shape:** `(N_pressing_events, T=3, F=15)` — but stored flat in Parquet as N rows × 45 columns.

**Column naming convention:** `f{timestep}_{feature_name}` e.g. `f0_dist_to_nearest_defender` (t-2), `f1_dist_to_nearest_defender` (t-1), `f2_dist_to_nearest_defender` (t=0).

| Column group | Columns | Notes |
|---|---|---|
| Event metadata | `event_uuid`, `match_id`, `player_id`, `player_name` | Carry-along columns, not model inputs |
| Label | `label` | 0 or 1 — from `assign_labels()` |
| t-2 features | `f0_dist_to_nearest_defender` … `f0_body_orientation_cos` | 15 float columns |
| t-1 features | `f1_dist_to_nearest_defender` … `f1_body_orientation_cos` | 15 float columns |
| t=0 features | `f2_dist_to_nearest_defender` … `f2_body_orientation_cos` | 15 float columns |
| Padding flags | `f0_is_padded`, `f1_is_padded` | Boolean — True if timestep was zero-padded (match start) |

**Validation assertions to run after creation:**
```python
assert features.shape[1] == 47  # 45 feature cols + event_uuid + match_id + player_id + player_name + label + 2 padding flags = 50 total; adjust as needed
assert features[["f2_dist_to_nearest_defender"]].min().item() >= 0
assert features["label"].isin([0, 1]).all()
assert features["f0_is_padded"].dtype == bool
```

### pressure_ratings.csv (to be created — Week 6 deliverable)

Final output of the player ranking engine. One row per qualifying player per competition-season.

| Column | Type | Notes |
|---|---|---|
| `player_id` | int | StatsBomb player ID |
| `player_name` | str | |
| `team` | str | Team name in that competition |
| `competition` | str | e.g. "EURO 2024" |
| `season` | str | e.g. "2024" |
| `n_pressing_events` | int | Total qualifying pressing events; must be ≥ 100 to appear |
| `mean_pap_score` | float | Core metric — positive = outperforms expectation, negative = underperforms |
| `pap_percentile` | float | 0–100; percentile rank among all qualifying players in that competition |
| `avg_difficulty_faced` | float | Mean model probability across player's events — measures how hard their situations were on average |
| `top_pressure_zone` | str | Pitch zone with most of their pressing events: `own_box`, `def_third`, `mid_third`, `final_third`, `opp_box` |
| `attention_top_feature_1` | str | Feature name with highest average attention weight across player's events |
| `attention_top_feature_2` | str | Second highest attention feature — together these describe the player's pressure signature |

**Example rows:**
```
player_id | player_name    | team    | competition | n_pressing_events | mean_pap_score | pap_percentile | avg_difficulty_faced
3089      | Kylian Mbappé  | France  | EURO 2024   | 247               | 0.087          | 91             | 0.51
5503      | Granit Xhaka   | Switzerland | EURO 2024| 198              | -0.031         | 42             | 0.46
```

---

## StatsBomb Data Conventions

- **Pitch coordinates:** x: 0-120 (left goal to right goal), y: 0-80 (bottom touchline to top)
- **Pass outcome:** a completed pass has `outcome = None` (the field is absent or null). An incomplete pass has `outcome = {"id": ..., "name": "Incomplete"}` etc.
- **under_pressure:** stored as `True` or `NaN` (not `False`) — always use `== True` or `.isna()` checks, never `== False`
- **Event type names:** accessed as `row["type"]["name"]` or `row["type"].get("name")` — it's a dict, not a string
- **Location:** `[x, y]` list — access as `row["location"][0]` for x, `row["location"][1]` for y

---

## Upcoming Modules to Build

### `src/models/transformer.py`
```python
class PressureTransformer(nn.Module):
    # Input shape:  (batch_size, T=3, F=15)
    # Output shape: (batch_size, 1) — raw logit (apply sigmoid for probability)
```
See CLAUDE.md for the full architecture specification.

### `src/models/dataset.py`
```python
class PressureDataset(Dataset):
    # __init__(features_path: str, labels_path: str)
    # __len__() -> int
    # __getitem__(idx) -> tuple[torch.Tensor, torch.Tensor]
    #   returns: (feature_tensor of shape (3, 15), label scalar)
    # Must handle padding masks for sequences shorter than T=3
```

### `src/models/train.py`
Training loop responsibilities:
- Load `features.parquet`, split by match_id (not by event row)
- Instantiate `PressureTransformer`, `AdamW`, `CosineAnnealingLR`
- Run train/val loop with early stopping (patience=7)
- Save best checkpoint to `models/pressurenet.pt`
- Log train/val loss curves

### `src/models/calibrate.py`
```python
class PlattCalibrator:
    # fit(raw_logits: np.ndarray, labels: np.ndarray) -> None
    # predict_proba(raw_logits: np.ndarray) -> np.ndarray
    # save(path: str) -> None
    # load(path: str) -> PlattCalibrator
```

### `src/evaluation/metrics.py`
```python
def compute_pap_scores(events_with_preds: pd.DataFrame, min_events: int = 100) -> pd.DataFrame
    # PAP(player) = (1/N) × Σ [actual_outcome(e) − model_probability(e)] for all N events
    # Filters to players with n_pressing_events >= min_events (default 100)
    # Returns DataFrame matching pressure_ratings.csv schema (see Data Schemas section)

def build_leaderboard(pap_scores: pd.DataFrame, top_n: int = 20) -> pd.DataFrame
    # Returns top_n and bottom_n players by pap_score

def compute_all_eval_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict
    # Returns dict with keys: roc_auc, brier_score, log_loss
    # Use after Platt calibration on the held-out test set
    # Targets: roc_auc > 0.70, brier_score < 0.22, log_loss < 0.60
```

### `src/ranking/pap_score.py`
```python
def compute_pap(events_df: pd.DataFrame, min_events: int = 100) -> pd.DataFrame
    # events_df must have columns: player_id, player_name, team, competition, season,
    #   actual_outcome (0/1), model_probability (float), pitch_zone
    # Returns pressure_ratings.csv with all columns including pap_percentile

def get_top_pressure_zone(player_events: pd.DataFrame) -> str
    # Returns the pitch_zone with the most events for this player
```

### `src/ranking/explainer.py`
```python
# Extracts attention weights from the trained PressureTransformer's first encoder layer.
# Uses PyTorch forward hooks — register on TransformerEncoderLayer[0].self_attn during inference.
#
# For each player's pressing events:
#   1. Collect CLS token attention row per event: shape (n_heads=4, T+1=4)
#      CLS attends to positions [1,2,3] = timesteps [t-2, t-1, t=0]
#   2. Average across all events + all heads → shape (T=3,) = per-timestep importance
#   3. Weight features by XGBoost importance scores → (T=3, F=15) heatmap
#   4. Normalise so heatmap values sum to 1 per player
#
# Output files:
#   outputs/attention/{player_id}.npy  → shape (3, 15) float32 array
#   outputs/attention/summary.csv      → player_id, top_feature_t0, top_feature_t1, top_feature_t2
#
# Player profile interpretation examples:
#   High [t0, closing_speed_nearest]  → player struggles when presser is sprinting to close
#   High [t-1, voronoi_area]          → player struggles when space was tight before receiving
#   High [t0, dist_to_nearest]        → player struggles even when pressure is moderate (mental)
#   Flat distribution                 → elite, no single pressure weakness

def extract_attention_weights(model, dataloader) -> dict[int, np.ndarray]
    # Returns {player_id: attention_heatmap (3, 15)} for all players in dataloader

def plot_attention_heatmap(heatmap: np.ndarray, player_name: str, save_path: str) -> None
    # matplotlib imshow — rows=timesteps (t-2/t-1/t=0), cols=feature names
    # Use diverging colourmap; annotate top-3 cells
```

### `api/main.py` — FastAPI Endpoints

Run locally:
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
# Auto-generated OpenAPI docs: http://localhost:8000/docs
```

| Endpoint | Method | Query Params | Description |
|---|---|---|---|
| `/api/v1/health` | GET | — | Model version, calibration status, n_training_events |
| `/api/v1/evaluate-event` | POST | — | Input: T=3 feature vectors → difficulty score + attention weights |
| `/api/v1/player/{player_id}/rating` | GET | `competition`, `season` | PAP score, percentile, n_events, avg_difficulty_faced |
| `/api/v1/leaderboard` | GET | `competition`, `season`, `min_events`, `top_n` | Sorted player list |

**POST /api/v1/evaluate-event — full JSON contract:**
```json
// Request
{
  "features": [
    [0.2, 0.4, 0.6, 1.2, 0.8, 0.55, 0.42, 18.5, 4, 1.2, 2, 0.30, 0, 0.50, 0.87],
    [0.3, 0.5, 0.7, 1.5, 0.9, 0.54, 0.43, 17.2, 4, 0.8, 2, 0.35, 0, 0.50, 0.87],
    [0.15, 0.35, 0.55, 2.1, 1.1, 0.53, 0.44, 14.1, 4, 0.3, 3, 0.40, 0, 0.50, 0.87]
  ]
}
// Rows: [t-2, t-1, t=0]. Each row: 15 features in index order (0=dist_to_nearest … 14=body_orientation_cos)

// Response
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

**GET /api/v1/player/{player_id}/rating — full JSON contract:**
```json
// Response
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

**GET /api/v1/leaderboard — full JSON contract:**
```json
// Response
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

### `dashboard/app.py` — Streamlit Pages

1. **Leaderboard** — sortable table of all players by PAP score, colour-coded
2. **Player Profile** — PAP score, difficulty distribution, 3×15 attention heatmap
3. **Event Explorer** — match pitch viz with pressing events coloured by difficulty
4. **Model Calibration** — reliability diagram, AUC curve, feature importance

---

## Configuration (Planned)

`config.yaml` does not yet exist. When created, it should contain:

```yaml
data:
  raw_dir: data/raw
  processed_dir: data/processed
  features_dir: data/features
  outputs_dir: outputs

competitions:
  - {id: 55, season: 282, name: "EURO 2024"}
  - {id: 55, season: 43,  name: "EURO 2020"}
  - {id: 43, season: 106, name: "World Cup 2022"}

model:
  d_model: 64
  n_heads: 4
  n_layers: 2
  d_ff: 128
  dropout_attn: 0.1
  dropout_head: 0.2
  seq_len: 3
  n_features: 15

training:
  batch_size: 128
  lr: 3e-4
  weight_decay: 1e-2
  epochs: 50
  early_stopping_patience: 7
  train_split: 0.70
  val_split: 0.15
  test_split: 0.15

evaluation:
  min_pressing_events: 100
```

---

## Testing Strategy

Unit tests should live in `tests/` (does not yet exist). Priority order:

1. `test_engineer.py` — test each feature function on a known small freeze frame
2. `test_transformer.py` — test forward pass shape `(batch, T, F) → (batch, 1)`
3. `test_dataset.py` — test DataLoader produces correct shapes and types
4. `test_pap.py` — test PAP computation on a synthetic known-answer dataset
5. `test_api.py` — pytest + httpx for FastAPI endpoint contracts

Run with:
```bash
uv run pytest tests/ -v
```

Baseline AUC checks (Week 3 gates):
- LR AUC > 0.63 on flattened features
- XGBoost AUC > 0.68
- Transformer val AUC > 0.70

If baselines fail, the labels or features have a bug — do not proceed to the Transformer.

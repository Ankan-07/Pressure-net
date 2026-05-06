import os
import sys
from logger import logging
from exception import CustomException
import pandas as pd
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, MultiPolygon

def filter_pressing_events(events, frames_exploded):
    allowed = ["Pass", "Carry", "Dribble", "Shot"]
    event_id_with_freezeframes = frames_exploded["event_uuid"]

    pressing_events = events[

        events["type"].apply(lambda x: x.get("name")).isin(allowed) &

        events["id"].isin(event_id_with_freezeframes) &

        events["id"].isin(frames_exploded[frames_exploded["teammate"] == False]["event_uuid"].unique())  &

        events["goalkeeper"].isna() &

        (events["under_pressure"] == True)
    ]

    return pressing_events

def get_label(row):
    event_type = row["type"].get("name")

    if event_type == "Pass":
        if row["pass"].get("outcome") is None:
            return 1
    elif event_type == "Dribble":
        if (row["dribble"].get("outcome", {}).get("name") == "Complete"):
            return 1
    elif event_type == "Shot":
        if row["shot"].get("outcome",{}).get("name") in ["Goal", "Saved", "Blocked", "Post", "Saved to Post", "Saved Off Target"]:
            return 1
    elif event_type == "Carry":
        if row["carry"].get("end_location")[0] - row["location"][0] >=5:
            return 1

    return 0

def assign_labels(pressing_events):
    pressing_events = pressing_events.copy()
    pressing_events["label"] = pressing_events.apply(get_label, axis=1)

    return pressing_events

def get_defender_distance(event_id, event_loc, frames_exploded):
    defenders = frames_exploded[(frames_exploded['event_uuid'] == event_id) 
                                & (frames_exploded['teammate']==False)
                                & (frames_exploded['actor']== False)
                                & (frames_exploded['keeper']==False)]
    
    if len(defenders) == 0:
        return (np.nan, np.nan, np.nan, 0)
    
    distances = np.sqrt((defenders["loc_x"]-event_loc[0])**2+ (defenders["loc_y"] - event_loc[1])**2)

    n_defenders_within_5m = (distances <= 5).sum()

    sorted_dists = sorted(distances.values)
    return (
        sorted_dists[0] if len(sorted_dists) >0 else np.nan,
        sorted_dists[1] if len(sorted_dists) >1 else np.nan,
        sorted_dists[2] if len(sorted_dists) >2 else np.nan,
        n_defenders_within_5m
    )

def get_closing_speeds(event_t0, event_t1, frames_exploded):
    """
    Calculate closing speeds of nearest and 2nd nearest defenders from t-1 to t=0.

    Args:
        event_t0: pandas Series - the current pressing event (t=0)
        event_t1: pandas Series - the previous event (t-1)
        frames_exploded: DataFrame - exploded freeze frames

    Returns:
        Tuple of (closing_speed_nearest, closing_speed_2nd) in m/s
        Positive = defender is approaching (distance decreasing)
    """
    # Check if t-1 has a freeze frame
    event_id_t1 = event_t1["id"]
    if len(frames_exploded[frames_exploded["event_uuid"] == event_id_t1]) == 0:
        return (0.0, 0.0)

    # Check if periods differ (e.g. end of first half to start of second)
    if event_t0.get("period") != event_t1.get("period"):
        return (0.0, 0.0)

    # Compute time delta in seconds
    time_t0 = event_t0["minute"] * 60 + event_t0["second"]
    time_t1 = event_t1["minute"] * 60 + event_t1["second"]
    time_delta = time_t0 - time_t1

    if time_delta <= 0:
        return (0.0, 0.0)

    # Get defender distances at both time points
    event_id_t0 = event_t0["id"]
    loc_t0 = event_t0["location"]
    loc_t1 = event_t1["location"]

    dist_t0_1, dist_t0_2, _, _ = get_defender_distance(event_id_t0, loc_t0, frames_exploded)
    dist_t1_1, dist_t1_2, _, _ = get_defender_distance(event_id_t1, loc_t1, frames_exploded)

    # Calculate closing speeds: (distance_at_t-1 - distance_at_t0) / time_delta
    # Positive = defender is closing in
    closing_speed_nearest = (dist_t1_1 - dist_t0_1) / time_delta if not (pd.isna(dist_t1_1) or pd.isna(dist_t0_1)) else 0.0
    closing_speed_2nd = (dist_t1_2 - dist_t0_2) / time_delta if not (pd.isna(dist_t1_2) or pd.isna(dist_t0_2)) else 0.0

    return (closing_speed_nearest, closing_speed_2nd)

def get_pitch_zone(x, y):
    # Zone 0: own penalty area
    if x < 18:
        return 0
    # Zone 1: defensive third outside own box
    if x < 40:
        return 1
    # Middle third (x: 40–80) — split into half-spaces and central channel
    if x < 80:
        if y < 27:
            return 2  # left half-space
        if y > 53:
            return 3  # right half-space
        return 4      # central channel → grouped with final third
    # Zone 5: opponent penalty area
    if x > 102:
        return 5
    # Zone 4: final third outside opp box (x: 80–102)
    return 4


def get_pass_lane_density(event_id, ball_carrier_loc, frames_exploded):
    opponents = frames_exploded[
        (frames_exploded["event_uuid"] == event_id)
        & (frames_exploded["teammate"] == False)
        & (frames_exploded["actor"] == False)
        & (frames_exploded["keeper"] == False)
    ]

    if len(opponents) == 0:
        return 0.0

    dx = opponents["loc_x"].values - ball_carrier_loc[0]
    dy = opponents["loc_y"].values - ball_carrier_loc[1]
    angles = np.degrees(np.arctan2(dy, dx)) % 360  # 0–360

    # 8 bins of 45° each
    bin_counts = np.zeros(8)
    for angle in angles:
        bin_idx = int(angle // 45) % 8
        bin_counts[bin_idx] += 1

    # Average of top-3 most populated bins
    top3 = np.sort(bin_counts)[-3:]
    return float(np.mean(top3))


def get_ball_carrier_location(event):
    x, y = event["location"]
    return (x / 120.0, y / 80.0)

PITCH = Polygon([(0, 0), (120, 0), (120, 80), (0, 80)])

def get_voronoi_area(event_id, frames_exploded):
    players = frames_exploded[frames_exploded["event_uuid"] == event_id]

    if len(players) < 4:
        return np.nan

    points = players[["loc_x", "loc_y"]].values

    # Find ball-carrier using actor flag (more reliable than coordinate matching)
    actor_mask = players["actor"].values == True
    if not actor_mask.any():
        return np.nan
    carrier_idx = np.where(actor_mask)[0][0]

    vor = Voronoi(points)

    # Get the region index for the ball-carrier, then the vertex indices for that region
    region_idx = vor.point_region[carrier_idx]
    region = vor.regions[region_idx]

    if len(region) == 0:
        return np.nan

    if -1 in region:
        # Open region — build from finite vertices only, then clip to pitch to close it
        finite_verts = [vor.vertices[i] for i in region if i != -1]
        if len(finite_verts) < 2:
            return np.nan
        region_poly = Polygon(finite_verts).convex_hull
    else:
        region_poly = Polygon([vor.vertices[i] for i in region])

    clipped = region_poly.intersection(PITCH)
    return clipped.area


ACTION_TYPE_MAP = {"Pass": 0, "Dribble": 1, "Carry": 2, "Shot": 3, "Cross": 4}

def build_feature_vector(event, prev_event, frames_exploded):
    event_id = event["id"]
    loc = event["location"]

    # Features 0-2, 10: defender distances + count within 5m
    d1, d2, d3, n_within_5m = get_defender_distance(event_id, loc, frames_exploded)

    # Features 3-4: closing speeds (0.0 if no prior event)
    if prev_event is not None:
        cs_nearest, cs_2nd = get_closing_speeds(event, prev_event, frames_exploded)
    else:
        cs_nearest, cs_2nd = 0.0, 0.0

    # Features 5-6: normalised ball-carrier position
    norm_x, norm_y = get_ball_carrier_location(event)

    # Feature 7: Voronoi area (nan → 0.0)
    voronoi_area = get_voronoi_area(event_id, frames_exploded)
    if np.isnan(voronoi_area):
        voronoi_area = 0.0

    # Feature 8: pitch zone
    pitch_zone = get_pitch_zone(loc[0], loc[1])

    # Feature 9: time since last touch in seconds (0.0 if no prior event or different period)
    if prev_event is not None and event.get("period") == prev_event.get("period"):
        time_since = (event["minute"] * 60 + event["second"]) - (prev_event["minute"] * 60 + prev_event["second"])
        time_since = max(0.0, float(time_since))
    else:
        time_since = 0.0

    # Feature 11: pass lane density
    pass_lane_density = get_pass_lane_density(event_id, loc, frames_exploded)

    # Feature 12: action type integer
    action_name = event["type"].get("name", "Pass")
    action_type = ACTION_TYPE_MAP.get(action_name, 0)

    # Features 13-14: body orientation sin/cos (0.0 if missing)
    # StatsBomb stores player orientation in the `player_data` field as `position_angle`
    orientation_deg = None
    if isinstance(event.get("player_data"), dict):
        orientation_deg = event["player_data"].get("position_angle")
    if orientation_deg is None:
        orientation_sin, orientation_cos = 0.0, 0.0
    else:
        rad = orientation_deg * np.pi / 180.0
        orientation_sin = float(np.sin(rad))
        orientation_cos = float(np.cos(rad))

    return np.array([
        d1 if not np.isnan(d1) else 0.0,   # 0
        d2 if not np.isnan(d2) else 0.0,   # 1
        d3 if not np.isnan(d3) else 0.0,   # 2
        cs_nearest,                          # 3
        cs_2nd,                              # 4
        norm_x,                              # 5
        norm_y,                              # 6
        voronoi_area,                        # 7
        float(pitch_zone),                   # 8
        time_since,                          # 9
        float(n_within_5m),                  # 10
        pass_lane_density,                   # 11
        float(action_type),                  # 12
        orientation_sin,                     # 13
        orientation_cos,                     # 14
    ], dtype=np.float32)
import os
import sys
from logger import logging
from exception import CustomException
import pandas as pd
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon

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
                                     
    event_id_t1 = event_t1["id"]
    if len(frames_exploded[frames_exploded["event_uuid"] == event_id_t1]) == 0:
        return (0.0, 0.0)

                                                                         
    if event_t0.get("period") != event_t1.get("period"):
        return (0.0, 0.0)

                                   
    time_t0 = event_t0["minute"] * 60 + event_t0["second"]
    time_t1 = event_t1["minute"] * 60 + event_t1["second"]
    time_delta = time_t0 - time_t1

    if time_delta <= 0:
        return (0.0, 0.0)

                                                
    event_id_t0 = event_t0["id"]
    loc_t0 = event_t0["location"]
    loc_t1 = event_t1["location"]

    dist_t0_1, dist_t0_2, _, _ = get_defender_distance(event_id_t0, loc_t0, frames_exploded)
    dist_t1_1, dist_t1_2, _, _ = get_defender_distance(event_id_t1, loc_t1, frames_exploded)

                                                                               
    closing_speed_nearest = (dist_t1_1 - dist_t0_1) / time_delta if not (pd.isna(dist_t1_1) or pd.isna(dist_t0_1)) else 0.0
    closing_speed_2nd = (dist_t1_2 - dist_t0_2) / time_delta if not (pd.isna(dist_t1_2) or pd.isna(dist_t0_2)) else 0.0

    return (closing_speed_nearest, closing_speed_2nd)

def get_pitch_zone(x, y):
                              
    if x < 18:
        return 0
                                             
    if x < 40:
        return 1
                                                                          
    if x < 80:
        if y < 27:
            return 2                   
        if y > 53:
            return 3                    
        return 4                                                  
                                   
    if x > 102:
        return 5
                                                     
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
    angles = np.degrees(np.arctan2(dy, dx)) % 360         

                        
    bin_counts = np.zeros(8)
    for angle in angles:
        bin_idx = int(angle // 45) % 8
        bin_counts[bin_idx] += 1

                                          
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

                                                                                 
    actor_mask = players["actor"].values == True
    if not actor_mask.any():
        return np.nan
    carrier_idx = np.where(actor_mask)[0][0]

    vor = Voronoi(points)

                                                                                        
    region_idx = vor.point_region[carrier_idx]
    region = vor.regions[region_idx]

    if len(region) == 0:
        return np.nan

    if -1 in region:
                                                                                       
        finite_verts = [vor.vertices[i] for i in region if i != -1]
        if len(finite_verts) < 3:
            return np.nan
        region_poly = Polygon(finite_verts).convex_hull
    else:
        verts = [vor.vertices[i] for i in region]
        if len(verts) < 3:
            return np.nan
        region_poly = Polygon(verts)

    clipped = region_poly.intersection(PITCH)
    return clipped.area


ACTION_TYPE_MAP = {"Pass": 0, "Dribble": 1, "Carry": 2, "Shot": 3, "Cross": 4}

def build_feature_vector(event, prev_event, frames_exploded):
    event_id = event["id"]
    loc = event["location"]

                                                            
    d1, d2, d3, n_within_5m = get_defender_distance(event_id, loc, frames_exploded)

                                                          
    if prev_event is not None:
        cs_nearest, cs_2nd = get_closing_speeds(event, prev_event, frames_exploded)
    else:
        cs_nearest, cs_2nd = 0.0, 0.0

                                                    
    norm_x, norm_y = get_ball_carrier_location(event)

                                         
    voronoi_area = get_voronoi_area(event_id, frames_exploded)
    if np.isnan(voronoi_area):
        voronoi_area = 0.0

                           
    pitch_zone = get_pitch_zone(loc[0], loc[1])

                                                                                             
    if prev_event is not None and event.get("period") == prev_event.get("period"):
        time_since = (event["minute"] * 60 + event["second"]) - (prev_event["minute"] * 60 + prev_event["second"])
        time_since = max(0.0, float(time_since))
    else:
        time_since = 0.0

                                   
    pass_lane_density = get_pass_lane_density(event_id, loc, frames_exploded)

                                     
    action_name = event["type"].get("name", "Pass")
    action_type = ACTION_TYPE_MAP.get(action_name, 0)

                                                               
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
        d1 if not np.isnan(d1) else 0.0,      
        d2 if not np.isnan(d2) else 0.0,      
        d3 if not np.isnan(d3) else 0.0,      
        cs_nearest,                             
        cs_2nd,                                 
        norm_x,                                 
        norm_y,                                 
        voronoi_area,                           
        float(pitch_zone),                      
        time_since,                             
        float(n_within_5m),                      
        pass_lane_density,                       
        float(action_type),                      
        orientation_sin,                         
        orientation_cos,                         
    ], dtype=np.float32)


def build_temporal_window(event_t0, prior_events, frames_lookup, feature_cache=None):
    event_t1 = prior_events.iloc[-1] if len(prior_events) >= 1 else None
    event_t2 = prior_events.iloc[-2] if len(prior_events) >= 2 else None
    event_t3 = prior_events.iloc[-3] if len(prior_events) >= 3 else None                                

                                                               
    timesteps = [
        (event_t2, event_t3),               
        (event_t1, event_t2),               
        (event_t0, event_t1),               
    ]

    features = np.zeros((3, 15), dtype=np.float32)
    mask = np.zeros(3, dtype=bool)

    for i, (event, prev) in enumerate(timesteps):
        if event is None:
            mask[i] = True
            continue

        eid = event["id"]

                                                             
        if feature_cache is not None and eid in feature_cache:
            cached = feature_cache[eid]
            if cached is None:
                mask[i] = True
            else:
                features[i] = cached
            continue

                                                                          
        event_frames = frames_lookup.get(eid)
        if event_frames is None or len(event_frames) == 0:
            mask[i] = True
            if feature_cache is not None:
                feature_cache[eid] = None
            continue

        try:
            vec = build_feature_vector(event, prev, event_frames)
            features[i] = vec
            if feature_cache is not None:
                feature_cache[eid] = vec
        except Exception as e:
            mask[i] = True
            if feature_cache is not None:
                feature_cache[eid] = None
            logging.warning(f"build_feature_vector failed for event {eid} at timestep {i}: {type(e).__name__}: {e}")

    return features, mask


def build_all_features(pressing_events_labeled, events, frames_exploded,
                       output_path="data/features/features.parquet",
                       checkpoint_every=5000, sample_n=None):
    from tqdm import tqdm
    import time

    if sample_n is not None:
        pressing_events_labeled = pressing_events_labeled.iloc[:sample_n]

                                                    
    t0 = time.time()
    events_sorted = events.sort_values(["match_id", "index"]).reset_index(drop=True)
    events_by_match = {mid: g.reset_index(drop=True) for mid, g in events_sorted.groupby("match_id")}
    setup_events_time = time.time() - t0

                                                             
    t1 = time.time()
    frames_lookup = {eid: g for eid, g in frames_exploded.groupby("event_uuid")}
    setup_frames_time = time.time() - t1

    print(f"Setup: events sort+group {setup_events_time:.1f}s, frames groupby {setup_frames_time:.1f}s")

                                                                                        
    feature_cache = {}

    n = len(pressing_events_labeled)
    feature_rows = np.zeros((n, 3, 15), dtype=np.float32)
    mask_rows = np.zeros((n, 3), dtype=bool)
    event_ids = []
    match_ids = []
    labels = []

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    for i, (_, ev) in enumerate(tqdm(pressing_events_labeled.iterrows(), total=n, desc="Building features")):
        match_id = ev["match_id"]
        idx = ev["index"]

        match_events = events_by_match.get(match_id)
        if match_events is None:
            mask_rows[i, :] = True
        else:
            prior = match_events[match_events["index"] < idx]
            features, mask = build_temporal_window(ev, prior, frames_lookup, feature_cache)
            feature_rows[i] = features
            mask_rows[i] = mask

        event_ids.append(ev["id"])
        match_ids.append(match_id)
        labels.append(ev["label"])

                                    
        if (i + 1) % checkpoint_every == 0:
            _save_partial(feature_rows[:i + 1], mask_rows[:i + 1],
                          event_ids, match_ids, labels, output_path + ".partial")

    df_out = _build_output_df(feature_rows, mask_rows, event_ids, match_ids, labels)
    df_out.to_parquet(output_path, index=False)

                           
    partial = output_path + ".partial"
    if os.path.exists(partial):
        os.remove(partial)

    return df_out


def _build_output_df(features, masks, event_ids, match_ids, labels):
                                                            
    n = features.shape[0]
    flat = features.reshape(n, 45)
    feat_cols = [f"feat_t{t}_{f}" for t in range(3) for f in range(15)]
    df = pd.DataFrame(flat, columns=feat_cols)
    df["mask_t0"] = masks[:, 0]       
    df["mask_t1"] = masks[:, 1]       
    df["mask_t2"] = masks[:, 2]                                
    df["event_id"] = event_ids
    df["match_id"] = match_ids
    df["label"] = labels
    return df


def _save_partial(features, masks, event_ids, match_ids, labels, path):
    df = _build_output_df(features, masks, event_ids, match_ids, labels)
    df.to_parquet(path, index=False)
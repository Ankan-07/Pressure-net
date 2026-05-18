from src.ingestion.loader import load_processed
from src.transformation.engineer import (filter_pressing_events, get_label, assign_labels, get_defender_distance, get_closing_speeds, get_voronoi_area, get_pitch_zone, get_pass_lane_density, build_feature_vector)
import pandas as pd
import numpy as np

PROCESSED_DIR = "data/processed"

events, frames, lineups = load_processed(PROCESSED_DIR)

print(f"Events:  {events.shape}")
print(f"Frames:  {frames.shape}")
print(f"Lineups: {lineups.shape}")

                                
pressing_events = filter_pressing_events(events, frames)
pressing_events_labeled = assign_labels(pressing_events)
print(f"\nLabel distribution:\n{pressing_events_labeled['label'].value_counts()}")

"""
def test_get_defender_distance():
    
    print("\n" + "="*60)
    print("Testing get_defender_distance()")
    print("="*60)

    # Get a sample pressing event with a freeze frame
    sample_event = pressing_events_labeled.iloc[0]
    event_id = sample_event["id"]
    event_loc = sample_event["location"]

    print(f"\nTest Event Details:")
    print(f"  Event ID: {event_id}")
    print(f"  Event Type: {sample_event['type'].get('name')}")
    print(f"  Ball-carrier Location: {event_loc}")

    # Get defender distances and count
    dist_nearest, dist_2nd, dist_3rd, n_defenders_within_5m = get_defender_distance(
        event_id, event_loc, frames
    )

    print(f"\nDefender Distances:")
    print(f"  Nearest defender: {dist_nearest:.2f}m" if not pd.isna(dist_nearest) else "  Nearest defender: NaN")
    print(f"  2nd nearest: {dist_2nd:.2f}m" if not pd.isna(dist_2nd) else "  2nd nearest: NaN")
    print(f"  3rd nearest: {dist_3rd:.2f}m" if not pd.isna(dist_3rd) else "  3rd nearest: NaN")
    print(f"  Defenders within 5m: {n_defenders_within_5m}")

    # Validation checks
    print(f"\nValidation Checks:")

    # Check 1: Distances should be non-negative
    assert dist_nearest >= 0 or pd.isna(dist_nearest), "Nearest distance must be >= 0"
    print("  [PASS] Distances are non-negative")

    # Check 2: Distances should be in increasing order
    valid_dists = [d for d in [dist_nearest, dist_2nd, dist_3rd] if not pd.isna(d)]
    if len(valid_dists) > 1:
        assert all(valid_dists[i] <= valid_dists[i+1] for i in range(len(valid_dists)-1)), \
            "Distances must be in increasing order"
        print("  [PASS] Distances are in increasing order")

    # Check 3: Nearest distance should be reasonable for football pitch (< 40m)
    if not pd.isna(dist_nearest):
        assert dist_nearest < 40, "Nearest defender distance should be < 40m"
        print("  [PASS] Nearest distance is reasonable (< 40m)")

    # Check 4: n_defenders_within_5m should be non-negative integer
    assert isinstance(n_defenders_within_5m, (int, np.integer)), "Count must be an integer"
    assert n_defenders_within_5m >= 0, "Defender count must be non-negative"
    print("  [PASS] Defender count is valid non-negative integer")

    # Check 5: n_defenders_within_5m should not exceed total defenders
    total_defenders = len(frames[
        (frames["event_uuid"] == event_id) &
        (frames["teammate"] == False) &
        (frames["actor"] == False) &
        (frames["keeper"] == False)
    ])
    assert n_defenders_within_5m <= total_defenders, \
        f"Defenders within 5m ({n_defenders_within_5m}) cannot exceed total defenders ({total_defenders})"
    print(f"  [PASS] Count is reasonable: {n_defenders_within_5m} / {total_defenders} defenders within 5m")

    # Check 6: Nearest distance should correlate with count
    if not pd.isna(dist_nearest):
        if dist_nearest <= 5:
            assert n_defenders_within_5m >= 1, "Should have at least 1 defender within 5m since nearest is within 5m"
            print("  [PASS] Count is consistent with nearest distance")

    print("\n[SUCCESS] test_get_defender_distance PASSED\n")

def test_get_closing_speeds():
    
    print("\n" + "="*60)
    print("Testing get_closing_speeds()")
    print("="*60)

    # Need to sort events by match_id and index to find temporal sequences
    events_sorted = events.sort_values(["match_id", "index"]).reset_index(drop=True)

    # Find a pressing event that has a prior event in the same match
    pressing_indices = pressing_events_labeled.index

    # Find first pressing event that's not the first event in its match
    test_event_idx = None
    for idx in pressing_indices:
        event_row = events_sorted.loc[idx]
        # Check if this event has a previous event in the same match
        prev_events = events_sorted[
            (events_sorted["match_id"] == event_row["match_id"]) &
            (events_sorted["index"] < event_row["index"])
        ]
        if len(prev_events) > 0:
            test_event_idx = idx
            break

    if test_event_idx is None:
        print("[SKIP] No pressing event with prior event found")
        return

    event_t0 = events_sorted.loc[test_event_idx]

    # Get the previous event
    prev_events = events_sorted[
        (events_sorted["match_id"] == event_t0["match_id"]) &
        (events_sorted["index"] < event_t0["index"])
    ]
    event_t1 = prev_events.iloc[-1]

    print(f"\nTest Events:")
    print(f"  t=0 Event Type: {event_t0['type'].get('name')}, Index: {event_t0['index']}")
    print(f"  t-1 Event Type: {event_t1['type'].get('name')}, Index: {event_t1['index']}")
    print(f"  Time t0: minute {event_t0['minute']}:{event_t0['second']:.1f}s")
    print(f"  Time t1: minute {event_t1['minute']}:{event_t1['second']:.1f}s")

    # Get closing speeds
    closing_speed_nearest, closing_speed_2nd = get_closing_speeds(event_t0, event_t1, frames)

    print(f"\nClosing Speeds:")
    print(f"  Nearest: {closing_speed_nearest:.2f} m/s")
    print(f"  2nd nearest: {closing_speed_2nd:.2f} m/s")

    # Validation checks
    print(f"\nValidation Checks:")

    # Check 1: Return type and shape
    assert isinstance(closing_speed_nearest, (float, np.floating)), "Nearest speed must be float"
    assert isinstance(closing_speed_2nd, (float, np.floating)), "2nd speed must be float"
    print("  [PASS] Return types are correct (float)")

    # Check 2: Values in plausible range (-15 to +15 m/s for football)
    assert -15 <= closing_speed_nearest <= 15, f"Nearest speed {closing_speed_nearest} out of plausible range"
    assert -15 <= closing_speed_2nd <= 15, f"2nd speed {closing_speed_2nd} out of plausible range"
    print("  [PASS] Values in plausible range (-15 to +15 m/s)")

    # Check 3: Test edge case - event with no freeze frame at t-1
    # Create a non-pressing event (no freeze frame) and test
    non_pressing_event = events_sorted[~events_sorted["id"].isin(frames["event_uuid"].unique())].iloc[0]
    dummy_pressing_event = events_sorted.iloc[0]

    speed_edge, _ = get_closing_speeds(dummy_pressing_event, non_pressing_event, frames)
    assert speed_edge == 0.0, "Should return 0.0 when t-1 has no freeze frame"
    print("  [PASS] Edge case (no t-1 freeze frame) returns 0.0")

    print("\n[SUCCESS] test_get_closing_speeds PASSED\n")
"""
def test_get_voronoi_area():
    print("\n" + "="*60)
    print("Testing get_voronoi_area()")
    print("="*60)

    sample_event = pressing_events_labeled.iloc[14]
    event_id = sample_event["id"]
    event_loc = sample_event["location"]

    print(f"\nTest Event Details:")
    print(f"  Event ID: {event_id}")
    print(f"  Event Type: {sample_event['type'].get('name')}")
    print(f"  Ball-carrier Location: {event_loc}")

    area = get_voronoi_area(event_id, frames)

    print(f"\nVoronoi Area: {area:.2f} sq m" if not pd.isna(area) else "\nVoronoi Area: NaN")

    print(f"\nValidation Checks:")

                                     
    assert isinstance(area, float) or pd.isna(area), "Area must be a float"
    print("  [PASS] Return type is float")

                                   
    if not pd.isna(area):
        assert area >= 0, "Area must be non-negative"
        print("  [PASS] Area is non-negative")

                                                                    
    if not pd.isna(area):
        assert area <= 9600, f"Area {area} exceeds total pitch size"
        print("  [PASS] Area is within pitch bounds (<= 9600 sq m)")

                                                                                         
    if not pd.isna(area):
        assert area < 500, f"Area {area} is implausibly large for a pressing event"
        print("  [PASS] Area is plausibly small for a pressing event (< 500 sq m)")

                                                           
    event_players = frames[frames["event_uuid"] == event_id]
    fake_frames = event_players.iloc[:3].copy()
    area_edge = get_voronoi_area(event_id, fake_frames)
    assert pd.isna(area_edge), "Should return nan when fewer than 4 players available"
    print(f"  [PASS] Edge case (< 4 players) returns nan (tested with {len(fake_frames)} players)")

    print("\n[SUCCESS] test_get_voronoi_area PASSED\n")

def test_get_pitch_zone():
    print("\n" + "="*60)
    print("Testing get_pitch_zone()")
    print("="*60)

    print("\nValidation Checks:")

    assert get_pitch_zone(5, 40) == 0
    assert get_pitch_zone(17.9, 40) == 0
    print("  [PASS] Zone 0 (own box): x < 18")

    assert get_pitch_zone(18, 40) == 1
    assert get_pitch_zone(39.9, 40) == 1
    print("  [PASS] Zone 1 (defensive third): 18 <= x < 40")

    assert get_pitch_zone(60, 10) == 2
    assert get_pitch_zone(40, 26.9) == 2
    print("  [PASS] Zone 2 (left half-space): 40 <= x < 80, y < 27")

    assert get_pitch_zone(60, 70) == 3
    assert get_pitch_zone(79.9, 53.1) == 3
    print("  [PASS] Zone 3 (right half-space): 40 <= x < 80, y > 53")

    assert get_pitch_zone(60, 40) == 4
    assert get_pitch_zone(40, 27) == 4
    print("  [PASS] Zone 4 (central/final third): 40 <= x < 80, 27 <= y <= 53")

    assert get_pitch_zone(80, 40) == 4
    assert get_pitch_zone(102, 40) == 4
    print("  [PASS] Zone 4 (final third): 80 <= x <= 102")

    assert get_pitch_zone(103, 40) == 5
    assert get_pitch_zone(119, 40) == 5
    print("  [PASS] Zone 5 (opp box): x > 102")

    x, y = pressing_events_labeled.iloc[0]["location"]
    zone = get_pitch_zone(x, y)
    assert isinstance(zone, int) and 0 <= zone <= 5, f"Zone {zone} out of range 0-5"
    print(f"  [PASS] Sample event at ({x}, {y}) -> zone {zone}")

    print("\n[SUCCESS] test_get_pitch_zone PASSED\n")


def test_get_pass_lane_density():
    print("\n" + "="*60)
    print("Testing get_pass_lane_density()")
    print("="*60)

    sample_event = pressing_events_labeled.iloc[0]
    event_id = sample_event["id"]
    event_loc = sample_event["location"]

    print(f"\nTest Event: {sample_event['type'].get('name')} at {event_loc}")

    density = get_pass_lane_density(event_id, event_loc, frames)
    print(f"Pass Lane Density: {density:.4f}")

    print("\nValidation Checks:")

    assert isinstance(density, float), "Density must be a float"
    print("  [PASS] Return type is float")

    assert density >= 0, "Density must be non-negative"
    print("  [PASS] Density is non-negative")

    assert density <= 11, f"Density {density} implausibly high (max 11 opponents)"
    print("  [PASS] Density within plausible range (<= 11)")

    event_players = frames[frames["event_uuid"] == event_id]
    teammates_only = event_players[event_players["teammate"] == True].copy()
    density_edge = get_pass_lane_density(event_id, event_loc, teammates_only)
    assert density_edge == 0.0, "Should return 0.0 when no opponents present"
    print("  [PASS] Edge case (no opponents) returns 0.0")

    densities = [get_pass_lane_density(ev["id"], ev["location"], frames)
                 for _, ev in pressing_events_labeled.iloc[:20].iterrows()]
    assert len(set(densities)) > 1, "Density should vary across different events"
    print(f"  [PASS] Values vary across 20 events: min={min(densities):.2f}, max={max(densities):.2f}")

    print("\n[SUCCESS] test_get_pass_lane_density PASSED\n")


def test_build_feature_vector():
    print("\n" + "="*60)
    print("Testing build_feature_vector()")
    print("="*60)

    events_sorted = events.sort_values(["match_id", "index"]).reset_index(drop=True)

    sample_event = None
    prev_event = None
    for _, ev in pressing_events_labeled.iterrows():
        prior = events_sorted[
            (events_sorted["match_id"] == ev["match_id"]) &
            (events_sorted["index"] < ev["index"])
        ]
        if len(prior) > 0:
            sample_event = ev
            prev_event = prior.iloc[-1]
            break

    print(f"\nTest Event: {sample_event['type'].get('name')} at {sample_event['location']}")
    print(f"Prior Event: {prev_event['type'].get('name')}")

    vec = build_feature_vector(sample_event, prev_event, frames)
    print(f"\nFeature vector: {vec}")

    print("\nValidation Checks:")

    assert vec.shape == (15,), f"Expected shape (15,), got {vec.shape}"
    print("  [PASS] Shape is (15,)")

    assert vec.dtype == np.float32, f"Expected float32, got {vec.dtype}"
    print("  [PASS] dtype is float32")

    assert not np.any(np.isnan(vec)), "Feature vector contains NaN"
    assert not np.any(np.isinf(vec)), "Feature vector contains Inf"
    print("  [PASS] No NaN or Inf values")

    assert 0.0 <= vec[5] <= 1.0 and 0.0 <= vec[6] <= 1.0
    print(f"  [PASS] Normalised position in [0,1]: x={vec[5]:.3f}, y={vec[6]:.3f}")

    assert 0.0 <= vec[8] <= 5.0
    print(f"  [PASS] Pitch zone in [0,5]: {vec[8]:.0f}")

    assert 0.0 <= vec[12] <= 4.0
    print(f"  [PASS] Action type in [0,4]: {vec[12]:.0f}")

    assert -1.0 <= vec[13] <= 1.0 and -1.0 <= vec[14] <= 1.0
    print(f"  [PASS] Orientation sin/cos in [-1,1]: sin={vec[13]:.3f}, cos={vec[14]:.3f}")

    assert vec[0] >= 0.0 and vec[1] >= 0.0 and vec[2] >= 0.0
    print(f"  [PASS] Defender distances non-negative: {vec[0]:.2f}, {vec[1]:.2f}, {vec[2]:.2f}")

    vec_no_prior = build_feature_vector(sample_event, None, frames)
    assert vec_no_prior[3] == 0.0 and vec_no_prior[4] == 0.0 and vec_no_prior[9] == 0.0
    print("  [PASS] No prior event -> closing speeds and time_since are 0.0")

    print("\n[SUCCESS] test_build_feature_vector PASSED\n")


test_build_feature_vector()
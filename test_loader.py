from src.ingestion.loader import load_processed
from src.transformation.engineer import (filter_pressing_events, get_label, assign_labels, get_defender_distance, get_closing_speeds, get_voronoi_area)
import pandas as pd
import numpy as np

PROCESSED_DIR = "data/processed"

events, frames, lineups = load_processed(PROCESSED_DIR)

print(f"Events:  {events.shape}")
print(f"Frames:  {frames.shape}")
print(f"Lineups: {lineups.shape}")

#print("\nSaving to parquet...")
#save_processed(events, frames, lineups, PROCESSED_DIR)
#print("Done.")

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

    # Check 1: Area is a float or nan
    assert isinstance(area, float) or pd.isna(area), "Area must be a float"
    print("  [PASS] Return type is float")

    # Check 2: Area is non-negative
    if not pd.isna(area):
        assert area >= 0, "Area must be non-negative"
        print("  [PASS] Area is non-negative")

    # Check 3: Area is within pitch bounds (0 to 9600 sq m = 120*80)
    if not pd.isna(area):
        assert area <= 9600, f"Area {area} exceeds total pitch size"
        print("  [PASS] Area is within pitch bounds (<= 9600 sq m)")

    # Check 4: Area is plausible — ball-carrier under pressure unlikely to own > 500 sq m
    if not pd.isna(area):
        assert area < 500, f"Area {area} is implausibly large for a pressing event"
        print("  [PASS] Area is plausibly small for a pressing event (< 500 sq m)")

    # Check 5: Edge case — fewer than 4 players returns nan
    # Take only 3 rows from the actual event's players so the event_uuid filter still matches
    event_players = frames[frames["event_uuid"] == event_id]
    fake_frames = event_players.iloc[:3].copy()
    area_edge = get_voronoi_area(event_id, fake_frames)
    assert pd.isna(area_edge), "Should return nan when fewer than 4 players available"
    print(f"  [PASS] Edge case (< 4 players) returns nan (tested with {len(fake_frames)} players)")

    print("\n[SUCCESS] test_get_voronoi_area PASSED\n")

#test_get_defender_distance()
#test_get_closing_speeds()
test_get_voronoi_area()
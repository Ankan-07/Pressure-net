import os
import sys
import time
import numpy as np

                                                                                    
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion.loader import load_processed
from src.transformation.engineer import (
    filter_pressing_events,
    assign_labels,
    build_all_features,
)


PROCESSED_DIR = "data/processed"
OUTPUT_PATH = "data/features/features.parquet"


def main():
    overall_start = time.time()

    print("=" * 60)
    print("PressureNet — feature build pipeline")
    print("=" * 60)

                                     
    t = time.time()
    events, frames, lineups = load_processed(PROCESSED_DIR)
    load_time = time.time() - t
    print(f"\n[1/3] Loaded processed data in {load_time:.1f}s")
    print(f"      events:  {events.shape}")
    print(f"      frames:  {frames.shape}")
    print(f"      lineups: {lineups.shape}")

                                                    
    t = time.time()
    pressing_events = filter_pressing_events(events, frames)
    pressing_events_labeled = assign_labels(pressing_events)
    filter_time = time.time() - t
    print(f"\n[2/3] Filtered + labeled pressing events in {filter_time:.1f}s")
    print(f"      pressing events: {len(pressing_events_labeled)}")
    print(f"      label distribution:\n{pressing_events_labeled['label'].value_counts().to_string()}")

                              
    print(f"\n[3/3] Building (3,15) feature windows for all pressing events...")
    build_start = time.time()
    df_out = build_all_features(
        pressing_events_labeled, events, frames,
        output_path=OUTPUT_PATH,
    )
    build_time = time.time() - build_start

             
    total_time = time.time() - overall_start
    n = len(df_out)
    feat_cols = [c for c in df_out.columns if c.startswith("feat_")]
    feat_values = df_out[feat_cols].values

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Output:           {OUTPUT_PATH}")
    print(f"File size:        {os.path.getsize(OUTPUT_PATH) / 1e6:.1f} MB")
    print(f"Rows:             {n}")
    print(f"Columns:          {df_out.shape[1]}")
    print(f"Padded t-2:       {int(df_out['mask_t0'].sum())} ({df_out['mask_t0'].mean() * 100:.1f}%)")
    print(f"Padded t-1:       {int(df_out['mask_t1'].sum())} ({df_out['mask_t1'].mean() * 100:.1f}%)")
    print(f"Padded t=0:       {int(df_out['mask_t2'].sum())} (must be 0)")
    print(f"NaN in features:  {int(np.isnan(feat_values).sum())}")
    print(f"Inf in features:  {int(np.isinf(feat_values).sum())}")
    print(f"Label dist:\n{df_out['label'].value_counts().to_string()}")
    print()
    print("Timing:")
    print(f"  Load processed:           {load_time:6.1f}s")
    print(f"  Filter + label:           {filter_time:6.1f}s")
    print(f"  Build windows (total):    {build_time:6.1f}s  ({build_time / 60:.1f} min)")
    print(f"  Per-event avg:            {build_time / n * 1000:6.1f}ms")
    print(f"  Wall-clock total:         {total_time:6.1f}s  ({total_time / 60:.1f} min)")


if __name__ == "__main__":
    main()

from src.ingestion.loader import (load_events, load_frames_exploaded, 
                                   load_lineups, save_processed)
from src.transformation.engineer import (filter_pressing_events, get_label, assign_labels)
import pandas as pd

DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

events = load_events(DATA_DIR)
frames = load_frames_exploaded(DATA_DIR)
lineups = load_lineups(DATA_DIR)

print(f"Events:  {events.shape}")
print(f"Frames:  {frames.shape}")
print(f"Lineups: {lineups.shape}")

print("\nSaving to parquet...")
save_processed(events, frames, lineups, PROCESSED_DIR)
print("Done.")

pressing_events = filter_pressing_events(events,frames)
pressing_events_labeled = assign_labels(pressing_events)
print(pressing_events_labeled['label'].value_counts())
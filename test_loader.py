from src.ingestion.loader import (load_events, load_frames_exploaded, 
                                   load_lineups, save_processed)

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
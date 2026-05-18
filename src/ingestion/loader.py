import json
import os
import sys
import pandas as pd 
from src.exception import CustomException
from src.logger import logging
import pyarrow

def load_events(data_dir):
    all_events = []
    events_dir = os.path.join(data_dir, "events")

    for filename in os.listdir(events_dir):
        if not filename.endswith(".json"):
            continue

        match_id = filename.replace(".json", "")
        file_path = os.path.join(events_dir, filename)

        with open(file_path, encoding="utf-8") as f:
            events = json.load(f)
        
        for event in events:
            event["match_id"] = match_id

        all_events.extend(events)


    return pd.DataFrame(all_events)

def load_frames(data_dir):
    all_frames = []
    frames_dir = os.path.join(data_dir, "three-sixty")

    for filename in os.listdir(frames_dir):
        if not filename.endswith(".json"):
            continue

        match_id = filename.replace(".json", "")
        file_path = os.path.join(frames_dir, filename)

        with open(file_path, encoding="utf-8") as f:
            frames = json.load(f)
        
        for frame in frames:
            frame["match_id"] = match_id

        all_frames.extend(frames)

    return pd.DataFrame(all_frames)

def load_lineups(data_dir):
    all_lineups = []
    lineup_dir = os.path.join(data_dir,"lineups")

    for filename in os.listdir(lineup_dir):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(lineup_dir, filename)
        match_id = filename.replace(".json", "")

        with open(file_path, encoding="utf-8") as f:
            lineups = json.load(f)
        
        for team in lineups:
            for player in team.get("lineup", []):
                player["team_id"] = team["team_id"]
                player["team_name"] = team["team_name"]
                player["match_id"] = match_id

            all_lineups.extend(team.get("lineup", []))

    return pd.DataFrame(all_lineups)

def load_frames_exploaded(data_dir):
    try:
        '''Load 360 frames with one row per player per event.'''
        frames = load_frames(data_dir)

                            
        frames = frames.explode("freeze_frame").reset_index(drop=True)

                                            
        player_data = pd.json_normalize(frames["freeze_frame"])

                                                    
        frames = frames[["event_uuid", "match_id"]].reset_index(drop=True)
        frames = pd.concat([frames, player_data], axis=1)

        frames[["loc_x", "loc_y"]] = pd.DataFrame(frames["location"].tolist(), index=frames.index)
        frames = frames.drop(columns=["location"])

        return frames
    
    except Exception as e:
        raise CustomException(e, sys)

def save_processed(events, frames, lineups, output_dir):
    try:
        os.makedirs(output_dir, exist_ok=True)

        events.to_parquet(os.path.join(output_dir, "events.parquet"), index = False)
        frames.to_parquet(os.path.join(output_dir, "frames.parquet"), index = False)
        lineups.to_parquet(os.path.join(output_dir, "lineups.parquet"), index = False)

        logging.info(f"Parquet files are saved to {output_dir}")

    except Exception as e:
        raise CustomException(e,sys)

def load_processed(output_dir):
    try:
        events = pd.read_parquet(os.path.join(output_dir, "events.parquet"))
        frames = pd.read_parquet(os.path.join(output_dir, "frames.parquet"))
        lineups = pd.read_parquet(os.path.join(output_dir, "lineups.parquet"))
        
        return events, frames, lineups
    except Exception as e:
        raise CustomException(e,sys)

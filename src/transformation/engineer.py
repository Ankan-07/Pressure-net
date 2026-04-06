import os
import sys
from logger import logging
from exception import CustomException
import pandas as pd

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

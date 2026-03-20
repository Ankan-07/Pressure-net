import os
import sys
from src.exception import CustomException
import urllib.request
import json
import time

source_path = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

competitions = [
    (55, 282, "EURO 2024"),
    (55,  43, "EURO 2020"),
    (43, 106, "World Cup 2022"),
]

data_dir = "data/raw"

def download_file(source, save_path):
    if os.path.exists(save_path):
        return

    os.makedirs(os.path.dirname(save_path), exist_ok = True)

    try:
        urllib.request.urlretrieve(source,save_path)
    except Exception as e:
        raise CustomException(e,sys)

def get_match_ids(competition_id, season_id):
    """Download the matches file and extract all match IDs."""
    matches_url = f"{source_path}/matches/{competition_id}/{season_id}.json"
    matches_path = f"{data_dir}/matches/{competition_id}_{season_id}.json"

    download_file(matches_url, matches_path)

    with open(matches_path, "r", encoding="utf-8") as f:
        matches = json.load(f)

    return [m["match_id"] for m in matches]

def main():
    os.makedirs(data_dir, exist_ok=True)

    for comp_id, season_id, name in competitions:
        print(f"\n{'='*50}")
        print(f"  {name} (competition={comp_id}, season={season_id})")
        print(f"{'='*50}")

        match_ids = get_match_ids(comp_id, season_id)
        print(f"  Found {len(match_ids)} matches")

        for i, match_id in enumerate(match_ids):
            print(f"  [{i+1}/{len(match_ids)}] match {match_id}", end="  ")

            # Events file
            download_file(
                f"{source_path}/events/{match_id}.json",
                f"{data_dir}/events/{match_id}.json"
            )
            print("events ✓", end="  ")

            # 360 freeze frames
            download_file(
                f"{source_path}/three-sixty/{match_id}.json",
                f"{data_dir}/three-sixty/{match_id}.json"
            )
            print("360 ✓", end="  ")

            # Lineups
            download_file(
                f"{source_path}/lineups/{match_id}.json",
                f"{data_dir}/lineups/{match_id}.json"
            )
            print("lineups ✓")

    print("\n\nDone! All files downloaded.")


if __name__ == "__main__":
    main()

import json
import numpy as np
import sounderpy as spy

from tqdm import tqdm
from glob import glob
from datetime import datetime
from pint import Quantity
from pathlib import Path


EVENTS_DIR = "data/events"
STATION    = "VEF"


def to_jsonable(obj):
    """Recursively convert pint.Quantity, numpy types, and datetimes to JSON-safe values."""
    if isinstance(obj, Quantity):
        mag = obj.magnitude
        if isinstance(mag, np.ndarray):
            mag = mag.tolist()
        return {"value": mag, "unit": str(obj.units)}
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.floating, np.integer)):
        return obj.item()
    if isinstance(obj, (np.datetime64,)):
        return np.datetime_as_string(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


def main():
    
    all_events = glob(f"{EVENTS_DIR}/*")
    
    for edir in tqdm(all_events, total=len(all_events)):

        try:
            dt_str   = edir.split("/")[-1]
            yyyymmdd = dt_str.split(" ")[0]
            year     = int(yyyymmdd[:4])
            month    = int(yyyymmdd[5:7])
            day      = int(yyyymmdd[8:])
        except:
            continue

        # grab 0z sounding
        out_fp = str(Path(edir) / Path(f"{dt_str}_VEF_0Z_sounding.json"))
        if not Path(out_fp).is_file():
            try:
                clean_data = spy.get_obs_data(STATION, year, month, day, 0)
                with open(out_fp, "w") as f:
                    json.dump(to_jsonable(clean_data), f, indent=2)
            except:
                print(f"Error: could not download sounding to {out_fp}")

        # grab 12z sounding
        out_fp = str(Path(edir) / Path(f"{dt_str}_VEF_12Z_sounding.json"))
        if not Path(out_fp).is_file():
            try:
                clean_data = spy.get_obs_data(STATION, year, month, day, 12)
                with open(out_fp, "w") as f:
                    json.dump(to_jsonable(clean_data), f, indent=2)
            except:
                print(f"Error: could not download sounding to {out_fp}")


if __name__ == "__main__":
    main()
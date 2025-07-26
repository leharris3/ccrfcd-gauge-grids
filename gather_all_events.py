import os
from pathlib import Path
import xarray

from glob import glob
from tqdm import tqdm
from datetime import datetime, timedelta
from src.stats.mrms_ccrfcd_stats_client import StatsClient, MRMSProductsEnum
from src.mrms_qpe.fetch_mrms_qpe import MRMSQPEClient

TEMP_DIR   = "__temp"
EVENTS_DIR = "data/events"

JUNE = 6
SEPTEMBER = 8
DATERANGE = [datetime(2021, 5, 20), datetime(2025, 7, 25)]
MONSOON_MONTHS = [6, 7, 8]
JUNE_DAYS = [25, 26, 27, 28, 29, 30, 31]
SEP_DAYS = [1, 2, 3, 4, 5, 6, 7]

MIN_PRECIP_THRESH = 0.25
LAT_MIN = 35.8
LAT_MAX = 36.4
LON_MIN = -115.4
LON_MAX = -114.8


stats_client = StatsClient()
mrms_qpe_client = MRMSQPEClient()


def is_valid_date(dt: datetime) -> bool:
    
    if dt.month not in MONSOON_MONTHS: return False
    elif dt.month == JUNE:
        if dt.day not in JUNE_DAYS: return False
    elif dt.month == SEPTEMBER: 
        if dt.day not in SEP_DAYS: return False
    
    return True


def is_min_rain_day(dt: datetime) -> bool:

    next_day = dt + timedelta(days=1)
    xarr: xarray.Dataset = mrms_qpe_client.fetch_radar_only_qpe_24hr(next_day)
    
    # no MRMS file @path
    if xarr == None: return False
    
    qpe = xarr.unknown.sel(
        latitude=slice(LAT_MAX, LAT_MIN),
        longitude=slice(LON_MIN + 360, LON_MAX + 360)
    )
    
    # mm -> inch
    qpe_in = qpe / 25.44

    return qpe_in.max() > MIN_PRECIP_THRESH


def process_day(start_time: datetime) -> None:

    end_time = start_time + timedelta(hours=1)
    event_out_dir = Path(EVENTS_DIR) / Path(str(start_time))
    ccrfcd_gauge_deltas_fp = event_out_dir / Path(f"ccrfcd_gauge_deltas_{str(start_time)}.csv")
    os.makedirs(event_out_dir)

    df = stats_client.fetch_stats_for_range(
        start_time,
        end_time,
        MRMSProductsEnum.RadarOnly_QPE_01H,
        timezone="UTC",
        timedelta_interval=timedelta(minutes=2)
    )
    df.to_csv(str(ccrfcd_gauge_deltas_fp))


def main():

    curr_day   = DATERANGE[0]
    last_day   = DATERANGE[-1]
    total_days = (last_day - curr_day).days
    
    with tqdm(total=total_days, desc="Processing Days") as pbar:

        while curr_day < last_day:
            
            if not is_valid_date(curr_day): 
                curr_day += timedelta(days=1)
                pbar.update(1)
                continue

            # determine if CC exceeded >= 0.25 in. precip.
            # in a 24H period (as measured by MRMS-QPE)
            if not is_min_rain_day(curr_day):
                curr_day += timedelta(days=1)
                pbar.update(1)
                continue

            process_day(curr_day)

            # clean up __temp dir
            temp_files = [f for f in glob(f"{TEMP_DIR}/*")]
            for fp in temp_files:
                os.remove(fp)

            curr_day += timedelta(days=1)
            pbar.update(1)


if __name__ == "__main__":
    main()
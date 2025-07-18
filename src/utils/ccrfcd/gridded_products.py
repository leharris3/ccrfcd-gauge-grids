"""
# TODO:
---
We want to create 1x1km gridded products for CCRFCD rain gauges equivalent to
    1. MRMS_RadarOnly_QPE_1H
    2. MRMS_RadarOnly_QPE_3H

Also, want to create some plotting functionality to display gif-style loops.

A few problems to consider:
    1. MRMS temporal resolution is 2 min, CCRFCD data are 5 minutes
        - May consider creating a 10 minute gridded product
    2. What will be the dimensions of the actual gridded product?

keys:
    1. time
    2. lat/lon*
queries:
    1. [M, N] 2D array with ``mm`` accumulated precipitation values
"""

import logging
import numpy as np
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta


class Location:

    def __init__(self, lat: float, lon: float):
        self.lat = lat
        self.lon = lon


class CCRFCDGriddedProducts:

    _METADATA_FP    = "data/clark-county-rain-gauges/ccrfcd_rain_gauge_metadata.csv"
    _GAUGE_DATA_DIR = "data/clark-county-rain-gauges/2021-"

    # state of nevada
    _LAT_MIN = 34.751857
    _LAT_MAX = 37.103662
    _LON_MIN = -116.146925
    _LON_MAX = -113.792819

    # 0.1° ~ 10 km?
    _DLAT = _DLON = 0.045

    # # clark county
    # _LAT_MIN = 36.0
    # _LAT_MAX = 36.5
    # _LON_MIN = -115.2
    # _LON_MAX = -114.7

    # # 0.01° ~ 1km
    # _DLAT = _DLON = 0.01

    def __init__(self, ):

        self.metadata                            = pd.read_csv(CCRFCDGriddedProducts._METADATA_FP)
        self.valid_station_ids                   = self.metadata[self.metadata['station_id'] > 0]['station_id'].astype(int).tolist()
        self.data_cache: Dict[int, pd.DataFrame] = {}

    def _get_gauge_df(self, gauge_id) -> pd.DataFrame | None:

        if gauge_id in self.data_cache:
            return self.data_cache[gauge_id]
        
        fp = Path(self._GAUGE_DATA_DIR) / f"gagedata_{gauge_id}.csv"
        if not fp.is_file():
            return None
        
        df = pd.read_csv(fp)
        df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.set_index('datetime', inplace=True)
        self.data_cache[gauge_id] = df

        return df

    def _fetch_gauge_qpe(self, gauge_id: int, start_time: datetime, end_time: datetime) -> Tuple[Location, float]:
        """
        Returns
        --- 
        - Cumlative precipitation (QPE) for a clark county rain gauge between ``start_time`` and ``end_time``.
        """

        assert start_time < end_time, f"Error: expected `start_time` < `end_time`"

        df = self._get_gauge_df(gauge_id)
        if df is None:
            return (None, None)

        # HACK: df is sorted present -> past, delta values are negative
        if 'delta' not in df.columns:
            df['delta'] = df['Value'].diff().fillna(0) * -1  # invert sign once

        # cumulative precip
        cum_precip = None

        # grab gauge location
        location_row = self.metadata[self.metadata['station_id'] == gauge_id]
        assert len(location_row) == 1, f"Error: no metadata available for `gauge_id`: {gauge_id}"
        row          = location_row.iloc[0]
        location     = Location(lat=float(row.lat), lon=float(row.lon))

        # # find `start_time` and `end_time` rows
        # # date/time columns are sorted
        # closest_start_time  = None
        # closest_start_idx   = None
        # closest_end_time    = None
        # closest_end_idx     = None

        # O(N)
        # for idx, row in enumerate(df.iterrows()):

        #     _date         = row[1].Date
        #     _time         = row[1].Time
        #     _datetime_str = f"{_date} {_time}"
        #     _datetime     = datetime.strptime(_datetime_str, '%m/%d/%Y %H:%M:%S')

        #     # find the closest `_datetime` to `start_time`
        #     if closest_start_time == None:
        #         closest_start_time  = _datetime
        #         closest_start_idx   = idx 
        #     else:
        #         if abs(start_time - _datetime) < abs(start_time - closest_start_time):
        #             closest_start_time  = _datetime
        #             closest_start_idx   = idx

        #     # find the closest `_datetime` to `end_time`
        #     if closest_end_time == None:
        #         closest_end_time  = _datetime
        #         closest_end_idx   = idx
        #     else:
        #         if abs(end_time - _datetime) < abs(end_time - closest_end_time):
        #             closest_end_time  = _datetime
        #             closest_end_idx   = idx

        # sum delta_precip to get the total precip over a window of time
        # if closest_start_time != closest_end_time:
        #     cum_precip = df[closest_end_idx: closest_start_idx]['delta'].sum()
        
        # return (location, cum_precip)

        # get [start, end] bounds
        start_idx = df.index.get_indexer([start_time], method='nearest')[0]
        end_idx   = df.index.get_indexer([end_time],   method='nearest')[0]
        cum_precip = df.iloc[end_idx:start_idx+1]['delta'].sum()

        return location, float(cum_precip)

    def _grid_all_gauge_qpe(self, gauge_qpes: List[Tuple[Location, float]]) -> List[List[float]]: 
        """
        Convert a list of (Location, precip: float) values into a 2D grid of precipitation values (in.)
        # TODO: how do we map MRMS data and our rain gauge data to the same 2D cartesian grid?
        """

        def _latlon_to_idx(lat, lon) -> Tuple[int, int]:
            """
            Return (i, j) indices on grid.
            """
            i = int((lat - self._LAT_MIN) / self._DLAT)
            j = int((lon - self._LON_MIN) / self._DLON)
            return i, j

        # 1-km grid covering the CCRFCD domain
        lat_bins = np.arange(self._LAT_MIN, self._LAT_MAX + self._DLAT, self._DLAT)
        lon_bins = np.arange(self._LON_MIN, self._LON_MAX + self._DLON, self._DLON)

        grid_sum = np.full((len(lat_bins), len(lon_bins)), 0.0)
        grid_cnt = np.zeros_like(grid_sum)

        for loc, precip in gauge_qpes:
            
            # skip bad gauges
            if precip is None:
                continue
            
            i, j = _latlon_to_idx(loc.lat, loc.lon)
            
            if 0 <= i < grid_sum.shape[0] and 0 <= j < grid_sum.shape[1]:
                grid_sum[i, j] += float(precip)
                grid_cnt[i, j] += 1

        with np.errstate(invalid="ignore"):
            grid_mean = grid_sum / grid_cnt

        return grid_mean

    def _fetch_all_gauge_qpe(self, start_time: datetime, end_time: datetime) -> List[Tuple[Location, float]]:
        """
        Gather all cummulative precipitation values (in.) between bounds of [start_time, end_time]; inclusive.
        # TODO: make less slow...
        """
        
        # order doesn't matter, we should parallelize
        all_gauge_qpe = []

        for _id in tqdm(self.valid_station_ids, total=len(self.valid_station_ids)):

            try:
                res: Tuple[Optional[Location], Optional[float]] = self._fetch_gauge_qpe(_id, start_time, end_time)
            except Exception as e:
                print(e)
                print(f"Error fetching gauge id: {_id}")
                continue

            # remove invalid results
            # TODO: clarify this >>
            if res[0] == None or res[1] == None: continue
            
            all_gauge_qpe.append(res)

        return all_gauge_qpe

    def _fetch_ccrfcd_qpe_xhr(self, start_time, delta: int) -> np.ndarray:
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        end_time  = start_time + timedelta(hours=delta)
        pts       = self._fetch_all_gauge_qpe(start_time, end_time)
        grid_mean = self._grid_all_gauge_qpe(pts)
        return grid_mean

    def fetch_ccrfcd_qpe_1hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=1)

    def fetch_ccrfcd_qpe_3hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=3)

    def fetch_ccrfcd_qpe_6hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=6)

    def fetch_ccrfcd_qpe_12hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=12)

    def fetch_ccrfcd_qpe_24hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=24)

    def fetch_ccrfcd_qpe_48hr(self, start_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(start_time, delta=48)

if __name__ == "__main__": 

    t1 = datetime(year=2024, month=7, day=14, hour=6)
    t2 = datetime(year=2024, month=7, day=14, hour=12)
    obj = CCRFCDGriddedProducts()
    gauge_qpes = obj.fetch_ccrfcd_qpe_1hr(t1)

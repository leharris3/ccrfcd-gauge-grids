import xarray
import warnings
import numpy as np
import pandas as pd

from tqdm import tqdm
from typing import List
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed

from src.utils.mrms.products import MRMSProductsEnum
from src.utils.ccrfcd.ccrfcd_client import CCRFCDClient
from src.mrms_qpe.fetch_mrms_qpe import MRMSQPEClient


warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=".*decode_timedelta will default to False.*"
)


class StatsClient:
    
    def __init__(self):
        
        self.ccrfcd_client = CCRFCDClient()
        self.mrms_client   = MRMSQPEClient()

    def _get_gauge_mrms_deltas(self, gpe_raw_vals: List[dict], xarr: xarray.Dataset) -> List[dict]:
        
        lats        = [item["lat"] for item in gpe_raw_vals]
        lons        = [item["lon"] for item in gpe_raw_vals]
        station_ids = [item["station_id"] for item in gpe_raw_vals]
        qpes        = [item["qpe"] for item in gpe_raw_vals]

        # slice to region to save time during search
        _xarr = xarr.sel(
            latitude =slice(self.ccrfcd_client._LAT_MAX, self.ccrfcd_client._LAT_MIN),
            longitude=slice(self.ccrfcd_client._LON_MIN + 360, self.ccrfcd_client._LON_MAX + 360)
        )

        grid_lats = _xarr['latitude'].values
        grid_lons = _xarr['longitude'].values

        # mm -> inch
        qpe_values = _xarr['unknown'].values / 25.4

        lats = np.array(lats)
        lons = np.array(lons)

        lat_indices = np.abs(grid_lats[:, None] - lats).argmin(axis=0)
        lon_indices = np.abs(grid_lons[:, None] - lons).argmin(axis=0)

        # get closest MRMS grid cell; read QPE value
        deltas = []
        for i, station_id in enumerate(station_ids):

            gauge_qpe = qpes[i]
            mrms_qpe  = qpe_values[lat_indices[i], lon_indices[i]]
            delta_qpe = gauge_qpe - float(mrms_qpe)

            # print(f"station id: {station_id} | delta: {delta_qpe}")

            deltas.append({
                "station_id": station_id,
                "mrms_qpe": mrms_qpe,
                "gauge_qpe": gauge_qpe,
                "delta_qpe": delta_qpe,
                "lat": lats[i],
                "lon": lons[i],
            })

        return deltas

    def _proc_gauge(self, xarr: xarray.Dataset) -> List[dict]:
        
        # get start_time from xarr
        secs = xarr.time.values.astype('datetime64[s]').astype('int64')

        # HACK:
        mrms_end_time = datetime.utcfromtimestamp(secs)
        mrms_start_time = mrms_end_time - timedelta(hours=1)

        # grab rain-gauge qpe
        gauge_qpes    = self.ccrfcd_client._fetch_all_gauge_qpe(mrms_start_time, mrms_end_time, disable_tqdm=True)

        deltas = self._get_gauge_mrms_deltas(gauge_qpes, xarr)
        return deltas, mrms_start_time, mrms_end_time

    def fetch_stats_for_range(
            self, 
            start_time: datetime, 
            end_time: datetime, 
            mrms_product: MRMSProductsEnum, 
            timezone: str = "UTC",
            timedelta_interval: timedelta = None,
            fetch_full_day: bool = False
        ) -> pd.DataFrame: 
        """
        **Timezone**: ``UTC``
        TODO: rewrite to ONLY support batch proc.; this func is in shambles
        """

        assert start_time < end_time, f"Error: `start_time` >= `end_time`"

        suffix = mrms_product.split("_")[-2]
        if suffix == "15M":
            raise NotImplementedError(f"Error: invalid product: {mrms_product}")
        elif suffix == "01H":
            step         = timedelta(hours=1)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_1hr
            if fetch_full_day:
                mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_full_day_1hr
        elif suffix == "03H":
            step         = timedelta(hours=3)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_3hr
        elif suffix == "6H":
            step         = timedelta(hours=6)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_6hr
        elif suffix == "12H":
            step         = timedelta(hours=12)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_12hr
        elif suffix == "24H":
            step         = timedelta(hours=24)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_24hr
        elif suffix == "48H":
            raise NotImplementedError(f"Error: invalid product: {mrms_product}")
        else: 
            raise NotImplementedError(f"Error: invalid product: {mrms_product}")
        
        if timedelta_interval != None:
            step = timedelta_interval

        df_dict = {
            "start_time": [],
            "end_time": [],
            "station_id": [],
            "lat": [],
            "lon": [],
            "gauge_qpe": [],
            "mrms_qpe": [],
            "delta_qpe": [],
        }

        mrms_qpe_xarrs = mrms_fetch_f(end_time, del_tmps=False)

        # HACK:

        with tqdm(total=len(mrms_qpe_xarrs), desc="Fetching stats.") as pbar:
            with ProcessPoolExecutor() as ex:
                futures = {ex.submit(self._proc_gauge, xarr): xarr for xarr in mrms_qpe_xarrs}
                for future in as_completed(futures):   
                    deltas, curr_start_time, next_time_ccrfcd = future.result()
                    for item in deltas:
                        df_dict['start_time'].append(str(curr_start_time))
                        df_dict['end_time'].append(str(next_time_ccrfcd))
                        df_dict['station_id'].append(item['station_id'])
                        df_dict['lat'].append(float(item['lat']))
                        df_dict['lon'].append(float(item['lon']))
                        df_dict['gauge_qpe'].append(float(item['gauge_qpe']))
                        df_dict['mrms_qpe'].append(float(item['mrms_qpe']))
                        df_dict['delta_qpe'].append(float(item['delta_qpe']))
                    pbar.update()

        # TODO: parallelize
        # for xarr in tqdm(mrms_qpe_xarrs, total=len(mrms_qpe_xarrs), desc="Fetching stats: "):
            
        #     # get start_time from xarr
        #     secs = xarr.time.values.astype('datetime64[s]').astype('int64')
        #     curr_start_time = datetime.utcfromtimestamp(secs)

        #     # HACK:
        #     next_time_ccrfcd = curr_start_time + timedelta(hours=1)

        #     # grab rain-gauge qpe
        #     gauge_qpes    = self.ccrfcd_client._fetch_all_gauge_qpe(curr_start_time, next_time_ccrfcd, disable_tqdm=True)

        #     deltas = self._get_gauge_mrms_deltas(gauge_qpes, xarr)

        #     for item in deltas:

        #         df_dict['start_time'].append(str(curr_start_time))
        #         df_dict['end_time'].append(str(next_time_ccrfcd))
        #         df_dict['station_id'].append(item['station_id'])
        #         df_dict['lat'].append(float(item['lat']))
        #         df_dict['lon'].append(float(item['lon']))
        #         df_dict['gauge_qpe'].append(float(item['gauge_qpe']))
        #         df_dict['mrms_qpe'].append(float(item['mrms_qpe']))
        #         df_dict['delta_qpe'].append(float(item['delta_qpe']))

        # # TODO: optimize
        # with tqdm(total=total_steps, desc="Fetching stats...") as pbar:

        #     while next_time <= end_time:

        #         # HACK
        #         next_time_ccrfcd = curr_start_time + timedelta(hours=1)

        #         # grab rain-gauge qpe
        #         gauge_qpes    = self.ccrfcd_client._fetch_all_gauge_qpe(curr_start_time, next_time_ccrfcd, disable_tqdm=True)
                
        #         # TODO: optimize
        #         # we download a seperate MRMS grib2 file for each 2-min interval at each step
        #         # could we save time here...
        #         # ... 1. bulk downloads
        #         # ... 2. view/peak MRMS data instead of downloading/unzipping/etc
        #         mrms_qpe_xarr = mrms_fetch_f(next_time, time_zone=timezone)

        #         deltas = self._get_gauge_mrms_deltas(gauge_qpes, mrms_qpe_xarr)

        #         for item in deltas:

        #             df_dict['start_time'].append(str(curr_start_time))
        #             df_dict['end_time'].append(str(next_time))
        #             df_dict['station_id'].append(item['station_id'])
        #             df_dict['lat'].append(float(item['lat']))
        #             df_dict['lon'].append(float(item['lon']))
        #             df_dict['gauge_qpe'].append(float(item['gauge_qpe']))
        #             df_dict['mrms_qpe'].append(float(item['mrms_qpe']))
        #             df_dict['delta_qpe'].append(float(item['delta_qpe']))

        #         curr_start_time += step
        #         next_time       += step
        #         pbar.update()

        return pd.DataFrame(df_dict)


if __name__ == "__main__":

    sc = StatsClient()
    t0 = datetime(year=2023, month=8, day=20)
    t1 = datetime(year=2023, month=8, day=21)
    df = sc.fetch_stats_for_range(t0, t1, MRMSProductsEnum.RadarOnly_QPE_01H, fetch_full_day=True)
    breakpoint()


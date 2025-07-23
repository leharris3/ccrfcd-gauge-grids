import xarray
import numpy as np
import pandas as pd

from typing import List
from datetime import datetime, timedelta
from src.utils.mrms.products import MRMSProductsEnum
from src.utils.ccrfcd.gridded_products import CCRFCDClient
from src.mrms_qpe.fetch_mrms_qpe import MRMSQPEClient


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

    def fetch_stats_for_range(
            self, 
            start_time: datetime, 
            end_time: datetime, 
            mrms_product: MRMSProductsEnum, 
            timezone: str = "UTC"
        ) -> pd.DataFrame: 
        """
        **Timezone**: ``UTC``
        """

        assert start_time < end_time, f"Error: `start_time` >= `end_time`"

        suffix = mrms_product.split("_")[-2]
        if suffix == "15M":
            raise NotImplementedError(f"Error: invalid product: {mrms_product}")
        elif suffix == "01H":
            step         = timedelta(hours=1)
            mrms_fetch_f = self.mrms_client.fetch_radar_only_qpe_1hr
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

        curr_start_time = start_time
        next_time = start_time + step

        while next_time <= end_time:

            # grab rain-gauge qpe
            gauge_qpes    = self.ccrfcd_client._fetch_all_gauge_qpe(curr_start_time, next_time)
            
            # grab mrms qpe
            mrms_qpe_xarr = mrms_fetch_f(next_time, time_zone=timezone)

            deltas = self._get_gauge_mrms_deltas(gauge_qpes, mrms_qpe_xarr)

            for item in deltas:

                df_dict['start_time'].append(str(curr_start_time))
                df_dict['end_time'].append(str(next_time))
                df_dict['station_id'].append(item['station_id'])
                df_dict['lat'].append(float(item['lat']))
                df_dict['lon'].append(float(item['lon']))
                df_dict['gauge_qpe'].append(float(item['gauge_qpe']))
                df_dict['mrms_qpe'].append(float(item['mrms_qpe']))
                df_dict['delta_qpe'].append(float(item['delta_qpe']))

            curr_start_time += step
            next_time       += step

        return pd.DataFrame(df_dict)


if __name__ == "__main__":

    sc = StatsClient()
    t0 = datetime(year=2023, month=8, day=21, hour=2)
    t1 = datetime(year=2023, month=8, day=21, hour=10)
    df = sc.fetch_stats_for_range(t0, t1, MRMSProductsEnum.RadarOnly_QPE_01H)
    breakpoint()


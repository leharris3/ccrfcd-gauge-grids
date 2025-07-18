import os
import xarray as xr

from typing import List
from datetime import datetime
from bisect import bisect_left, bisect_right

from src.utils.mrms.files import ZippedGrib2File, Grib2File
from src.utils.mrms.mrms import MRMSDomain, MRMSPath
from src.utils.mrms.mrms import MRMSAWSS3Client
from src.utils.mrms.products import MRMSProductsEnum


class MRMSQPEClient:

    def __init__(self):
        self.mrms_client = MRMSAWSS3Client()

    def _get_closest_file(self, paths: List[str], start_time: datetime, mode="nearest") -> str:
        
        if not paths:
            raise ValueError("Received an empty list of paths.")

        # 1. Build and sort (datetime, path) pairs
        dt_path_pairs = sorted(
            ((MRMSPath.from_str(fp).get_base_datetime(), fp) for fp in paths),
            key=lambda t: t[0],
        )
        dts = [p[0] for p in dt_path_pairs]  # just the datetimes
        mode = (mode or "nearest").lower()

        # 2. Choose according to mode
        if mode == "nearest":
            chosen_dt, chosen_fp = min(
                dt_path_pairs, key=lambda t: abs(t[0] - start_time)
            )
            return chosen_fp

        elif mode == "first":
            idx = bisect_right(dts, start_time) - 1
            if idx < 0:
                raise ValueError(
                    "No file time is ≤ start_time; cannot satisfy mode='first'."
                )
            return dt_path_pairs[idx][1]

        elif mode == "next":
            idx = bisect_left(dts, start_time)
            if idx >= len(dts):
                raise ValueError(
                    "No file time is ≥ start_time; cannot satisfy mode='next'."
                )
            return dt_path_pairs[idx][1]

        else:
            raise ValueError(f"Unrecognized mode '{mode}'. "
                             "Choose 'nearest', 'first', or 'next'.")

    def _fetch_radar_only_qpe_x(self, start_time: datetime, product: str, mode="nearest") -> xr.Dataset : 
        """
        Fetch MRMS ``RadarOnly_QPE`` suite of products. 

        Args
        ---
        :start_time: ``datetime``
        :mode: ``str`` 
        - "nearest", "first", or "next"
            - "nearest": find the closest valid file to provide ``datetime``
            - "first"  : closest valid file whos time < start_time
            - "next"   : closest valid file whos time > start_time

        Returns
        ---
        """

        yyyymmdd = start_time.strftime("%Y%m%d")
        basepath = MRMSPath(
            domain   = MRMSDomain.CONUS, 
            product  = product,
            yyyymmdd = yyyymmdd
            )
        
        file_paths   = self.mrms_client.ls(str(basepath))
        nearest_path = self._get_closest_file(file_paths, start_time)
        
        # TODO: del grib2 files after donwnload
        mp = MRMSPath.from_str(nearest_path)
        fp = self.mrms_client.download(str(mp), to="")
        
        zipped_gf = ZippedGrib2File(fp)
        gf        = zipped_gf.unzip(to_dir="")
        xa        = gf.to_xarray()

        return xa

    def fetch_radar_only_qpe_15m(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_15M, mode=mode)
    
    def fetch_radar_only_qpe_1hr(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_01H, mode=mode)

    def fetch_radar_only_qpe_3hr(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_03H, mode=mode)
    
    def fetch_radar_only_qpe_6hr(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_06H, mode=mode)
    
    def fetch_radar_only_qpe_12hr(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_12H, mode=mode)
    
    def fetch_radar_only_qpe_24hr(self, start_time: datetime, mode="nearest"):
        return self._fetch_radar_only_qpe_x(start_time, MRMSProductsEnum.RadarOnly_QPE_24H, mode=mode)


if __name__ == "__main__":
    client = MRMSQPEClient()
    date = datetime.now()
    ar = client.fetch_radar_only_qpe_6hr(date)
    breakpoint()

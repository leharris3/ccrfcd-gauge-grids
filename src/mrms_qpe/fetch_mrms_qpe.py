import os
import xarray as xr

from glob import glob
from typing import List
from datetime import datetime, timedelta
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

    def _fetch_radar_only_qpe_x(
            self, 
            end_time: datetime, 
            product: str, 
            mode="nearest", 
            time_zone="UTC", 
            to_dir="__temp",
            del_tmp_files=False,
        ) -> xr.Dataset | None:
        """
        **Timezone**: ``UTC``
        Fetch MRMS ``RadarOnly_QPE`` suite of products. 

        Args
        ---
        :start_time: ``end_time``
        :mode: ``str`` 
        - "nearest", "first", or "next"
            - "nearest": find the closest valid file to provide ``datetime``
            - "first"  : closest valid file whos time < start_time
            - "next"   : closest valid file whos time > start_time

        Returns
        ---
        """

        # HACK: PDT -> UTC
        if time_zone == "PDT":
            end_time += timedelta(hours=7)

        yyyymmdd = end_time.strftime("%Y%m%d")
        basepath = MRMSPath(
            domain   = MRMSDomain.CONUS, 
            product  = product,
            yyyymmdd = yyyymmdd
            )
        
        try:
            file_paths   = self.mrms_client.ls(str(basepath))
        except:
            print(f"Error: no MRMS file @{str(basepath)}")
            return None

        nearest_path = self._get_closest_file(file_paths, end_time)
        
        # TODO: del grib2 files after download
        mp = MRMSPath.from_str(nearest_path)
        fp = self.mrms_client.download(str(mp), to=to_dir)
        
        zipped_gf = ZippedGrib2File(fp)
        gf        = zipped_gf.unzip(to_dir=to_dir)
        xa        = gf.to_xarray()

        if del_tmp_files == True:
            tmp_fps = glob(f"{to_dir}/**")
            for fp in tmp_fps:
                os.remove(fp)

        return xa

    def fetch_radar_only_qpe_15m(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-0:15``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_15M, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_1hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-1:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_01H, mode=mode, time_zone=time_zone)

    def fetch_radar_only_qpe_3hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-3:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_03H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_6hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-6:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_06H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_12hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-12:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_12H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_24hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-24:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_24H, mode=mode, time_zone=time_zone)


if __name__ == "__main__":
    client = MRMSQPEClient()
    date = datetime.now()
    ar = client.fetch_radar_only_qpe_6hr(date)
    breakpoint()

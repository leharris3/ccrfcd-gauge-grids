# **A study of CCRFCD rain-gauge & MRMS QPE alignment**
---
> *Work completed at the NWS in Las Vegas, NV*

This repository contains contains data and analysis of MRMS and Clark County Regional Flood Control District (CCRFCD) rain gauge QPE values for January of 2021 through July of 2025.

### Background & motivation

Las Vegas, NV is a low-lying, dry, and heavily populated region. Here, rain events are infrequent and especially dangerous, as even small amounts of precipitation reaching the ground can trigger flash-flooding events. Exasperating the challenge of forecasting in this region are disagrements between radar products and surface-level observations. High-based storms and low humidity have lead meterologists to believe that radar-only QPE products tend to *over estimate* surface-level precipitation. But just how well aligned are radar products and rain gauges, and what are the most important variables influincing "rain gauge bias" in the Las Vegas valley?

To systematically probe the questions above, we set out to conduct a large-scale study using precipitation data spanning the past five years (i.e., 2021-2025). By uncovering the key factors driving rain-guage bias, we hoped to develop simple, robust models that enable operational meteorologists to predict the offset between radar-only products and ground-level QPE for a wide range of events. By increasing confidence in various sources of guidance, we also hope to allow mets to issue accurate warnings sooner.

### Data collection

##### *CCRFCD rain-gauge values*

- Data source: [https://gustfront.ccrfcd.org/gagedatalist/](https://gustfront.ccrfcd.org/gagedatalist/)

We scraped data for 223 CCRFCD rain-gauges between the dates of [1/1/2020-7/26/25] using the script in `scripts/scrape_gustfront_v2.py` to crawl the Gustfront website. For each rain-gauge id in `data/7-23-25-scrape`, we select `interval=None` and `rawValues=False`. The resulting `csv` files have the following format:

| Date | Time | Value |
| :--: | :--: | :--:  |
| MM/DD/YYYY | HH:MM:SS | X.XX (in.) |
| "07/22/2025" | "16:00:56" | "1.34" |
| ... | ... | ... |

Here, the `Value` column represents *accumulated precipitation*, and resets to 0.0 at semi-random intervals. Also, note that `Date` and `Time` columns are recorded in Las Vegas **local time**.

##### *CCRFCD rain-gauge metadata*

We also gathered metadata about each CCRFCD weather station at `data/clark-county-rain-gauges/CCRFCD Station Locations 2025.csv`. 

| Station ID |         Name         |    Type    |   OOS   | Latitude | Longitude  |     |
| :--------: | :------------------: | :--------: | :-----: | :------: | :--------: | --- |
|     2      | Willow Beach 2 (NPS) | NPS System | `FALSE` | 35.87789 | -114.58875 |     |
|    ...     |         ...          |    ...     |   ...   |   ...    |    ...     |     |

##### *MRMS 1H-QPE*

- Data source: AWS bucket @[s3://noaa-mrms-pds/CONUS](s3://noaa-mrms-pds/CONUS)

Fetching MRMS data for our region proved to be an interesting challenge. To make life easier, we built a small python API for the MRMS AWS bucket in `src.utils.mrmrs.mrms`.

```python
from src.utils.mrmrs.mrms import MRMSAWSS3Client, MRMSURLs


client   = MRMSAWSS3Client()
res      = client.ls(MRMSURLs.BASE_URL_CONUS)
print(res)
```

Next, we developed a higher-level API in `src.mrms_qpe.fetch_mrms_qpe` to fetch products by date and handle `grib2` files behind the scenes.

```python
import xarray
from src.mrms_qpe.fetch_mrms_qpe import MRMSQPEClient


client               = MRMSQPEClient()
date                 = datetime.now()
xarr: xarray.Dataset = client.fetch_radar_only_qpe_full_day_1hr(date, del_tmps=True)
```

##### MRMS/CCRFCD data alignment

We bring everything together in the script at `scripts/gather_all_events.py`, which is responsible for generating a unified dataset of MRMS radar-only 1H-QPE and CCRFCD rain-gauge 1H-QPE data. Let's briefly walk through the steps to generate this dataset.

For each unique day (i.e., 00:00:00-23:59:59 UTC window) from [01/01/21] to [07/25/25], select days during which the following criteria are met.

1. MRMS data is available.
2. At least one grid-cell between lat/lon 35.8-36.4/-115.4-(-)114.8 records >= 0.25 inches of precipitation in a 24 hour period.

Next, for all days that meet the above criteria, download all MRMS radar-only 1H-QPE `grib2` files. These files are spaced at two-minute intervals, yeilding a total of $30 * 24 = 720$ MRMS files/day. 

Now, for each rain-gauge we have data for, we run several algorithms to calculate the 1H-QPE of each CRFCD gauge, and align it to our MRMS 1H-QPE data. It may be easier to visualize what's going by looking a complete, aligned data-table for a 24H period.

|     start_time      |      end_time       | station_id |    lat    |    lon     | gauge_qpe | mrms_qpe |     |
| :-----------------: | :-----------------: | :--------: | :-------: | :--------: | :-------: | :------: | --- |
| 2021-01-24 00:12:00 | 2021-01-24 01:12:00 |    2754    | 36.707972 | 245.923861 |    0.0    |   0.0    | 0.0 |
|         ...         |         ...         |    ...     |    ...    |    ...     |    ...    |   ...    |     |

Let's break down each of these columns:
- `start_time`: (**UTC**)
- `end_time`: (**UTC**)
- `station_id`: the CCRFCD rain-gauge's ID number
- `lat`/`lon`: location of the rain-gauge
- `gauge_qpe`: **(inches)** cummulative rainfall recorded by gauge between `start_time`/`end_time`
- `mrms_qpe`: **(inches)** cummulative rainfall recorded by MRMS radar-only product between `start_time`/`end_time` at `lat`/`lon`
    - Value for *the **nearest** MRMS grid-cell* to `lat`/`lon`

Note that we've taken extra care to convert the CCRFCD gauge timesteps from PDT->UTC.

### Methodology

### Conclusions

### Acknowledgements

# **Using notebooks in this repo**
---

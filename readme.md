# **A study of CCRFCD rain-gauge & MRMS QPE alignment**
---
> *Work completed at the NWS in Las Vegas, NV*

This repository contains contains data and analysis of MRMS and Clark County Regional Flood Control District (CCRFCD) rain-guage QPE values for January of 2021 through July of 2025.

### Background & motivation

Las Vegas, NV is a low-lying, dry, and heavily populated region. Here, rain events are infrequent and especially dangerous, as even small amounts of precipitation reaching the ground can trigger flash-flooding events. Exasperating the challenge of forecasting in this region are disagrements between radar products and surface-level observations. High-based stroms and low humidity have lead meterologists to believe that radar-only QPE products tend to *over estimate* surface-level precipitation. But just how well aligned are radar products and rain gauges, and what are the most important variables influincing "rain gauge bias" in the Las Vegas valley?

To systematically probe the questions above, we set out to conduct a large-scale study using precipitation data spanning the past five years (i.e., 2021-2025). By uncovering the key factors driving rain-guage bias, we hoped to develop simple, robust models that enable opperational meteorologists to predict the offset between radar-only products and ground-level QPE for a wide range of events. By increasing confidence in various sources of guidance, we also hope to allow mets to issue accurate warnings sooner.

### Data collection

##### *CCRFCD rain-gauge values*

We scraped data for 223 CCRFCD rain-gauges between the dates of 1/1/2020-7/26/25 using the script in `scripts/scrape_gustfront_v2.py` to access [https://gustfront.ccrfcd.org/gagedatalist/](https://gustfront.ccrfcd.org/gagedatalist/). For each rain-gauge id in `data/7-23-25-scrape`, we ...

##### *MRMS 1H-QPE*

### Methodology

### Conclusions

# **Using notebooks in this repo**
---

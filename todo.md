# **CCRFCD Rain-Gauge & MRMS QPE Alignment**
---

## Backend

- [x] 1. Functionality to update database of rain-gauge data to present

## Middleware/Logic

- [ ] 1. ...

## Frontend

- [x] 1. Side-by-side MRMS-QPE/CCRFCD with identical y-axis

## Data Collection

- [x] 1. 2021-2025 (late may.-early sep.) days 24HR MRMS QPE exceeded .25 in. at any point in the las vegas valley
- [x] 2. Delta-QPE for all days in `1.`
- [ ] 3. Eviornmental parameters (0Z + 12Z soundings) for all days in `1.`
    - [x] 3a. 0Z/12Z soundings
    - [x] 3b. ASOS data
    - [ ] 3c. Calculate derived fields

## Visualization & Analysis

- [ ] 1. Histogram of dates/max-qpe of data sources
- [ ] 2. ...

## Post-Processing/Data Cleaning

- [ ] 1. Remove bad gauges
- [ ] 2. Remove enormous delta_qpe spikes (e.g., 2.5 in.+)
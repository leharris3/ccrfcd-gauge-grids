python scrape_gustfront.py \
  --gage 1 \
  --start-date 01/1/2021 \
  --start-time "06:00 AM" \
  --end-date   05/30/2025 \
  --end-time   "12:32 PM" \
  --include-raw \
  --output willow_beach_rain.csv > _scrape.out 2>&1 &

exit
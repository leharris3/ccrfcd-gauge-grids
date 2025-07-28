import time
import subprocess

from tqdm import tqdm
from glob import glob
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

EVENTS_DIR = "data/events"
BASE_CMD = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=NV_ASOS&station=05U&station=10U&station=9BB&station=AWH&station=B23&station=BAM&station=BJN&station=BVU&station=CXP&station=DRA&station=EKO&station=ELY&station=HND&station=HTH&station=INS&station=LAS&station=LOL&station=LSV&station=MEV&station=NFL&station=P38&station=P68&station=RNO&station=RTS&station=TMT&station=TPH&station=U31&station=VGT&station=WMC&data=all&year1=2023&month1=1&day1=1&year2=2023&month2=1&day2=2&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=M&trace=T&direct=no&report_type=3&report_type=4"

CURL_BIN   = "curl"
CURL_FLAGS = ["-fLsS"]
MAX_RETRIES = 3
RETRY_SLEEP = 3


def build_url_for_day(base_url: str, day: datetime) -> str:
    """Replace the date fields in BASE_CMD with a single day's window."""
    next_day = day + timedelta(days=1)

    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)

    # Update date params (parse_qs gives lists)
    qs["year1"]  = [str(day.year)]
    qs["month1"] = [str(day.month)]
    qs["day1"]   = [str(day.day)]

    qs["year2"]  = [str(next_day.year)]
    qs["month2"] = [str(next_day.month)]
    qs["day2"]   = [str(next_day.day)]

    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def fetch_day(day: datetime, out_fp: Path):
    """Download one day's CSV using curl, with retries."""
    
    url = build_url_for_day(BASE_CMD, day)

    if out_fp.exists() and out_fp.stat().st_size > 0:
        print(f"[SKIP] {out_fp.name} already exists.")
        return

    for attempt in range(1, MAX_RETRIES + 1):
        
        try:
            cmd = [CURL_BIN, *CURL_FLAGS, "-o", str(out_fp), url]
            subprocess.run(cmd, check=True)
            
            # quick sanity check (file not empty)
            if out_fp.stat().st_size == 0:
                raise RuntimeError("Empty file downloaded.")
            print(f"[OK]   {out_fp.name}")
            return
        except Exception as e:
            print(f"[ERR]  {out_fp.name} attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
            else:
                # Optionally remove the zero-byte (or partial) file.
                if out_fp.exists():
                    out_fp.unlink()
                print(f"[FAIL] Giving up on {out_fp.name}")


def main():
    
    all_events = glob(f"{EVENTS_DIR}/*")
    for edir in tqdm(all_events, total=len(all_events)):

        try:
            dt_str   = edir.split("/")[-1]
            yyyymmdd = dt_str.split(" ")[0]
            year     = int(yyyymmdd[:4])
            month    = int(yyyymmdd[5:7])
            day      = int(yyyymmdd[8:])

            out_fp = Path(edir) / Path(f"{dt_str}_ASOS.csv")
            dt     = datetime(year, month, day)
        except:
            continue

        if out_fp.is_file(): continue

        fetch_day(dt, out_fp)



if __name__ == "__main__":
    main()
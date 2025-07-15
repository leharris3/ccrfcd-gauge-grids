"""
# TODO: rewrite; fix early stop bugs

Helper methods to scrape rain-gauge data from the Clark County Regional 
Flood Control District's portal.
URL: https://gustfront.ccrfcd.org/gagedatalist/
"""

import time
import logging
import pandas as pd

from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


DOWNLOAD_DIR = Path("/playpen/mufan/levi/tianlong-chen-lab/nws-lv-precip-forecasting/data/clark-county-rain-gauges/_test")
_URL = "https://gustfront.ccrfcd.org/gagedatalist/"
START_DATE = datetime(2021, 1, 1)
WEBDRIVER_WAIT_TIMEOUT = 120

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

def get_chrome_driver(download_path: Path) -> webdriver.Chrome:
    """
    Configures and returns a headless Chrome WebDriver instance.
    """

    logging.info(f"Setting up Chrome driver to download to: {download_path}")
    download_path.mkdir(exist_ok=True)

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    
    prefs = {
        "download.default_directory": str(download_path),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safeBrowse.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(download_path)},
    )
    return driver

def wait_for_download_complete(csv_path: Path,
                               timeout: int = 120,
                               poll: float = 1.0) -> None:
    """
    Block until `csv_path` has fully downloaded or until `timeout` (seconds)
    elapses.  Raises TimeoutError on failure.

    Chrome streams to `csv_path.with_suffix(csv_path.suffix + ".crdownload")`
    while the transfer is in progress, then renames it to the final file name
    when done.  We monitor that pattern.
    """
    deadline = time.time() + timeout
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".crdownload")

    last_size = -1
    stable_cycles = 0          # extra safety: require size to stop changing twice

    while time.time() < deadline:
        if csv_path.exists() and not tmp_path.exists():
            # Extra guard: make sure size has stabilised for 2 polls
            size = csv_path.stat().st_size
            if size == last_size:
                stable_cycles += 1
                if stable_cycles >= 2:          # size unchanged for 2 cycles
                    return                      # download complete
            else:
                stable_cycles = 0
                last_size = size

        time.sleep(poll)

    raise TimeoutError(f"Download of {csv_path.name} didn’t finish within {timeout}s")

def main():

    metadata_fp     = "data/clark-county-rain-gauges/ccrfcd_rain_gauge_metadata.csv"
    rain_gauge_dir  = "data/clark-county-rain-gauges/2021-"
    rain_gauge_csvs = [f for f in Path(rain_gauge_dir).glob("*.csv")]
    metadata        = pd.read_csv(metadata_fp)

    # remove invalid gauges
    metadata         = metadata[metadata["station_id"] > 0]
    unique_gauge_ids = sorted(list(set(metadata['station_id'].astype(int))))

    driver = get_chrome_driver(DOWNLOAD_DIR)

    # current TO ~60 sec
    wait = WebDriverWait(driver, WEBDRIVER_WAIT_TIMEOUT)

    # ---- setup ----
    logging.info(f"Navigating to {_URL}")

    driver.get(_URL)
    
    logging.info("Locating the gauge dropdown and extracting all gauge names.")
    
    # wait for dropdown to be clickable
    gauge_dropdown_element = wait.until(
        EC.element_to_be_clickable((By.ID, "uiGage"))
    )
    
    # get all <option> tags within the dropdown
    gauge_options = gauge_dropdown_element.find_elements(By.TAG_NAME, "option")
    
    # we get the visible text of each gauge to use for selection later
    all_gauge_names = [opt.text for opt in gauge_options if opt.text]
    
    if not all_gauge_names:
        logging.error("Could not find any gauge names. Exiting.")
        return
        
    logging.info(f"Found {len(all_gauge_names)} gauges to process.")

    # -------------

    for gauge_name in tqdm(all_gauge_names, desc="Downloading Gauge Data"):

        # skip invalid gauges
        _id = int(gauge_name.split(" - ")[0])
        try:
            if _id not in unique_gauge_ids: 
                logging.info(f"Skipping: {gauge_name}")
                continue
        except Exception as e:
            logging.error(f"Error: {e}") 
            logging.error(f"Error: could not find a valid gauge id for: {gauge_name}")
            continue

        # if fp already exists: skip
        fp = Path(f"{DOWNLOAD_DIR}") / Path(f"gagedata_{_id}.csv")
        if fp.is_file(): continue

        try:

            logging.info(f"Processing gauge: {gauge_name}")
            
            # the page state might change, so we re-locate the dropdown.
            # the 'wait' ensures we don't fail if the page is slow.
            gauge_dropdown = Select(wait.until(EC.element_to_be_clickable((By.ID, "uiGage"))))
            gauge_dropdown.select_by_visible_text(gauge_name)

            # set start date
            start_date_input = wait.until(
                EC.visibility_of_element_located((By.ID, "startDate"))
            )
            start_date_input.send_keys(Keys.CONTROL + "a") # Select all existing text
            start_date_input.send_keys(START_DATE.strftime("%m/%d/%Y"))
            
            # an action (like setting date) can trigger JS. A small, defensive pause.
            # a better way would be to wait for a specific element to update.
            driver.find_element(By.ID, "endDate").click()

            # # set interval
            # interval_dropdown = Select(wait.until(
            #     EC.element_to_be_clickable((By.ID, "uiInterval"))
            # ))

            # # select '5 minutes' time interval
            # interval_dropdown.select_by_index(1)

            # click download
            download_button = wait.until(
                EC.element_to_be_clickable((By.ID, "uiDownload"))
            )

            # download & wait
            download_button.click()
            wait_for_download_complete(fp, timeout=WEBDRIVER_WAIT_TIMEOUT)

        except (TimeoutException, NoSuchElementException) as e:
            logging.error(f"Failed to download data for '{gauge_name}'. Error: {e}")
            logging.info("Page state might be invalid. Refreshing the page to recover.")
            driver.refresh()
            continue

    logging.info("Scraping process finished. Closing driver.")
    driver.quit()


if __name__ == "__main__":
    main()

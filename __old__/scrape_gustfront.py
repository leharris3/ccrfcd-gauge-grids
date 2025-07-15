"""
Helper methods to scrape rain-gauge data from:
https://gustfront.ccrfcd.org/gagedatalist/
"""

import time

from pathlib import Path
from datetime import datetime
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait


_URL = "https://gustfront.ccrfcd.org/gagedatalist/"


def get_chrome_driver() -> webdriver.Chrome:

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    curr_dir = "/playpen/mufan/levi/tianlong-chen-lab/nws-lv-precip-forecasting/data/clark-county-rain-gauges/2021-"
    prefs = {
        "download.default_directory": curr_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": curr_dir},
    )
    return driver


driver = get_chrome_driver()
driver.get(_URL)
print(driver.title)

gauge_element     = driver.find_element(By.ID, "uiGage")
gauge_selector    = Select(gauge_element)
gauge_selector.select_by_index(0)

gauges            = gauge_element.find_elements(By.TAG_NAME, "option")
all_gauge_names   = [g.accessible_name for g in gauges]
total_gauges      = len(all_gauge_names)
gauge_id_name_map = {}

for name in all_gauge_names:
    gauge_id = int(name.split(" - ")[0])
    gauge_id_name_map[gauge_id] = name
    breakpoint()

download_element = driver.find_element(By.ID, "uiDownload")


for i in tqdm(range(total_gauges), total=total_gauges):

    try:
        
        gauge_element = driver.find_element(By.ID, "uiGage")
        gauge_selector = Select(gauge_element)

        gauges = gauge_element.find_elements(By.TAG_NAME, "option")

        # 2. set start/end time
        start_date_element = driver.find_element(By.ID, "startDate")
        start_date_element.send_keys(Keys.CONTROL + "a")

        # 2a. set start date
        start_date = datetime(2021, 1, 1)
        start_date_element.send_keys(start_date.strftime("%m/%d/%Y"))
        end_date_element = driver.find_element(By.ID, "endDate")

        # 3. set interval to second index value
        interval_element = driver.find_element(By.ID, "uiInterval")
        interval_select = Select(interval_element)

        # 3a. set interval to "5 minutes"
        interval_select.select_by_index(1)
        
        gauge_selector.select_by_index(i)
        download_element.click()

    except:

        print(f"failed to download {i}")
        driver.quit()
        driver = get_chrome_driver()
        driver.get(_URL)

        gauge_element = driver.find_element(By.ID, "uiGage")
        gauge_selector = Select(gauge_element)

        gauges = gauge_element.find_elements(By.TAG_NAME, "option")

        # 2. set start/end time
        start_date_element = driver.find_element(By.ID, "startDate")
        start_date_element.send_keys(Keys.CONTROL + "a")

        # 2a. set start date
        start_date = datetime(2021, 1, 1)
        start_date_element.send_keys(start_date.strftime("%m/%d/%Y"))
        end_date_element = driver.find_element(By.ID, "endDate")

        # 3. set interval to second index value
        interval_element = driver.find_element(By.ID, "uiInterval")
        interval_select = Select(interval_element)

        # 3a. set interval to "5 minutes"
        interval_select.select_by_index(1)

        # 4. click "Download CSV File" and collect result
        download_element = driver.find_element(By.ID, "uiDownload")


breakpoint()
driver.quit()

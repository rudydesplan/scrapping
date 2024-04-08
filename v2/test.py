from helium import *
import logging
from selenium import webdriver
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException , StaleElementReferenceException
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

class CookieWarningFilter(logging.Filter):
    def filter(self, record):
        return 'cookies' not in record.getMessage().lower()

# Configure logging
logging.basicConfig(level=logging.WARNING)

# Get the root logger and add the filter
logger = logging.getLogger()
logger.addFilter(CookieWarningFilter())


def start_edge_with_helium(headless=True):
    options = webdriver.EdgeOptions()
    options.add_argument("--disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure")
    if headless:
        options.add_argument("--headless")
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)   
    service = webdriver.EdgeService(executable_path=EdgeChromiumDriverManager().install())
    driver = webdriver.Edge(service=service, options=options)
    set_driver(driver)

def extract_company_links(page_url):
    go_to(page_url)
    try:
        WebDriverWait(get_driver(), 30).until(
            EC.presence_of_element_located((By.ID, "pcd_top_title"))
        )
    except TimeoutException:
        print(f"Timeout waiting for the company directory on {page_url}")
        return []
    company_links = find_all(S('a.pcd_list_company_link'))
    urls = [link.web_element.get_attribute('href') for link in company_links]
    return urls

def navigate_and_extract(letter):
    base_url = 'https://join.com/companies/'
    initial_page = f'{base_url}{letter}'
    urls = extract_company_links(initial_page)
    try:
        num_pages = len(find_all(S('a.pcd_pagination_link')))
        for page_num in range(2, num_pages + 1):
            page_url = f"{base_url}{letter}/page/{page_num}"
            urls += extract_company_links(page_url)
    except TimeoutException:
        print(f"Could not find pagination for letter {letter}, moving on.")
    return urls

def check_status_and_extract_keywords(company_url):
    go_to(company_url)
    job_keywords = {"Data": False, "Devops": False, "SRE": False, "Analytics": False}
    
    # Refetch the element immediately before accessing its text attribute
    total_positions_element = WebDriverWait(get_driver(), 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="TabBadge"]'))
    )
    total_positions_element = WebDriverWait(get_driver(), 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="TabBadge"]'))
    )
    total_positions = int(total_positions_element.text)
    total_pages = max(-(-total_positions // 5), 1)  # Calculate the number of pages

    for page_num in range(1, total_pages + 1):
        current_page_url = f"{company_url}?page={page_num}"
        go_to(current_page_url)

        # Check if the page is active
        try:
            WebDriverWait(get_driver(), 10).until(EC.presence_of_element_located((By.TAG_NAME, "title")))
            is_active = True
        except TimeoutException:
            is_active = False

        if is_active:
            job_listings = find_all(S('.JobTile___StyledJobLink-sc-989ef686-0'))
            for listing in job_listings:
                job_title = listing.web_element.text.lower()

                if "data" or "données" in job_title:
                    job_keywords["Data"] = True
                if "devops" in job_title:
                    job_keywords["Devops"] = True
                if "sre" in job_title or "site reliability" in job_title:
                    job_keywords["SRE"] = True
                if "analytics" in job_title:
                    job_keywords["Analytics"] = True

        # Break out of the loop early if any job keyword is found
        if any(job_keywords.values()):
            break

    # Return company URL only if any job keyword is True
    return {"Company URL": company_url} if any(job_keywords.values()) else {}

def process_letter(letter):
    print(f"Processing letter: {letter.upper()}")
    company_urls = navigate_and_extract(letter)
    all_company_info = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = [executor.submit(check_status_and_extract_keywords, url) for url in company_urls]
        for future in as_completed(future_to_url):
            all_company_info.extend(future.result())
    print(f"Processed {len(company_urls)} URLs for letter {letter.upper()}.")
    return all_company_info


if __name__ == '__main__':
    start_edge_with_helium(headless=True)  # Initiate the browser in headless mode as required
    letters = ['x']

    all_company_info = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        # Process each letter in parallel
        futures_to_letters = [executor.submit(process_letter, letter) for letter in letters]

        for future in as_completed(futures_to_letters):
            all_company_info.extend(future.result())

    # Convert the results into a pandas DataFrame
    df = pd.DataFrame(all_company_info)

    # Export the DataFrame to a CSV file
    df.to_csv("company_status_with_keywords.csv", index=False)

    kill_browser()  # Close the browser session

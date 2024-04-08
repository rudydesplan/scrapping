from helium import *
from selenium import webdriver
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

def start_edge_with_helium(headless=True):
    options = webdriver.EdgeOptions()
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

def check_company_status(company_url):
    go_to(company_url)
    try:
        title = WebDriverWait(get_driver(), 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "title"))
        ).get_attribute("textContent")
        is_active = "Page not found (404)" not in title
    except TimeoutException:
        is_active = False
    return is_active

def get_job_keywords(company_url):
    go_to(company_url)
    job_keywords = {"Data": False, "Devops": False, "SRE": False, "Analytics": False}
    try:
        job_listings = find_all(S('.JobTile___StyledJobLink-sc-989ef686-0'))
        for listing in job_listings:
            job_title = listing.web_element.text.lower()
            if "data" in job_title:
                job_keywords["Data"] = True
            if "devops" in job_title:
                job_keywords["Devops"] = True
            if "sre" in job_title or "site reliability" in job_title:
                job_keywords["SRE"] = True
            if "analytics" in job_title:
                job_keywords["Analytics"] = True
    except TimeoutException:
        print(f"Timeout occurred while trying to access {company_url}")
    return job_keywords

def process_company_url(url):
    status = check_company_status(url)
    if status:
        job_keywords = get_job_keywords(url)
        return {"Company URL": url, "Status": status, **job_keywords}
    else:
        return {"Company URL": url, "Status": status, "Data": False, "Devops": False, "SRE": False, "Analytics": False}

def process_letter(letter):
    print(f"Processing letter: {letter.upper()}")
    company_urls = navigate_and_extract(letter)
    company_info = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_url = {executor.submit(process_company_url, url): url for url in company_urls}
        for future in as_completed(future_to_url):
            company_info.append(future.result())
    print(f"Processed {len(company_urls)} URLs for letter {letter.upper()}.")
    return company_info

if __name__ == '__main__':
    start_edge_with_helium()
    letters = ['x']
    all_company_info = []

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_letter, letter) for letter in letters]
        for future in as_completed(futures):
            all_company_info.extend(future.result())

    df = pd.DataFrame(all_company_info)
    df.to_csv("company_status_with_keywords.csv", index=False)
    
    kill_browser()
    print("DataFrame exported to company_status_with_keywords.csv")

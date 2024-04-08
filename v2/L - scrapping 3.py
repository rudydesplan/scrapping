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

def check_status_and_extract_keywords(company_url):
    go_to(company_url)
    job_keywords = {"Data": False, "Devops": False, "SRE": False, "Analytics": False}
    is_active = False
    locations = []  # Store each job's location
    contract_types = []  # Store each job's contract type
    page_num = 1  # Initialize the page number
    has_next_page = True

    while has_next_page:
        current_page_url = f"{company_url}?page={page_num}"
        go_to(current_page_url)

        try:
            title = WebDriverWait(get_driver(), 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "title"))
            ).get_attribute("textContent")
            is_active = "Page not found (404)" not in title

            if is_active:
                job_listings = find_all(S('.JobTile___StyledJobLink-sc-989ef686-0'))
                for listing in job_listings:
                    # Extract location first
                    text_elements = find_all(S('.JobTile-elements___StyledText-sc-e7e7aa1d-4'))
                    if len(text_elements) >= 1:
                        location_text = text_elements[0].web_element.text.strip()
                        if "Suisse" in location_text:
                            # Proceed only if location contains "Suisse"
                            locations.append(location_text)
                            contract_type = text_elements[1].web_element.text.strip() if len(text_elements) >= 2 else "Unknown"
                            contract_types.append(contract_type)

                            job_title = listing.web_element.text.lower()
                            if "data" in job_title or "données" in job_title:
                                job_keywords["Data"] = True
                            if "devops" in job_title:
                                job_keywords["Devops"] = True
                            if "sre" in job_title or "site reliability" in job_title:
                                job_keywords["SRE"] = True
                            if "analytics" in job_title:
                                job_keywords["Analytics"] = True

                # Check for the next page link by its aria-label
                next_page_elements = find_all(S('[aria-label="Next page"]'))
                if next_page_elements:
                    page_num += 1  # Prepare to load the next page
                else:
                    has_next_page = False  # No more pages to load

        except TimeoutException:
            print(f"Timeout occurred while trying to access {current_page_url}")
            break

    #return is_active, job_keywords, locations, contract_types
    return {"Company URL": company_url, "Status": is_active, **job_keywords, "Locations": locations, "Contract Types": contract_types}

def process_company_url(url):
    status, job_keywords, locations, contract_types = check_status_and_extract_keywords(url)
    results = []
    any_job_keyword_true = any(job_keywords.values())
    if status and any_job_keyword_true:
        for location, contract_type in zip(locations, contract_types):
            result = {"Company URL": url, "Status": status, **job_keywords, "Location": location, "Contract Type": contract_type}
            results.append(result)
    return results

def process_letter(letter):
    print(f"Processing letter: {letter.upper()}")
    company_urls = navigate_and_extract(letter)
    all_company_info = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_url = [executor.submit(process_company_url, url) for url in company_urls]
        for future in as_completed(future_to_url):
            all_company_info.extend(future.result())
    print(f"Processed {len(company_urls)} URLs for letter {letter.upper()}.")
    return all_company_info


if __name__ == '__main__':
    start_edge_with_helium(headless=True)  # Initiate the browser in headless mode as required

    letters = [chr(i) for i in range(97, 123)]  # Generating letters a-z
    letters.append('#')  # Include any additional characters if needed

    all_company_info = []

    with ThreadPoolExecutor(max_workers=16) as executor:
        # Process each letter in parallel
        futures_to_letters = [executor.submit(process_letter, letter) for letter in letters]

        for future in as_completed(futures_to_letters):
            all_company_info.extend(future.result())

    # Convert the results into a pandas DataFrame
    df = pd.DataFrame(all_company_info)

    # Export the DataFrame to a CSV file
    df.to_csv("company_status_with_keywords.csv", index=False)

    kill_browser()  # Close the browser session

    print("DataFrame exported to company_status_with_keywords.csv")

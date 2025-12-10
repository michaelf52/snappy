#!/usr/bin/env python3
import os
import csv
import webbrowser
import time
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from typing import Optional, Tuple

SAVE_PAGE_COUNTER = 0

# ========================= 
# helper functions
# =========================


# extract user id from URL

def user_id_from_url(url: str) -> str | None:

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    user_vals = qs.get("user")
    if user_vals:
        return user_vals[0]
    return None

# =======================

# sanitize URLs

def sanitize_urls(urls: list[str]) -> list[str]:
    sanitized = []
    for url in urls:
        user_id = user_id_from_url(url)
        if user_id:
            sanitized_url = f"https://scholar.google.com/citations?user={user_id}"
            sanitized.append(sanitized_url)
        else:
            print(f" Warning - Could not extract user id from URL: {url}, skipping.")
    return sanitized

# =======================

# save the page

def save_page(
    url: str,
    html_dir: str,
    driver: webdriver.Chrome,
    timeout: int = 10,
    max_retries: int = 5,
) -> str | None:

    global SAVE_PAGE_COUNTER
    SAVE_PAGE_COUNTER += 1
    out_dir = Path(html_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # derive user_id from the URL (fallback if pattern not found)
    if "user=" in url:
        user_id = url.split("user=")[-1].split("&")[0]
    else:
        # alternative fallback
        user_id = url.rstrip("/").split("/")[-1]

    out_path = out_dir / f"{user_id}.htm"

    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n Profile #{SAVE_PAGE_COUNTER} - Attempt {attempt}/{max_retries} for {url}")
            driver.get(url)

            # wait for the page to be "ready"
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            # wait for an element to ensure page loaded
            # profile name: <div id="gsc_prf_in">Name</div>
            try:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.ID, "gsc_prf_in"))
                )
            except TimeoutException:
                print(" Warning - Did not find #gsc_prf_in, continuing anyway.")

            # small delay to let JS finish populating tables
            time.sleep(1.0)

            page_source = driver.page_source or ""

            # sanity checks on page source
            if len(page_source) < 2000 or "<html" not in page_source.lower():
                print(" Warning - Page source looks very small or malformed, retrying...")
                continue

            # GS-specific sanity check
            if "gsc_rsb_st" not in page_source and "gsc_a_t" not in page_source:
                print(" Warning - expected page elements not found, may be a blocker page (e.g. CAPTCHA).")
                continue

            # write file
            with open(out_path, "w", encoding="utf-8", errors="replace") as f:
                f.write(page_source)

            print(f" Saved HTML to: {out_path}")
            return str(out_path)

        except (TimeoutException, WebDriverException) as e:
            print(f" ERROR - Exception during load: {type(e).__name__}: {e}")
            if attempt < max_retries:
                print(" Retrying ...")
                time.sleep(2)
            else:
                print(" Giving up after max retries.")

    return None


# =======================

# beautiful soup scraper 

from typing import Optional, Tuple, List
from bs4 import BeautifulSoup

def scrape_it(html: str, journal_list: list[str]) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    List[str],              # research areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    dict                    # journal_match_counts
]:
    soup = BeautifulSoup(html, "html.parser")

    # ---------------------------------------------------------------------
    # name: <div id="gsc_prf_in">name</div>
    # ---------------------------------------------------------------------
    name = None
    name_div = soup.find("div", id="gsc_prf_in")
    if name_div:
        name = name_div.get_text(strip=True)
    if name:
        print(f"\nScraping profile for '{name}'")
    else:
        print("\nScraping profile for 'UNKNOWN'")

    # ---------------------------------------------------------------------
    # institution / affiliation:
    #   <div class="gsc_prf_il">The University of X</div>
    #   (first such div is usually the affiliation)
    # ---------------------------------------------------------------------
    institution: Optional[str] = None
    inst_divs = soup.find_all("div", class_="gsc_prf_il")
    if inst_divs:
        institution = inst_divs[0].get_text(strip=True)
    if institution:
        print(f"Institution: {institution}")
    else:
        print("Institution: None found")

    # ---------------------------------------------------------------------
    # research areas / interests:
    #   <div id="gsc_prf_int">
    #       <a class="gsc_prf_inta">Area 1</a>
    #       <a class="gsc_prf_inta">Area 2</a>
    #   </div>
    # ---------------------------------------------------------------------
    research_areas: List[str] = []
    int_div = soup.find("div", id="gsc_prf_int")
    if int_div:
        for a in int_div.find_all("a", class_="gsc_prf_inta"):
            text = a.get_text(strip=True)
            if text:
                research_areas.append(text)
    if research_areas:
        print("Research areas: " + ", ".join(research_areas))
    else:
        print("Research areas: None found")
        
    # ---------------------------------------------------------------------
    # h-index and citations table:
    # <table id="gsc_rsb_st">
    #   rows for "Citations", "h-index", "i10-index"
    #   columns: [label, All, Since YYYY]
    # ---------------------------------------------------------------------
    h_all: Optional[int] = None
    h_5y: Optional[int] = None
    cit_all: Optional[int] = None
    cit_5y: Optional[int] = None

    table = soup.find("table", id="gsc_rsb_st")
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            label = cells[0].get_text(strip=True).lower()

            # Citations row
            if "citations" in label:
                if len(cells) >= 2:
                    text_all = cells[1].get_text(strip=True)
                    try:
                        cit_all = int(text_all)
                    except ValueError:
                        cit_all = None

                if len(cells) >= 3:
                    text_since = cells[2].get_text(strip=True)
                    try:
                        cit_5y = int(text_since)
                    except ValueError:
                        cit_5y = None

            # h-index row
            if "h-index" in label:
                if len(cells) >= 2:
                    text_all = cells[1].get_text(strip=True)
                    try:
                        h_all = int(text_all)
                    except ValueError:
                        h_all = None

                if len(cells) >= 3:
                    text_5y = cells[2].get_text(strip=True)
                    try:
                        h_5y = int(text_5y)
                    except ValueError:
                        h_5y = None

    print(f"h-index (all): {h_all}, h-index (5y): {h_5y}")
    print(f"citations (all): {cit_all}, citations (5y): {cit_5y}")

    # ---------------------------------------------------------------------
    # journal matching in publications table
    # <table id="gsc_a_t">...</table>
    # ---------------------------------------------------------------------
    journal_match_counts = {journal: 0 for journal in journal_list}

    table_pubs = soup.find("table", id="gsc_a_t")
    if table_pubs and journal_list:
        for row in table_pubs.find_all("tr", class_="gsc_a_tr"):
            cells = row.find_all("td", class_="gsc_a_t")
            if not cells:
                continue

            td = cells[0]
            gray_elems = td.find_all("div", class_="gs_gray")

            # expect at least 2 gs_gray divs:
            # [0] authors
            # [1] journal info
            if len(gray_elems) < 2:
                continue

            journal_info = gray_elems[1].get_text(strip=True).lower()
            for journal in journal_list:
                if journal.lower() in journal_info:
                    print(f"Journal match: '{gray_elems[1].get_text(strip=True)}' -> '{journal}'")
                    journal_match_counts[journal] += 1

    print("Journal counts:")
    for journal, count in journal_match_counts.items():
        print(f"  • {journal}: {count}")
    print("\n------------------------------------------")

    return name, institution, research_areas, h_all, h_5y, cit_all, cit_5y, journal_match_counts


# =======================

# process a profile

def process_profile(url: str, journal_list: dir, html_dir: str = "./html"):

    user_id = user_id_from_url(url)
    if not user_id:
        print(f" Warning - Could not extract user id from URL: {url}")
        user_id = "UNKNOWN"

    html_path = os.path.join(html_dir, f"{user_id}.htm")
    if not os.path.exists(html_path):
        print(f" Warning - HTML file not found for user_id={user_id}, expected: {html_path}")
        return None

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # call the scraper
    name, institution, research_areas, h_all, h_5y, cit_all, cit_5y, journal_match_counts = scrape_it(html, journal_list) 

    record =  {
        "url": url,
        "user_id": user_id,
        "name": name if name is not None else "",
        "institution": institution if institution is not None else "",
        "research_areas": "; ".join(research_areas) if research_areas else "",
        "citations_all": cit_all if cit_all is not None else "",
        "citations_5y": cit_5y if cit_5y is not None else "",
        "h_index_all": h_all if h_all is not None else "",
        "h_index_5y": h_5y if h_5y is not None else "",
    }
    
    # calculate the total number of journal matches
    record["journal_count_tot"] = sum(journal_match_counts.values())
    
    for journal in journal_list:
        record[journal] = journal_match_counts.get(journal, 0)
    
    return record

# initialize webdriver 

def get_driver(headless=True) -> webdriver.Chrome | None:
    print(" Checking/initialising Chrome WebDriver...")

    try:
        # Chrome options
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        # use cached driver if available; installs only when needed
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        print(" Chrome WebDriver initialised successfully.")
        return driver

    except Exception as e:
        print(" ERROR - Failed to start Chrome WebDriver.")
        print(f" Exception: {type(e).__name__}: {e}")   
        print("Try manually installing ChromeDriver matching your Chrome version:")

        return None

# =========== 

# open default browser

def open_default_browser(url: str = "www.google.com") -> bool:

    try:
        opened = webbrowser.open(url, new=2)  #
        if opened:
            print(f" Opened default browser with URL: {url}")
            return True
        else:
            print(f" Warning - Browser did not open URL: {url}")
            return False

    except Exception as e:
        print(" ERROR - Failed to open the default web browser.")
        print(f" Exception: {type(e).__name__}: {e}")
        return False

# =====================================================

# main

def main():
    
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" )    
    print(" ~~~~~~ Welcome to Snapppy - The Super Neat Academic Profile Parser.py ~~~~~~" )
    print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" )

    # request URL list file name (default is URL_list.txt)
    url_list_file = input(
        " Enter the name of the file containing a list of Google Scholar URLs "
        "or press Enter for default ('URL_list.txt'):\n ~ "
    ) or "URL_list.txt"
    
    # read URL list
    if not os.path.exists(url_list_file):
        print(f" ERROR - {url_list_file} not found in current directory.")
        return
        
    with open(url_list_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
        
    # sanitize URLs - this converts foreign language versions to standard English URLs
    urls = sanitize_urls(urls)

    if not urls:
        print(" No URLs found in URL_list.txt.")
        return
        
    # read list of key journal names that are of interest
    # the number of publications in these journals will be counted for each profile
    journal_list_file = input(
        " Enter the name of the file containing a list of journal names of interest "
        "or press Enter for default ('journal_list.txt'):\n ~ "
    ) or "journal_list.txt"
    
    if not os.path.exists(journal_list_file):
        print(f" Warning - {journal_list_file} not found in current directory. I will not count journal publications.")
        journal_titles = []
    else:
        with open(journal_list_file, "r", encoding="utf-8") as f:
            journal_titles = [line.strip() for line in f if line.strip()]
            
        if not journal_titles:
            print(" Warning - No journal titles found in journal_list.txt. This is okay, but no journal counts will be recorded.")
        else:
            print(f" Loaded {len(journal_titles)} journal titles from {journal_list_file}")
            for jt in journal_titles:
                print(f"       - {jt}")
                
    # output file name
    output_file = "snapppy_results.csv"    
    
    # do a test that the output file can be opened for writing
    print(f" Testing that I can open CSV output file '{output_file}' for writing...")
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f_out:
            pass
    except Exception as e:
        print(f" ERROR - Could not open output file '{output_file}' for writing. \n You probably have it open in another program.")
        print(f" Exception: {type(e).__name__}: {e}\n")
        return       
    
    xlsx_file = output_file.replace(".csv", ".xlsx")
    print(f" Testing that I can open Excel output file '{xlsx_file}' for writing...")
    try:
        with open(xlsx_file, "w", newline="", encoding="utf-8") as f_out:
            pass
    except Exception as e:
        print(f" ERROR - Could not open output file '{xlsx_file}' for writing. \n You probably have it open in another program.")
        print(f" Exception: {type(e).__name__}: {e}\n")
        return
             
        
    # create html directory if not exists
    html_dir = "./html"
    
    if not os.path.isdir(html_dir):
        os.makedirs(html_dir , exist_ok=True)
                    
    # ask user if they wish to skip the page downloading step
    print("\n Do you wish to skip the page downloading step and only parse existing HTML files? (y/n): ", end="")
    choice = input().strip().lower()
    
    if choice != 'y':
        print("\n Proceeding to download HTML files.\n")
                    
        # delete all files in html_dir
        print (" Deleting existing files in html directory...")
        for filename in os.listdir(html_dir):
            file_path = os.path.join(html_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f" Warning - Could not delete file: {file_path}, error: {e}")
    
        # initialize Chrome webdriver
        driver = get_driver(headless=True)
        if driver is None:
            print(" ERROR - Could not initialize Chrome WebDriver. Exiting.")
            return
            
        # process each URL        
        print (" Opening each URL in your web browser and then saving the page...")
        print (" This may take a while depending on number of URLs.")

        try:
            for url in urls:
                print(f" Processing URL: {url}")
                save_page(url, html_dir, driver, timeout=15, max_retries=3)      
        finally:
            driver.quit()

        print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")        
        print("\n All done! HTML pages saved in html directory.")
        print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    else:
        print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~") 
        print("\n Skipping page downloading step. Will only parse existing HTML files.\n")
        print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\n Now scraping the web pages for key research metrics...")
    print("\n ------------------------------------------") 
                    
    # process each URL            
    results = []
    for url in urls:
        record = process_profile(url, journal_titles, html_dir)
        if record is not None:
            results.append(record)

    if not results:
        print(" No results to write (no HTML files found / parsed).")
        return
    
    # write results to CSV
    print(f"\n Writing results to CSV file: {output_file} ...")
    
    fieldnames = ["url", 
                  "user_id", 
                  "name", 
                  "institution", 
                  "research_areas", 
                  "citations_all", 
                  "citations_5y", 
                  "h_index_all", 
                  "h_index_5y",
                  "journal_count_tot"] + journal_titles
    
    column_labels = {
        "url": "URL",
        "user_id": "Scholar ID",
        "name": "Name",
        "institution": "Institution",
        "research_areas": "Research Areas",
        "citations_all": "Citations (All)",
        "citations_5y": "Citations (5y)",
        "h_index_all": "H-index (All)",
        "h_index_5y": "H-index (5y)",
        "journal_count_tot": "Total Journal Publications"
    }

    for j in journal_titles:
        column_labels[j] = f"Publications in {j}"

    pretty_headers = [column_labels[col] for col in fieldnames]

    with open(output_file, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(pretty_headers)
        for row in results:
            writer.writerow([row[col] for col in fieldnames])

    # convert output_file to an xlsx file
    print("\n Converting CSV results to Excel format...")
    try:
        xlsx_file = output_file.replace(".csv", ".xlsx")
        df = pd.read_csv(output_file)
        df.to_excel(xlsx_file, index=False)
        print(f" Excel file saved: {xlsx_file}")
        
    except Exception as e:
        print(f"\n ERROR - Could not convert CSV to Excel. Exception: {type(e).__name__}: {e}")
    
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"\n All done! Wrote {len(results)} rows to {output_file} and {xlsx_file}.")
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    # view the urls in default browser
    print("\n Would you like to step through each of the URLs? (y/n): ", end="")
    choice = input().strip().lower()

    if choice == 'y':
        print("\n Stepping through each URL in your default web browser...")
        for url in urls:
            print(f"\nOpening URL: {url}")
            opened = open_default_browser(url)

            if not opened:
                print("Warning: Could not open browser. Stopping step-through.\n")
                break
            
            time.sleep(0.2)

            input("Press Enter to continue to the next URL...")
            
    print(f"\n All done! The results are in {output_file}. Bye!\n")
    print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    
# =====================================================

# main entry point

if __name__ == "__main__":
    main()        
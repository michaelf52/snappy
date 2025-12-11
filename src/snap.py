#!/usr/bin/env python3
import os
import csv
import webbrowser
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Optional, Tuple, List, Dict, Generator
import requests
import pandas as pd
from bs4 import BeautifulSoup

# =========================
# helper functions
# =========================

# extract user_id from URL

def user_id_from_url(url: str) -> Optional[str]:

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    user_vals = qs.get("user")
    if user_vals:
        return user_vals[0]
    return None

# =========================

# sanitize list of URLs to standard format in English!

def sanitize_urls(urls: List[str]) -> List[str]:

    sanitized: List[str] = []
    for url in urls:
        user_id = user_id_from_url(url)
        if user_id:
            sanitized_url = f"https://scholar.google.com/citations?user={user_id}&hl=en"
            sanitized.append(sanitized_url)
        else:
            print(f" Warning - Could not extract user id from URL: {url}, skipping.")
    return sanitized

# =========================

# deal with GS blocking and CAPTCHA and other antics

def looks_like_block_page(html: str) -> bool:

    text = html.lower()
    block_markers = [
        "unusual traffic",
        "/sorry/",
        "not a robot",
        "solve the captcha",
        "submit a verification",
        "our systems have detected",
        "scholar help",
    ]

    if "captcha" in text:
        return True

    return any(marker in text for marker in block_markers)

# =========================

# get the list_works URL using cstart/pagesize params

def build_list_works_url(base_url: str, cstart: int, pagesize: int = 100) -> str:

    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)

    # make sure key params are present; add/override view_op, cstart, pagesize
    qs["view_op"] = ["list_works"]
    qs["cstart"] = [str(cstart)]
    qs["pagesize"] = [str(pagesize)]

    new_query = urlencode(qs, doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)

# =========================

# step through pages with requests

def iter_scholar_pages_requests(
    base_url: str,
    session: requests.Session,
    pagesize: int = 100,
    max_pages: int = 50,
    delay: float = 1.0,              # normal delay between successful pages
    max_block_retries: int = 5,      # how many times to retry a blocked page
    block_backoff_base: float = 10.0 # starting backoff in seconds
) -> Generator[str, None, None]:

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        )
    }

    cstart = 0
    page_index = 0

    while page_index < max_pages:
        url = build_list_works_url(base_url, cstart=cstart, pagesize=pagesize)
        print(f"\n Loading publications page {page_index + 1} (cstart={cstart})")

        block_attempts = 0

        while True:
            try:
                resp = session.get(url, headers=headers, timeout=15)
            except requests.RequestException as e:
                print(f"  Error: request exception {type(e).__name__}: {e}")
                block_attempts += 1
                if block_attempts > max_block_retries:
                    print("  Too many request failures for this page, giving up.")
                    return
                backoff = block_backoff_base * (2 ** (block_attempts - 1))
                print(f"  Backing off for {backoff:.1f} seconds before retrying...")
                time.sleep(backoff)
                continue

            # deal with status-based blocking
            if resp.status_code in (429, 503):
                block_attempts += 1
                print(f"  HTTP {resp.status_code} suggests rate limiting or temporary block.")
                if block_attempts > max_block_retries:
                    print("  Too many block responses, stopping pagination.")
                    return
                backoff = block_backoff_base * (2 ** (block_attempts - 1))
                print(f"  Backing off for {backoff:.1f} seconds before retrying...")
                time.sleep(backoff)
                continue

            if resp.status_code != 200:
                print(f"  Error: HTTP {resp.status_code}, stopping.")
                return

            html = resp.text

            # check for CAPTCHA / unusual traffic page
            if looks_like_block_page(html):
                block_attempts += 1
                print("  Page looks like a CAPTCHA / 'unusual traffic' block.")
                if block_attempts > max_block_retries:
                    print("  Too many block-like responses, stopping pagination.")
                    return
                backoff = block_backoff_base * (2 ** (block_attempts - 1))
                print(f"  Backing off for {backoff:.1f} seconds then retrying this page...")
                time.sleep(backoff)
                continue

            # woohoo - we have a page
            break

        # sanity check - parse the publications table to see if we have rows
        
        soup = BeautifulSoup(html, "html.parser")
        table_pubs = soup.find("table", id="gsc_a_t")
        if not table_pubs:
            print("  No publications table found, stopping.")
            return

        rows = table_pubs.find_all("tr", class_="gsc_a_tr")
        if not rows:
            print("  No publication rows found, stopping.")
            return

        print(f"  Found {len(rows)} publication rows on this page.")
        yield html

        # if we have fewer rows than pagesize then this is the last page
        if len(rows) < pagesize:
            print("  Last page detected (fewer than pagesize rows).")
            return

        cstart += pagesize
        page_index += 1

        # add another delay between pages just to avoid looking like a bot 
        time.sleep(delay)

# =========================

# I love BeautifulSoup :)~

def scrape_it(html: str, journal_list: List[str]) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    List[str],              # research areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    Dict[str, int]          # journal_match_counts
]:
    soup = BeautifulSoup(html, "html.parser")

    # ---------------------------------------------------------------------
    # name tag: <div id="gsc_prf_in">Name</div>
    # ---------------------------------------------------------------------
    name = None
    name_div = soup.find("div", id="gsc_prf_in")
    if name_div:
        name = name_div.get_text(strip=True)
    if name:
        print(f"\n Scraping profile for '{name}'")
    else:
        print("\n Scraping profile for 'UNKNOWN'")

    # ---------------------------------------------------------------------
    # institution / affiliation tag:
    #   <div class="gsc_prf_il">The University of Excellence and other Buzzwords</div>
    # ---------------------------------------------------------------------
    institution: Optional[str] = None
    inst_divs = soup.find_all("div", class_="gsc_prf_il")
    if inst_divs:
        institution = inst_divs[0].get_text(strip=True)
    print(f" Institution: {institution if institution else 'None found'}")

    # ---------------------------------------------------------------------
    # research areas / interests tags:
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
        print(" Research areas: " + ", ".join(research_areas))
    else:
        print(" Research areas: None found")

    # ---------------------------------------------------------------------
    # h-index and citations table tags:
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

    print(f" h-index (all): {h_all}, h-index (5y): {h_5y}")
    print(f" citations (all): {cit_all}, citations (5y): {cit_5y}")

    # ---------------------------------------------------------------------
    # journal matching in publications table
    # <table id="gsc_a_t">...</table>
    # ---------------------------------------------------------------------
    journal_match_counts: Dict[str, int] = {journal: 0 for journal in journal_list}
    
    article_count = 0

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

            article_count += 1
            
            journal_info = gray_elems[1].get_text(strip=True).lower()
            for journal in journal_list:
                if journal.lower() in journal_info:
                    print(f" Journal match: '{gray_elems[1].get_text(strip=True)}' -> '{journal}'")
                    journal_match_counts[journal] += 1
    
    print(f" Total articles on this page: {article_count}")
    print(" Journal counts:")
    for journal, count in journal_match_counts.items():
        print(f"  • {journal}: {count}")
    print("\n ------------------------------------------")

    return name, institution, research_areas, h_all, h_5y, cit_all, cit_5y, journal_match_counts, article_count

# ========================

# step through all GS pages and scrape profile info

def scrape_profile_all_publications_requests(
    profile_url: str,
    journal_list: List[str],
    session: requests.Session,
    pagesize: int = 100,
    max_pages: int = 50,
    delay: float = 1.0,
    max_block_retries: int = 5,
    block_backoff_base: float = 10.0,
    cache_html: bool = False,
    html_dir: str = "./user/html",
) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    List[str],              # research_areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    Dict[str, int],         # total journal_match_counts across all pages
]:

    name: Optional[str] = None
    institution: Optional[str] = None
    research_areas: List[str] = []
    h_all: Optional[int] = None
    h_5y: Optional[int] = None
    cit_all: Optional[int] = None
    cit_5y: Optional[int] = None

    total_journal_counts: Dict[str, int] = {j: 0 for j in journal_list}
    total_article_count = 0

    any_page = False
    user_id = user_id_from_url(profile_url) or "UNKNOWN"

    # set up cache directory if needed
    if cache_html:
        out_dir = Path(html_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

    for page_idx, html in enumerate(
        iter_scholar_pages_requests(
            profile_url,
            session,
            pagesize=pagesize,
            max_pages=max_pages,
            delay=delay,
            max_block_retries=max_block_retries,
            block_backoff_base=block_backoff_base,
        )
    ):
        any_page = True

        if cache_html:
            page_num = page_idx + 1
            out_path = Path(html_dir) / f"{user_id}_p{page_num}.htm"
            with open(out_path, "w", encoding="utf-8", errors="replace") as f:
                f.write(html)
            print(f"  Cached HTML for {user_id} page {page_num} -> {out_path}")

        (
            page_name,
            page_institution,
            page_research_areas,
            page_h_all,
            page_h_5y,
            page_cit_all,
            page_cit_5y,
            page_journal_counts,
            page_article_count,
        ) = scrape_it(html, journal_list)

        # extract profile-level info from the first page
        if page_idx == 0:
            name = page_name
            institution = page_institution
            research_areas = page_research_areas
            h_all = page_h_all
            h_5y = page_h_5y
            cit_all = page_cit_all
            cit_5y = page_cit_5y

        # accumulate journal counts and article counts
        for j in journal_list:
            total_journal_counts[j] += page_journal_counts.get(j, 0)

        total_article_count += page_article_count
        
    if not any_page:
        print(f"\n Warning - No publication pages scraped for URL: {profile_url}")

    print("\n === Aggregated over ALL publications pages ===")
    print(f" Name: {name}")
    print(f" Institution: {institution}")
    if research_areas:
        print(" Research areas: " + ", ".join(research_areas))
    print(f" h-index (all): {h_all}, h-index (5y): {h_5y}")
    print(f" citations (all): {cit_all}, citations (5y): {cit_5y}")
    print(" Journal counts (all pages):")
    for j, c in total_journal_counts.items():
        print(f"  • {j}: {c}")
    print(f" Total articles (all pages): {total_article_count}")
    print(" =============================================\n")

    return (
        name,
        institution,
        research_areas,
        h_all,
        h_5y,
        cit_all,
        cit_5y,
        total_journal_counts,
        total_article_count
    )

# =========================

# main driver to process a profile

def process_profile(
    url: str,
    journal_list: List[str],
    session: requests.Session,
    pagesize: int = 100,
    cache_html: bool = False,
    html_dir: str = "./html",
) -> Dict[str, object] | None:

    user_id = user_id_from_url(url)
    if not user_id:
        print(f" Warning - Could not extract user id from URL: {url}")
        user_id = "UNKNOWN"

    (
        name,
        institution,
        research_areas,
        h_all,
        h_5y,
        cit_all,
        cit_5y,
        journal_counts,
        article_count,
    ) = scrape_profile_all_publications_requests(
        url,
        journal_list,
        session,
        pagesize=pagesize,
        cache_html=cache_html,
        html_dir=html_dir,
    )

    if (
        name is None
        and institution is None
        and not research_areas
        and all(v == 0 for v in journal_counts.values())
    ):
        print(f" Warning - No meaningful data scraped for URL: {url}")

    record = {
        "url": url,
        "user_id": user_id,
        "name": name or "",
        "institution": institution or "",
        "research_areas": "; ".join(research_areas) if research_areas else "",
        "citations_all": cit_all if cit_all is not None else "",
        "citations_5y": cit_5y if cit_5y is not None else "",
        "h_index_all": h_all if h_all is not None else "",
        "h_index_5y": h_5y if h_5y is not None else "",
        "article_count": article_count if article_count is not None else "",
    }

    record["journal_count_tot"] = sum(journal_counts.values())
    for journal in journal_list:
        record[journal] = journal_counts.get(journal, 0)

    return record

# =========================

# open default web browser to a URL

def open_default_browser(url: str = "https://www.google.com") -> bool:
    try:
        opened = webbrowser.open(url, new=2)
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


# =========================
# main
# =========================

def main():
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(" ~~~~~~ Welcome to Snappy - The Super Neat Academic Profile Parser.py ~~~~~~")
    print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

    # request URL list file name (default is URL_list.txt)
    url_list_file = input(
        " Enter the name of the file containing a list of Google Scholar URLs "
        "or press Enter for default ('URL_list.txt'):\n ~ "
    ) or "URL_list.txt"

    url_list_file = url_list_file
    
    if not os.path.exists(url_list_file):
        print(f" ERROR - {url_list_file} not found in current directory.")
        return

    with open(url_list_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    urls = sanitize_urls(urls)

    if not urls:
        print(" No valid URLs found.")
        return

    # read list of key journal names
    journal_list_file = input(
        " Enter the name of the file containing a list of journal names of interest "
        "or press Enter for default ('journal_list.txt'):\n ~ "
    ) or "journal_list.txt"

    journal_list_file = journal_list_file
    
    if not os.path.exists(journal_list_file):
        print(f" Warning - {journal_list_file} not found. I will not count journal publications.")
        journal_titles: List[str] = []
    else:
        with open(journal_list_file, "r", encoding="utf-8") as f:
            journal_titles = [line.strip() for line in f if line.strip()]

        if not journal_titles:
            print(" Warning - No journal titles found. No journal counts will be recorded.")
        else:
            print(f" Loaded {len(journal_titles)} journal titles from {journal_list_file}")
            for jt in journal_titles:
                print(f"       - {jt}")

    # output file name
    output_file = "snappy_results.csv"

    # test CSV output
    print(f"\n Testing that I can open CSV output file '{output_file}' for writing...")
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f_out:
            pass
    except Exception as e:
        print(
            f" ERROR - Could not open output file '{output_file}' for writing.\n"
            f" You probably have it open in another program."
        )
        print(f" Exception: {type(e).__name__}: {e}\n")
        return

    xlsx_file = output_file.replace(".csv", ".xlsx")
    print(f" Testing that I can open Excel output file '{xlsx_file}' for writing...")
    try:
        with open(xlsx_file, "w", newline="", encoding="utf-8") as f_out:
            pass
    except Exception as e:
        print(
            f" ERROR - Could not open output file '{xlsx_file}' for writing.\n"
            f" You probably have it open in another program."
        )
        print(f" Exception: {type(e).__name__}: {e}\n")
        return

    # html cacheing
    html_dir = "./html"
    cache_html = False

    print("\n Do you want to cache the raw HTML pages to the 'html' directory for debugging/offline use? (y/n): ", end="")
    answer = input().strip().lower()
    if answer == "y":
        cache_html = True
        os.makedirs(html_dir, exist_ok=True)
        print(f" Will cache HTML pages under: {html_dir}")
    else:
        print(" Not caching HTML pages.")

    # start scraping
    print("\n Now scraping the web pages for key research metrics...")
    print("\n ------------------------------------------")

    session = requests.Session()

    results: List[Dict[str, object]] = []
    for url in urls:
        print(f"\n === Processing profile: {url} ===")
        record = process_profile(
            url,
            journal_titles,
            session,
            pagesize=100,
            cache_html=cache_html,
            html_dir=html_dir,
        )
        if record is not None:
            results.append(record)


    if not results:
        print(" No results to write (no profiles scraped).")
        return

    # write results to CSV
    print(f"\n Writing results to CSV file: {output_file} ...")

    fieldnames = [
        "url",
        "user_id",
        "name",
        "institution",
        "research_areas",
        "citations_all",
        "citations_5y",
        "h_index_all",
        "h_index_5y",
        "article_count",
        "journal_count_tot",
    ] + journal_titles

    column_labels: Dict[str, str] = {
        "url": "URL",
        "user_id": "Scholar ID",
        "name": "Name",
        "institution": "Institution",
        "research_areas": "Research Areas",
        "citations_all": "Citations (All)",
        "citations_5y": "Citations (5y)",
        "h_index_all": "H-index (All)",
        "h_index_5y": "H-index (5y)",
        "article_count": "Total Number of Publications",
        "journal_count_tot": "Total Journal Publications",
    }

    for j in journal_titles:
        column_labels[j] = f"Publications in {j}"

    pretty_headers = [column_labels[col] for col in fieldnames]

    with open(output_file, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(pretty_headers)
        for row in results:
            writer.writerow([row.get(col, "") for col in fieldnames])

    # convert CSV to Excel
    print("\n Converting CSV results to Excel format...")
    try:
        df = pd.read_csv(output_file)
        df.to_excel(xlsx_file, index=False)
        print(f" Excel file saved: {xlsx_file}")
    except Exception as e:
        print(f"\n ERROR - Could not convert CSV to Excel. Exception: {type(e).__name__}: {e}")

    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"\n All done! Wrote {len(results)} rows to {output_file} and {xlsx_file}.")
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    # optional: step through URLs in default browser
    print("\n Would you like to step through each of the URLs? (y/n): ", end="")
    choice = input().strip().lower()

    if choice == "y":
        print("\n Stepping through each URL in your default web browser...")
        for url in urls:
            print(f"\n Opening URL: {url}")
            opened = open_default_browser(url)
            if not opened:
                print(" Warning: Could not open browser. Stopping step-through.\n")
                break
            time.sleep(0.2)
            input(" Press Enter to continue to the next URL...")

    print(f"\n All done! The results are in {output_file}. Bye!\n")
    print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")


# =========================
# main entry point
# =========================

if __name__ == "__main__":
    main()

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
from pandas.api.types import is_string_dtype
from bs4 import BeautifulSoup
import re
import string
import random
import argparse

# =========================

# global constants and variables

PUNCT = str.maketrans("", "", string.punctuation)
MAX_BLOCK_RETRIES_DEFAULT = 0
BLOCKING_SUSPECTED = False
FETCH_ONLY_MODE = False
OFFLINE_MODE = False

# =========================
# classes
# =========================

class GSBlockedError(Exception):
    '''Raised when those Google yahoos appear to be blocking requests.'''
    pass

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

# sanitize a single URL to standard format in English!

def sanitize_url(url: str) -> Optional[str]:
    user_id = user_id_from_url(url)
    if user_id:
        sanitized_url = f"https://scholar.google.com/citations?user={user_id}&hl=en"
        return sanitized_url
    else:
        print(f" Warning - Could not extract user id from URL: {url}. sanitize_url failed!")
        return None

# =========================

# deal with GS blocking, CAPTCHA and other antics

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
    delay: float = 5.0,              # normal delay between successful pages
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
                print(f"  HTTP {resp.status_code} suggests rate limiting or temporary block.")
                print("  Stopping pagination for this profile.")
                return

            if resp.status_code != 200:
                print(f"  Error: HTTP {resp.status_code}, stopping.")
                return

            html = resp.text

            # check for CAPTCHA / unusual traffic page
            if looks_like_block_page(html):
                print("  Page looks like a CAPTCHA / 'unusual traffic' block.")
                print("  Stopping pagination for this profile.")
                return

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
        sleep_s = random.uniform(3.0, 12.0)
        print(f" Random (human-like) delay for {sleep_s:.1f} seconds before next page...")
        time.sleep(sleep_s)


# =========================

# I love BeautifulSoup :)~

def scrape_it(html: str, 
              journal_list: List[str], 
              normalized_journal_titles: Dict[str, str],
              page_idx: int,
) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    Optional[List[str]],              # research areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    Dict[str, int]          # journal_match_counts
]:
    global FETCH_ONLY_MODE
    soup = BeautifulSoup(html, "html.parser")

    # ---------------------------------------------------------------------
    # name tag: <div id="gsc_prf_in">Name</div>
    # ---------------------------------------------------------------------
    name = None
    name_div = soup.find("div", id="gsc_prf_in")
    if name_div:
        name = name_div.get_text(strip=True)
    if name:
        print(f"\n Scraping profile page {page_idx + 1} for {name}")
    else:
        print(f"\n Scraping profile {page_idx + 1} for 'UNKNOWN'")

    if page_idx == 0:
        # get the front matter data only on the first page
        # ---------------------------------------------------------------------
        # institution / affiliation tag:
        #   <div class="gsc_prf_il">The University of Excellence and other Buzzwords</div>
        # ---------------------------------------------------------------------
        institution: Optional[str] = None
        inst_divs = soup.find_all("div", class_="gsc_prf_il")
        if inst_divs:
            institution = inst_divs[0].get_text(strip=True)
        print(f"\n Institution: {institution if institution else 'None found'}")

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

    # journal matching on all pages
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
            
            raw_info = gray_elems[1].get_text(strip=True)

            # extract the journal title from the raw info string
            journal_title = extract_journal_name(raw_info)

            # remove punctuation and normalize
            journal_norm = normalize_journal_name(journal_title)

            # compare against normalized journal titles list
            matched_journal = normalized_journal_titles.get(journal_norm)

            if matched_journal:
                print(f" Journal match: '{raw_info}' -> '{matched_journal}'")
                journal_match_counts[matched_journal] += 1

    if not FETCH_ONLY_MODE:                  
        print(f" Total articles on this page: {article_count}")
        print(" Journal match counts on this page:")    
        for journal, count in journal_match_counts.items():
            if count > 0:
                print(f"  {journal}: {count}")

    print("\n ------------------------------------------\n")

    if page_idx == 0:
        return name, institution, research_areas, h_all, h_5y, cit_all, cit_5y, journal_match_counts, article_count
    else:
        return journal_match_counts, article_count

# ========================

# normalize a journal name by cleaning punctuation and whitespace

def normalize_journal_name(name: str) -> str:
    name = name.lower().strip()
    # remove punctuation but keep spaces
    name = name.translate(PUNCT)
    # collapse multiple spaces
    name = re.sub(r"\s+", " ", name)
    return name

# ========================

# extract the journal name from the journal_info field

def extract_journal_name(raw_info: str) -> str:

    s = raw_info.strip()

    # look for the first spot where a volume/pages/year chunk starts.
    # this is usually: space + digit, or comma + space + digit, or space + '(' + digit
    m = re.match(r"^(.*?)(?=\s\d|\s\(\d|,\s*\d{1,4})", s)
    if m:
        return m.group(1).strip()
    return s  # fallback: whole string if no match


# ========================

# step through all GS pages and scrape profile info

def scrape_profile_all_publications_requests(
    profile_url: str,
    journal_list: List[str],
    normalized_journal_titles: Dict[str, str],
    session: requests.Session,
    pagesize: int = 100,
    max_pages: int = 50,
    delay: float = 5.0,
    max_block_retries: int = MAX_BLOCK_RETRIES_DEFAULT,
    block_backoff_base: float = 10.0,
    cache_html: bool = False,
    html_dir: str = "./html",
) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    List[str],              # research_areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    Dict[str, int],         # total journal_match_counts across all pages
    int,                    # total_article_count
    bool                    # any_page
]:
    global FETCH_ONLY_MODE
    global BLOCKING_SUSPECTED

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

        if page_idx == 0:
            # first page - get full profile info
            (
                name,
                institution,
                research_areas,
                h_all,
                h_5y,
                cit_all,
                cit_5y,
                page_journal_counts,
                page_article_count,
            ) = scrape_it(html, journal_list, normalized_journal_titles, page_idx)   
        else:
            # subsequent pages - only get journal and article counts
            (
                page_journal_counts,
                page_article_count,
            ) = scrape_it(html, journal_list, normalized_journal_titles, page_idx)

        # accumulate journal counts and article counts
        for j in journal_list:
            total_journal_counts[j] += page_journal_counts.get(j, 0)

        total_article_count += page_article_count
        
    if not any_page:
        print(f"\n Warning - No publication pages scraped for URL: {profile_url}")
        # likely blocked or unreachable
        BLOCKING_SUSPECTED = True        
        raise GSBlockedError(f"Blocked or no pages for {profile_url}")
    else:
        if not FETCH_ONLY_MODE:
            print("\n === Results aggregated over ALL publications pages ===\n")
            print(f" Name: {name}")
            print(f" Institution: {institution}")
            if research_areas:
                print(" Research areas: " + ", ".join(research_areas))
            print(f" h-index (all): {h_all}, h-index (5y): {h_5y}")
            print(f" citations (all): {cit_all}, citations (5y): {cit_5y}")
            print(f" Total articles (all pages): {total_article_count}")
            print(" Journal match counts (all pages):")
            for journal, count in total_journal_counts.items():
                if count > 0:
                    print(f"  {journal}: {count}")
        print("\n ===============================================================================\n")

    return (
        name,
        institution,
        research_areas,
        h_all,
        h_5y,
        cit_all,
        cit_5y,
        total_journal_counts,
        total_article_count,
        any_page
    )

# =========================

# step through all GS pages and scrape profile info (offline mode)

def scrape_profile_all_publications_offline(
    profile_url: str,
    journal_list: List[str],
    normalized_journal_titles: Dict[str, str],
    html_dir: str = "./html",
    max_pages: int = 50,
) -> Tuple[
    Optional[str],          # name
    Optional[str],          # institution
    List[str],              # research_areas
    Optional[int],          # h_all
    Optional[int],          # h_5y
    Optional[int],          # cit_all
    Optional[int],          # cit_5y
    Dict[str, int],         # total journal_match_counts across all pages
    int,                    # total_article_count
    bool                    # any_page
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

    base_dir = Path(html_dir)
    page_idx = 0

    for page_num in range(1, max_pages + 1):
        path = base_dir / f"{user_id}_p{page_num}.htm"
        if not path.exists():
            # no more cached pages
            break

        any_page = True

        print(f" Loading cached HTML for {user_id} page {page_num} -> {path}")

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()

        if page_idx == 0:
            # first page - get full profile info
            (
                name,
                institution,
                research_areas,
                h_all,
                h_5y,
                cit_all,
                cit_5y,
                page_journal_counts,
                page_article_count,
            ) = scrape_it(html, journal_list, normalized_journal_titles, page_idx)   
        else:
            # subsequent pages - only get journal and article counts
            (
                page_journal_counts,
                page_article_count,
            ) = scrape_it(html, journal_list, normalized_journal_titles, page_idx)

        for j in journal_list:
            total_journal_counts[j] += page_journal_counts.get(j, 0)

        total_article_count += page_article_count
        
        page_idx += 1

    if not any_page:
        print(f"\n Warning - No cached HTML pages found for user_id={user_id} in {html_dir}\n")
    else:
        print("\n === Results aggregated over ALL cached pages (offline) ===\n")
        print(f" Name: {name}")
        print(f" Institution: {institution}")
        if research_areas:
            print(" Research areas: " + ", ".join(research_areas))
        print(f" h-index (all): {h_all}, h-index (5y): {h_5y}")
        print(f" citations (all): {cit_all}, citations (5y): {cit_5y}")
        print(f" Total articles (all pages): {total_article_count}")
        print("\n ===============================================================================\n")

    return (
        name,
        institution,
        research_areas,
        h_all,
        h_5y,
        cit_all,
        cit_5y,
        total_journal_counts,
        total_article_count,
        any_page
    )

# =========================

# fetch and cache profile HTML only

def fetch_and_cache_profile(
    candidate: tuple,
    session: requests.Session,
    pagesize: int,
    html_dir: str,
) -> None:

    print(f"\n === Fetch-only: candidate {candidate.candidate_id}: {candidate.candidate_name} ===")

    url = candidate.gs_url

    if pd.isna(url):
        print("\n  Warning - Empty Google Scholar Link, skipping profile.\n")
        print("\n ===============================================================================\n")
        return

    url_str = str(url).strip()
    print(f"  Attempting to fetch and cache Google Scholar URL: {url_str}")

    sanitized = sanitize_url(url_str)
    if sanitized is None:
        print("\n  Warning - Could not sanitize URL, skipping profile.")
        print("\n ===============================================================================\n")
        return

    # set up dummy journal list and normalized titles to satisfy function signature
    dummy_journal_list: List[str] = []
    dummy_normalized: Dict[str, str] = {}

    _ = scrape_profile_all_publications_requests(
        sanitized,
        dummy_journal_list,
        dummy_normalized,
        session,
        pagesize=pagesize,
        cache_html=True,
        html_dir=html_dir,
    )

# =========================

# main driver to process a profile

def process_profile(
    candidate: tuple,
    journal_list: List[str],
    normalized_journal_titles: Dict[str, str],
    session: requests.Session,
    pagesize: int = 100,
    cache_html: bool = False,
    html_dir: str = "./html",
) -> Dict[str, object] | None:

    global BLOCKING_SUSPECTED
    global OFFLINE_MODE
    info_found = False
    
    print(f"\n === Processing profile for candidate {candidate.candidate_id}: {candidate.candidate_name} ===\n")
    
    url = candidate.gs_url
        
    if pd.isna(url):
        print("\n Warning - Empty Google Scholar Link, skipping profile.")
        record = empty_record( 
            candidate=candidate,
            journal_list=journal_list
        )
        return record
            
    url = sanitize_url(str(url).strip())
    if url is None:
        print("\n Warning - Could not sanitize URL, skipping profile.")
        record = empty_record( 
            candidate=candidate,
            journal_list=journal_list
        )
        return record
    
    if OFFLINE_MODE:
        try:
            print(" Offline mode: parsing cached HTML only (no web requests).")
            (
                gs_name,
                gs_institution,
                gs_research_areas,
                h_all,
                h_5y,
                cit_all,
                cit_5y,
                journal_counts,
                article_count,
                info_found,
            ) = scrape_profile_all_publications_offline(
                url,
                journal_list,
                normalized_journal_titles,
                html_dir=html_dir,
            )
        except Exception as e:
            print(f" Detected error while processing cached HTML for {url}.")
            print(f" Details: {e}")
            return empty_record(candidate=candidate, journal_list=journal_list)
    else:

        try:
            print(f" Attempting to scrape info from Google Scholar URL: {url}")
            (
                gs_name,
                gs_institution,
                gs_research_areas,
                h_all,
                h_5y,
                cit_all,
                cit_5y,
                journal_counts,
                article_count,
                info_found,
            ) = scrape_profile_all_publications_requests(
                url,
                journal_list,
                normalized_journal_titles,
                session,
                pagesize=pagesize,
                cache_html=cache_html,
                html_dir=html_dir,
            )
        except GSBlockedError as e:
            BLOCKING_SUSPECTED = True
            print(f" Detected probable Google Scholar block while processing {url}.")
            print(f" Details: {e}")
            return empty_record(candidate=candidate, journal_list=journal_list)
        except Exception as e:
            print(f" Detected error while processing {url}.")
            print(f" Details: {e}")
            return empty_record(candidate=candidate, journal_list=journal_list)
        
    if (not info_found):
        print(f" Warning - No scrapable pages found for URL: {url}")
        return empty_record(candidate=candidate, journal_list=journal_list)

    if (
        gs_name is None
        and gs_institution is None
        and not gs_research_areas
        and all(v == 0 for v in journal_counts.values())
    ):
        print(f" Warning - No meaningful data scraped for URL: {url}")
        return empty_record(candidate=candidate, journal_list=journal_list)
    
    record = {
        "candidate_id": candidate.candidate_id,
        "candidate_name": candidate.candidate_name,
        "gender": candidate.gender,
        "email": candidate.email,
        "country": candidate.country,
        "current_employee": candidate.current_employee,
        "expertise_area": candidate.expertise_area,
        "academic_level": candidate.academic_level,
        "PhD_year": candidate.PhD_year,
        "gs_url": url,  
        "PhD_institution": candidate.PhD_institution,
        "YNM": "",
        "comments": "",
        "recruiter_notes": "",        
        "gs_name": gs_name or "",
        "gs_institution": gs_institution or "",
        "gs_research_areas": "; ".join(gs_research_areas) if gs_research_areas else "",
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

# return an empty record for failed profiles

def empty_record(
    candidate: tuple, 
    journal_list: List[str]
) -> Dict[str, object]:
   
    print(f"\n Setting empty record \n")
    print("\n ===============================================================================\n")
    record = {
        "candidate_id": candidate.candidate_id,
        "candidate_name": candidate.candidate_name,
        "gender": candidate.gender,
        "email": candidate.email,
        "country": candidate.country,
        "current_employee": candidate.current_employee,
        "expertise_area": candidate.expertise_area,
        "academic_level": candidate.academic_level,
        "PhD_year": candidate.PhD_year,
        "gs_url": "N/A",           
        "PhD_institution": candidate.PhD_institution,
        "YNM": "",
        "comments": "",
        "recruiter_notes": "",     
        "gs_name": "N/A",
        "gs_institution": "N/A",
        "gs_research_areas": "N/A",
        "citations_all": "N/A",
        "citations_5y": "N/A",
        "h_index_all": "N/A",
        "h_index_5y": "N/A",
        "article_count": "N/A",
        "journal_count_tot": "N/A",
    }
    for journal in journal_list:
        record[journal] = "N/A"
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
    
    global OFFLINE_MODE
    global FETCH_ONLY_MODE
    global BLOCKING_SUSPECTED
    
    parser = argparse.ArgumentParser(
        description="Snappy - Super Neat Academic Profile Parser"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--offline",
        action="store_true",
        help="Only parse existing cached HTML in user/html; do not fetch from Google Scholar.",
    )
    mode_group.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only fetch and cache HTML from Google Scholar; do not parse or write outputs.",
    )

    args = parser.parse_args()
    OFFLINE_MODE = args.offline
    FETCH_ONLY_MODE = args.fetch_only
    
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(" ~~~~~~ Welcome to Snappy - The Super Neat Academic Profile Parser.py ~~~~~~")
    print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

    # relative path for input/output files
    # if user is in snappy/src directory, go up one level
    cwd = os.getcwd()
    print(f" Current working directory: {cwd}")
    if cwd.endswith("src"):
        rel_path = "../user/"
    elif cwd.endswith("snappy"):
        rel_path = "./user/"
    elif cwd.endswith("user"):
        rel_path = "./"
    else:
        print(f"\n ERROR - Please run this script from within the 'snappy/user' directory.\n")
        return
    
    # output file name
    output_file = rel_path + "snappy_results.csv"

    # test CSV and XLSX output before we go any further
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

    # give user a chance to invoke these modes if not already specified
    if not OFFLINE_MODE and not FETCH_ONLY_MODE:
        # give user option to run in offline mode
        answer = input(
            "\n Do you want to run in OFFLINE mode (parse cached HTML only)? (y/N): "
        ).strip().lower()
        if answer == "y":
            OFFLINE_MODE = True

        if not OFFLINE_MODE:
            # give user option to run in fetch-only mode
            answer = input(
                "\n Do you want to run in FETCH-ONLY mode (download and cache pages only)? (y/N): "
            ).strip().lower()
            if answer == "y":
                FETCH_ONLY_MODE = True
                
    if OFFLINE_MODE:
        print("\n Running in OFFLINE mode (shhh!): I will not contact Google Scholar.")
        print(" Instead I will parse HTML files already present in the 'html' directory.\n")
    elif FETCH_ONLY_MODE:
        print("\n Running in FETCH-ONLY mode: I will download and cache pages from Google Scholar, ")
        print(" but will not parse or write to output files.\n")
    else:
        print("\n Running in NORMAL mode: I will download and parse Google Scholar profile pages.\n")
                        
    # request HR report name
    hr_report_file = input(
        " Enter the name of the HR report file to process "
        "or press Enter for default ('Campaign_Application_Report.xlsx'):\n "
    ) or "Campaign_Application_Report.xlsx"
    
    hr_report_file = rel_path + hr_report_file.strip()
    
    if not os.path.exists(hr_report_file):
        print(f" ERROR - {hr_report_file} not found in current directory.")
        return    
    
    # convert the report to CSV for easier processing
    print(f"\n Extracting information from '{hr_report_file}' for processing...")
    try:
        df_hr = pd.read_excel(hr_report_file)

        # remove the first two rows
        df_hr = df_hr.iloc[1:].reset_index(drop=True)

        # take the next row as header, clean its newlines, then set as columns
        raw_header = df_hr.iloc[0]

        # convert to string and strip/replace newlines
        clean_header = (
            raw_header
            .astype(str)
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )
        
        # print the header
        print(" Detected HR report columns:")
        for i, col in enumerate(clean_header):
            print(f"  {i + 1:02d}. {col}")

        # use this cleaned row as the header
        df_hr = df_hr[1:].reset_index(drop=True)
        df_hr.columns = clean_header

        # also: just in case, clean any lingering newlines in column names
        df_hr.columns = [
            str(c).replace("\r", " ").replace("\n", " ").strip()
            for c in df_hr.columns
        ]

        # clean up string-like columns to remove newline chars from cell values
        for col in df_hr.columns:
            if is_string_dtype(df_hr[col]):
                df_hr[col] = df_hr[col].map(
                    lambda x: x.replace("\r", " ").replace("\n", " ")
                    if isinstance(x, str) else x
                )

        hr_csv_file = hr_report_file.replace(".xlsx", ".csv")
        df_hr.to_csv(hr_csv_file, index=False)
        print(f" Converted HR report to CSV: {hr_csv_file}")

    except Exception as e:
        print(f" ERROR - Could not convert HR report to CSV. Exception: {type(e).__name__}: {e}")
        return
        
    # rename columns to something manageable and without grammatical errors ... 
    df_hr = df_hr.rename(columns={
        "Candidate Name": "candidate_name",
        "Candidate": "candidate_id",
        "Gender": "gender",
        "Email Address": "email",
        "In what country do you currently reside in?": "country",
        "Are you a student or current employee?": "current_employee",
        "What is your area of expertise?": "expertise_area",
        "What is the Academic Level you are applying for?": "academic_level",
        "Which year did you obtain your PhD? (YYYY)(Required if you have completed a PhD)": "PhD_year",
        "Which Institution did you obtain your PhD from?": "PhD_institution",
        "Google Scholar Link": "gs_url",
        "Would you like to longlist/Shortlist this candidate? Y= Yes M = Maybe N =No": "YNM",
        "Comments": "comments",
        "Recruiter Notes": "recruiter_notes",
    })
   
    print("\n ------------------------------------------\n")

    # read list of key journal names
    journal_list_file = input(
        " Enter the name of the file containing a list of journal names of interest "
        "or press Enter for default ('journal_list.txt'):\n "
    ) or "journal_list.txt"
    
    journal_list_file = rel_path + journal_list_file.strip()
    
    if not os.path.exists(journal_list_file):
        print(f" Warning - {journal_list_file} not found. I will not count journal publications.")
        journal_list: List[str] = []
    else:
        with open(journal_list_file, "r", encoding="utf-8") as f:
            journal_list = [line.strip() for line in f if line.strip()]

        if not journal_list:
            print(" Warning - No journal titles found. No journal counts will be recorded.")
        else:
            print(f" Loaded {len(journal_list)} journal titles from {journal_list_file}")

    normalized_journal_titles = {
        normalize_journal_name(j): j
        for j in journal_list
    }
    
    # html caching
    html_dir = rel_path + "html"
    cache_html = False

    if OFFLINE_MODE:
        # in offline mode we never write HTML, we just read from html_dir
        os.makedirs(html_dir, exist_ok=True)
        print(f"\n Offline mode: I will use cached HTML pages under: {html_dir}")
    else:
        cache_html = True
        os.makedirs(html_dir, exist_ok=True)
        print(f" Normal or Fetch-only mode: I will cache HTML pages under: {html_dir}")

    # ask user to enter the candidate number to start on (default 1)
    start_candidate_num_str = input("\n Enter the candidate number to start processing from (default 1):\n ")
    if not start_candidate_num_str.strip():
        start_candidate_num = 1
    else:
        try:
            start_candidate_num = int(start_candidate_num_str.strip())
        except ValueError:
            print(" Invalid candidate number entered, defaulting to 1.")
            start_candidate_num = 1
    
    if start_candidate_num < 1 or start_candidate_num > len(df_hr):
        print(f" Invalid candidate number {start_candidate_num}, must be between 1 and {len(df_hr)}. Defaulting to 1.")
        start_candidate_num = 1
    print(f" Starting processing from candidate number {start_candidate_num}...\n")
    df_hr = df_hr.iloc[start_candidate_num - 1 :].reset_index(drop=True)
    
    # ask user to enter the candidate number to stop on (default last)
    end_candidate_num_str = input(
        f"\n Enter the candidate number to stop processing on (default {len(df_hr) + start_candidate_num - 1}):\n "
    )
    if not end_candidate_num_str.strip():
        end_candidate_num = len(df_hr) + start_candidate_num - 1
    else:
        try:
            end_candidate_num = int(end_candidate_num_str.strip())
        except ValueError:
            print(f" Invalid candidate number entered, defaulting to {len(df_hr) + start_candidate_num - 1}.")
            end_candidate_num = len(df_hr) + start_candidate_num - 1
    if end_candidate_num < start_candidate_num or end_candidate_num > (len(df_hr) + start_candidate_num - 1):
        print(f" Invalid candidate number {end_candidate_num}, must be between {start_candidate_num} and {len(df_hr) + start_candidate_num - 1}.")
        end_candidate_num = len(df_hr) + start_candidate_num - 1
    print(f" Stopping processing on candidate number {end_candidate_num}...\n")
    df_hr = df_hr.iloc[: end_candidate_num - start_candidate_num + 1].reset_index(drop=True)
    
    # start scraping
    print("\n Now scraping the web pages for key research metrics...\n")
    print("\n ===============================================================================\n")

    session: Optional[requests.Session] = None
    if not OFFLINE_MODE:
        session = requests.Session()

    records: List[Dict[str, object]] = []
    
    for candidate in df_hr.itertuples(index=False):
        
        if FETCH_ONLY_MODE:
            # just fetch and cache HTML, no parsing / records
            if session is None:
                print(" ERROR - Session is None in fetch-only mode. This should not happen.")
                break

            fetch_and_cache_profile(
                candidate=candidate,
                session=session,
                pagesize=100,
                html_dir=html_dir,
            )

        else:
            record = process_profile(
                candidate,
                journal_list,
                normalized_journal_titles,
                session,
                pagesize=100,
                cache_html=cache_html,
                html_dir=html_dir,
            )
            if record is not None:
                records.append(record)
            else:
                print(f" Warning - No record returned for candidate {candidate.candidate_id}.")
                print(f"\n ===============================================================================\n")
                if BLOCKING_SUSPECTED:
                    # give user the option to continue or stop
                    print(f"\n I suspect that Google is blocking web requests. Would you like to continue or stop?")
                    print(f" Note that, if you stop now, you can restart from this candidate number next time.\n Then do a final run in OFFLINE mode to capture all candidates in the spreadsheet.\n")
                    answer = input(" Enter 'c' to continue, 's' to stop processing: ").strip().lower()
                    if answer == "c":
                        print(f"\n Continuing processing... but if this happens again soon I strongly suggest you stop and come back later.\n")
                        BLOCKING_SUSPECTED = False
                        time.sleep(5.0)  # brief pause before continuing
                        continue
                    else:
                        print(f"\n Stopping further processing due to suspected blocking by Google.")
                        print(f" Please try again in an hour or two.")
                        print(f"\n Bye!\n")
                        print(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
                        break

        # random delay between profiles to emulate human behaviour and reduce chance of blocking
        if not OFFLINE_MODE and not FETCH_ONLY_MODE:
            sleep_s = random.uniform(5.0, 12.0)
            print(f" Random (human-like) delay for {sleep_s:.1f} seconds before next profile...")
            time.sleep(sleep_s)
        elif FETCH_ONLY_MODE:
            sleep_s = random.uniform(5.0, 12.0)
            print(f" Random (human-like) delay {sleep_s:.1f} seconds before next fetch...")
            time.sleep(sleep_s)

    if FETCH_ONLY_MODE:
        print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n") 
        print(" Fetch-only mode complete.")
        print(" Cached HTML files (if any) are in the 'html' directory.")
        print(" You can now rerun Snappy in OFFLINE mode to parse them without contacting Google Scholar.\n")
        print(" Bye!\n")       
        return

    if not records:
        print(" No records to write (no profiles scraped). Bye!\n")
        return
    
    # write results to CSV
    print(f"\n Writing records to CSV file: {output_file} ...")
    
    fieldnames = list(records[0].keys()) + journal_list

    column_labels: Dict[str, str] = {
        # HR fields
        "candidate_id": "Candidate ID",
        "candidate_name": "Candidate Name",
        "gender": "Gender",
        "email": "Email Address",
        "country": "Country of Residence",
        "current_employee": "Current Employee / Student",
        "expertise_area": "Expertise Area",
        "academic_level": "Academic Level Applied For",
        "PhD_year": "PhD Completion Year",
        "gs_url": "Google Scholar URL",        # this is the sanitized URL
        "PhD_institution": "PhD Institution",
        "YNM": "Longlist/Shortlist (Yes/No/Maybe)",
        "comments": "Comments",
        "recruiter_notes": "Recruiter Notes",
        # GS fields
        "gs_name": "Google Scholar Name",
        "gs_institution": "Google Scholar Institution",
        "gs_research_areas": "Google Scholar Research Areas",
        "citations_all": "Citations (All)",
        "citations_5y": "Citations (5y)",
        "h_index_all": "H-index (All)",
        "h_index_5y": "H-index (5y)",
        "article_count": "Total Number of Publications",
        "journal_count_tot": "Total Number of Publications in Journal List",
    }

    for j in journal_list:
        column_labels[j] = f"Publications in {j}"

    pretty_headers = [column_labels[col] for col in fieldnames]

    with open(output_file, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(pretty_headers)
        for row in records:
            writer.writerow([row.get(col, "") for col in fieldnames])

    # convert CSV to Excel
    print("\n Converting CSV results to Excel format...")
    try:
        df = pd.read_csv(
            output_file,
            keep_default_na=False,  # important
            na_values=[]            # important
        )
        df.to_excel(xlsx_file, index=False)
        print(f" Excel file saved: {xlsx_file}")

    except Exception as e:
        print(f"\n ERROR - Could not convert CSV to Excel. Exception: {type(e).__name__}: {e}")

    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"\n All done! Wrote {len(records)} rows to {xlsx_file}.")
    print("\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    # optional: step through URLs in default browser
    if not OFFLINE_MODE:
        print("\n Would you like to step through each of the URLs? (y/N): ", end="")
        choice = input().strip().lower()

        if choice == "y":
            print("\n Stepping through each URL in your default web browser...")
            urls = sanitize_urls(df_hr["gs_url"].dropna().astype(str).tolist())
            for url in urls:
                print(f"\n Opening URL: {url}")
                opened = open_default_browser(url)
                if not opened:
                    print(" Warning: Could not open browser. Stopping step-through.\n")
                    break
                time.sleep(0.2)
                input(" Press Enter to continue to the next URL or Ctrl-C to quit...")

    print(f"\n Bye!\n")

# =========================
# main entry point
# =========================

if __name__ == "__main__":
    main()

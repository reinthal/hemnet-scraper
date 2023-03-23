import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper as cs
from dagster import MetadataValue, Output, asset, op
from datetime import datetime
import logging

HEMNET_SEARCH_BOSTADSRATTER_VG = "https://www.hemnet.se/bostader?item_types[]=bostadsratt&location_ids[]=17755"


def initial_search_nr_pages(initial_hemnet_search_start_page):
     soup = BeautifulSoup(initial_hemnet_search_start_page, 'html.parser')
     matches = soup.findAll("div", class_="pagination__item")
     return int(matches[-2].get_text())
    

@asset
def initial_hemnet_search_start_pages():
    logger = logging.getLogger('hemnet_logger')
    """requests all bostadsrätter from hemnet for Västra Götalands Län"""
    scraper = cs.create_scraper()
    search = scraper.get(HEMNET_SEARCH_BOSTADSRATTER_VG)
    
    if search.ok:
        nr_pages = initial_search_nr_pages(search.text)
        entry = {
            "data": search.text,
            "url": HEMNET_SEARCH_BOSTADSRATTER_VG,
            "server_headers": search.headers,
            "status_code": search.status_code,
            "reason": search.reason,
            "date": datetime.now()
        }
        all_pages = [entry]
        for i in range(2, nr_pages+1):
            next_page = HEMNET_SEARCH_BOSTADSRATTER_VG + f"&page_item={i}"            
            next_page_search = scraper.get(next_page)
            if not next_page_search.ok:
                raise Exception(f"Something went wrong when searching for {next_page}. Reason {next_page_search.reason}")
            all_pages.append(
                {
                    "data": next_page_search.text,
                    "url": next_page_search.url,
                    "server_headers": next_page_search.headers,
                    "status_code": search.status_code,
                    "reason": search.reason,
                    "date": datetime.now()
                }
            )
        df = pd.DataFrame(all_pages)
        metadata = {
            "num_records": len(df),
            "preview": MetadataValue.md(df[["url", "reason", "date"]].head(20).to_markdown())
        }
        return Output(value=df, metadata=metadata)
    else:
        raise Exception('Didnt get 200 back from server')

@asset
def hemnet_search_links(initial_hemnet_search_start_pages: pd.DataFrame):
    """takes all the start pages html and computes a new df with all links to pages"""
    
    def find_listing_url(html_text):
        soup = BeautifulSoup(html_text, "html.parser")
        matches = soup.findAll("a", class_="js-listing-card-link listing-card")
        return [m["href"] for m in matches]
    
    initial_hemnet_search_start_pages["listing_urls"] = initial_hemnet_search_start_pages["data"].apply(find_listing_url)
    hemnet_search_links = initial_hemnet_search_start_pages.explode("listing_urls", ignore_index=True)
    metadata = {
        "num_records": len(hemnet_search_links),
        "preview": MetadataValue.md(hemnet_search_links[["listing_urls", "url", "reason", "date"]].head(20).to_markdown())
    }
    return Output(value=hemnet_search_links, metadata=metadata)

@asset
def hemnet_initial_search_links_webpages(hemnet_search_links: pd.DataFrame):
    logger = logging.getLogger("my_logger")
    scraper = cs.create_scraper()
    hemnet_search_links = hemnet_search_links.reset_index() 
    hemnet_initial_search_links_webpages  = []
    for i, row in hemnet_search_links.iterrows():
        url = row["listing_urls"]
        rightnow = datetime.now().strftime("%Y:%H:%M:%S")
        print(f"{rightnow}: {i}, scraping '{url}'")
        resp = scraper.get(url)
        entry = {
            "data": resp.text,
            "url": row["listing_urls"],
            "date": datetime.now(),
            "reason": resp.reason
        }
        hemnet_initial_search_links_webpages.append(entry)
    df = pd.DataFrame(hemnet_initial_search_links_webpages)
    metadata = {
        "num_records": len(df),
        "preview": df[["url", "date", "reason"]].head(20).to_markdown()
    }
    return Output(df, metadata=metadata)

@asset
def hemnet_search_basic_listing_data(hemnet_initial_search_links_webpages: pd.DataFrame):
    """gets the pricing data from the final page"""
    def get_basic_data_from_html(html_text: str) -> dict():
        soup = BeautifulSoup(html_text, "html.parser")
        try:
            price = soup.find("p", class_="property-info__price qa-property-price").get_text()
        except AttributeError:
            price = "" 
        try:
            address = soup.find("h1", class_="qa-property-heading hcl-heading hcl-heading--size2").get_text()
        except AttributeError:
            address = ""
        try:
            location = soup.find("span", class_="property-address__area").get_text()
        except AttributeError:
            location = ""
        return {
            "price": price,
            "address": address,
            "location": location
        }
    entries = []
    for _, row in hemnet_initial_search_links_webpages.iterrows():
        entry = get_basic_data_from_html(row["data"])
        entry["url"] = row["url"]
        entry["reason"] = row["reason"]
        entry["date"] = row["date"]
    df = pd.DataFrame(entries)
    metadata = {
        "num_records": len(df),
        "preview": df[list(df.columns[:4])].head(20).to_markdown()
    }
    return Output(value=df, metadata=metadata)

@asset
def hemnet_search_detailed_listing_data(hemnet_initial_search_links_webpages: pd.DataFrame):
    def get_detailed_data_from_html(html_text: str):
        soup = BeautifulSoup(html_text, "html.parser")
        s = soup.find("div", class_="property-attributes-table")
        entry = {}
        # this parses the table on the main page of the listing
        # it includes some garbage data  which can be removed later
        for k,v in zip(s('dt'), s('dd')):
            entry[k] = v
        return entry
    entries = []
    for _, row in hemnet_initial_search_links_webpages.iterrows():
        entry = get_detailed_data_from_html(row["data"])
        entry["url"] = row["url"]
        entry["reason"] = row["reason"]
        entry["date"] = row["date"]
        entries.append(entry)
    df = pd.DataFrame(entries)
    metadata = {
        "num_records": len(df),
        "preview": df[list(df.columns[:4])].head(20).to_markdown()
    }
    return Output(value=df, metadata=metadata)

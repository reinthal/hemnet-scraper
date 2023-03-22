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
            logger.info(f'{i} {next_page}')
            next_page_search = scraper.get(next_page)
            if not next_page_search.ok:
                raise Exception(f"Something went wrong when searching for {next_page}. Reason {next_page_search.reason}")
            all_pages.append(
                {
                    "data": next_page_search.text,
                    "url": next_page_search,
                    "server_headers": next_page_search.headers,
                    "status_code": search.status_code,
                    "reason": search.reason,
                    "date": datetime.now()
                }
            )
        df = pd.DataFrame(all_pages)
        metadata = {
            "num_records": len(df),
            "preview": MetadataValue.md(df[["data", "url", "reason", "date"]].to_markdown())
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
    hemnet_search_links = initial_hemnet_search_start_pages.explode("listing_urls")
    metadata = {
        "num_records": len(hemnet_search_links),
        "preview": MetadataValue.md(hemnet_search_links[["listing_urls", "url", "reason", "date"]].to_markdown())
    }
    return Output(value=hemnet_search_links, metadata=metadata)
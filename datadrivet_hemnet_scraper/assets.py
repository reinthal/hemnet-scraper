import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper as cs
from dagster import MetadataValue, Output, asset
http_proxy  = "http://127.0.0.1:8080"
https_proxy = http_proxy


# trick cloudflare that we are a mac os x user not python
user_agent = "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0"



proxies = { 
              "http"  : http_proxy, 
              "https" : https_proxy, 
            }

HEMNET_SEARCH = "https://www.hemnet.se/bostader?item_types[]=bostadsratt&location_ids[]=17755"


def initial_search_nr_pages(initial_hemnet_search_start_page):
     soup = BeautifulSoup(initial_hemnet_search_start_page, 'html.parser')
     matches = soup.findAll("div", class_="pagination__item")
     return int(matches[-2].get_text())
    

@asset
def initial_hemnet_search_start_pages():
    scraper = cs.create_scraper()
    search = scraper.get(HEMNET_SEARCH)
    
    if search.ok:
        nr_pages = initial_search_nr_pages(search.text)
        all_pages = [search.text]
        for i in range(2, nr_pages+1):
            next_page = HEMNET_SEARCH + f"&page_item={i}"
            next_page_search = scraper.get(next_page)
            if not next_page_search.ok:
                raise Exception(f"Something went wrong when searching for {next_page}. Reason {next_page_search.reason}")
            all_pages.append(next_page_search.text)
        return all_pages
    else:
        raise Exception('Didnt get 200 back from server')

@asset
def hemnet_search_links(initial_hemnet_search_start_pages: list[str]):
    """takes all the start pages html and return a list of linkslinks to pages"""
    hemnet_search_links = []
    for html_text in initial_hemnet_search_start_pages:
        soup = BeautifulSoup(html_text, "html.parser")
        matches = soup.findAll("a", class_="js-listing-card-link listing-card")
        hemnet_search_links = hemnet_search_links + [m["href"] for m in matches]
    return hemnet_search_links
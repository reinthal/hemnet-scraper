import requests as rq
import pandas as pd
import cloudscraper as cs

from bs4 import BeautifulSoup
from dagster import MetadataValue, Output, asset
from datetime import datetime

HEMNET_SEARCH_BOSTADSRATTER_VG = "https://www.hemnet.se/bostader?item_types[]=bostadsratt&location_ids[]=17755"


def initial_search_nr_pages(initial_hemnet_search_start_page):
     soup = BeautifulSoup(initial_hemnet_search_start_page, 'html.parser')
     matches = soup.findAll("div", class_="pagination__item")
     return int(matches[-2].get_text())
    

@asset(metadata={
    "owner": "datadrivet-test-hemne-aaaajfyiclh3cwe5dzyxjypxvi@knowitcocreate.slack.com",
    "slack": "#datadrivet-test-hemnet-data-owner"
})
def initial_hemnet_search_start_pages() -> pd.DataFrame:
    """
    This asset contains the urls for bostadsrätter on the first page of the following link
    https://www.hemnet.se/bostader?item_types[]=bostadsratt&location_ids[]=17755
    """
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

@asset(metadata={
    "owner": "datadrivet-test-hemne-aaaajfyiclh3cwe5dzyxjypxvi@knowitcocreate.slack.com",
    "slack": "#datadrivet-test-hemnet-data-owner"
})
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

@asset(metadata={
    "owner": "datadrivet-test-hemne-aaaajfyiclh3cwe5dzyxjypxvi@knowitcocreate.slack.com",
    "slack": "#datadrivet-test-hemnet-data-owner"
})
def hemnet_initial_search_links_webpages(hemnet_search_links: pd.DataFrame) -> pd.DataFrame:
    """contains a dataframe with urls from hemnet_search_links and the respective webpage html under the column `data`"""
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

@asset(metadata={
    "owner": "datadrivet-test-hemne-aaaajfyiclh3cwe5dzyxjypxvi@knowitcocreate.slack.com",
    "slack": "#datadrivet-test-hemnet-data-owner"
})
def hemnet_search_basic_listing_data(hemnet_initial_search_links_webpages: pd.DataFrame) -> pd.DataFrame:
    """Gets the pricing data from the listing page, adds `price`, `address` and `location` for a given url"""
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
    df["basic_data_as_json"] = hemnet_initial_search_links_webpages["data"].apply(get_basic_data_from_html)
    metadata = {
        "num_records": len(df),
        "preview": df[["price", "address", "location", "url"]].head(20).to_markdown() # Fetches first 4 columns to be displayed
    }
    return Output(value=df, metadata=metadata)

@asset(metadata={
    "owner": "datadrivet-test-hemne-aaaajfyiclh3cwe5dzyxjypxvi@knowitcocreate.slack.com",
    "slack": "#datadrivet-test-hemnet-data-owner"
})
def hemnet_search_detailed_listing_data(hemnet_initial_search_links_webpages: pd.DataFrame) -> pd.DataFrame:
    """Contains various data like about the listing for each given url, parsed as json"""
    def get_detailed_data_from_html(html_text: str):
        soup = BeautifulSoup(html_text, "html.parser")
        s = soup.find("div", class_="property-attributes-table")
        entry = {}
        # this parses the table on the main page of the listing
        # it includes some garbage data  which can be removed later
        for k,v in zip(s('dt'), s('dd')):
            entry[k.get_text()] = v.get_text()
        return entry
    hemnet_initial_search_links_webpages["data_as_json"] = hemnet_initial_search_links_webpages["data"].apply(get_detailed_data_from_html)
    metadata = {
        "num_records": len(hemnet_initial_search_links_webpages),
        "preview": hemnet_initial_search_links_webpages[["data_as_json", "url"]].head(20).to_markdown()
    }
    return Output(value=hemnet_initial_search_links_webpages, metadata=metadata)

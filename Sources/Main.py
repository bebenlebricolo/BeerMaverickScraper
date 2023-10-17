#!/usr/bin/python3
import sys
import json
from pathlib import Path
import aiohttp

import time
import requests
from requests.adapters import HTTPAdapter

from bs4 import BeautifulSoup
import asyncio
from typing import Any

from threading import Thread

from urllib3 import Retry

from .Models.Hop import Hop
from .Models.Yeast import Yeast
# from .Models import Water
# from .Models import Fermentable

from .ProgressBar import draw_progress_bar, print_buffer

from .BaseScraper import BaseScraper
from .HopScraper import HopScraper
from .YeastScraper import YeastScraper
#from .FermentableScraper import FermentablePageScrapper

class Directories :
    SCRIPT_DIR = Path(__file__).parent
    CACHE_DIR = SCRIPT_DIR.joinpath(".cache")

    @staticmethod
    def ensure_cache_directory_exists():
        if not Directories.CACHE_DIR.exists() :
            Directories.CACHE_DIR.mkdir(parents=True)


def retrieve_links_from_sitemap() -> list[str] :

    result = requests.get("https://beermaverick.com/beerm-sitemap.xml")
    if result.status_code != 200 :
        # Whoops !
        return []

    content = result.content
    soup = BeautifulSoup(content, features="xml")

    all_links : list[str] = []

    for link in soup.find_all("loc", href=False) :
        all_links.append(link.next)
    return all_links

def cache_links(filepath: Path, links : list[str]) :
    with open(filepath, 'w') as file :
        json.dump({"links" : links}, file, indent=4)

def read_links_from_cache(filepath: Path) -> list[str]:
    links : list[str] = []

    if filepath.exists() :
        with open(filepath, 'r') as file :
            links = json.load(file)["links"]

    return links

def scrap_hops_from_website(hop_links : list[str], scraper : HopScraper, multi_threaded : bool = False, max_jobs : int = -1) -> list[Hop] :
    # Seems like running Tasks or threads is roughly equivalent in terms of performances
    # Takes roughly 11-15 seconds for 318 hops with 40 - 100 tasks/threads
    if multi_threaded :
        result = scraper.scrap(hop_links, max_jobs)
        if not result :
            print("Whoops")
    else :
        result = asyncio.run(scraper.scrap_async(hop_links, max_jobs))
        if not result :
            print("Whoops")

    return scraper.hops

def read_hops_from_cache(filepath : Path) -> list[Hop] :
    hops : list[Hop] = []
    if filepath.exists():
        with open(filepath, 'r') as file :
            content = json.load(file)

            for parsed in content["hops"] :
                new_hop = Hop()
                new_hop.from_json(parsed)
                hops.append(new_hop)
    return hops

def write_hops_json_to_disk(filepath : Path, hops : list[Hop]):
    hops_list : list[dict[str, Any]] = []
    for hop in hops :
        hops_list.append(hop.to_json())
    json_content = {
        "hops" : hops_list
    }

    with open(filepath, "w") as file :
        json.dump(json_content, file, indent=4)


def scrap_yeasts_from_website(yeast_links : list[str], scraper : YeastScraper, multi_threaded : bool = False, max_jobs : int = -1) -> list[Yeast] :
    # Seems like running Tasks or threads is roughly equivalent in terms of performances
    # Takes roughly 11-15 seconds for 318 yeasts with 40 - 100 tasks/threads
    if multi_threaded :
        result = scraper.scrap(yeast_links, max_jobs)
        if not result :
            print("Whoops")
    else :
        result = asyncio.run(scraper.scrap_async(yeast_links, max_jobs))
        if not result :
            print("Whoops")
    return scraper.yeasts

def read_yeasts_from_cache(filepath : Path) -> list[Yeast] :
    yeasts : list[Yeast] = []
    if filepath.exists():
        with open(filepath, 'r') as file :
            content = json.load(file)

            for parsed in content["yeasts"] :
                new_yeast = Yeast()
                new_yeast.from_json(parsed)
                yeasts.append(new_yeast)
    return yeasts

def write_yeasts_json_to_disk(filepath : Path, yeasts : list[Yeast]):
    yeasts_list : list[dict[str, Any]] = []
    for yeast in yeasts :
        yeasts_list.append(yeast.to_json())
    json_content = {
        "yeasts" : yeasts_list
    }

    with open(filepath, "w") as file :
        json.dump(json_content, file, indent=4)

async def instantiate_aiohttp_client_async() -> aiohttp.ClientSession :
    return aiohttp.ClientSession()

def report_loop(scraper : BaseScraper[Any], links : list[str]) :
    old_treated_elem_count = 0

    # Give the scraper some time before it actually starts processing anything
    time.sleep(0.5)

    # Dummy line that'll get overwritten
    print("", end="")
    buffer = draw_progress_bar(0)
    print_buffer(buffer)
    while scraper.treated_item < len(links) :

        # New item added event !
        if old_treated_elem_count != scraper.treated_item :
            old_treated_elem_count = scraper.treated_item
            percentage = round(old_treated_elem_count * 100 / len(links))
            buffer = draw_progress_bar(percentage)
            print_buffer(buffer)


def main(args : list[str]):
    max_jobs = int(args[1])
    use_threads = args[2].lower() == "true" if len(args) >= 2 else False
    force = args[3].lower() == "true" if len(args) >= 3 else False

    Directories.ensure_cache_directory_exists()

    link_cached_file = Directories.CACHE_DIR.joinpath("links.json")
    links = read_links_from_cache(link_cached_file)

    if len(links) == 0 :
        links = retrieve_links_from_sitemap()
        cache_links(link_cached_file, links)

    hop_links : list[str] = []
    yeast_links : list[str] = []
    fermentable_links : list[str] = []
    water_links : list[str] = []
    styles_links : list[str] = []

    for link in links :
        if "/hop/" in link:
            hop_links.append(link)
            continue
        if "/fermentable/" in link:
            fermentable_links.append(link)
            continue
        if "/beer-style/" in link:
            styles_links.append(link)
            continue
        if "/water/" in link:
            water_links.append(link)
            continue
        if "/yeast/" in link:
            yeast_links.append(link)
            continue

    sync_http_client = requests.Session()

    # Some retry strategy !
    retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    sync_http_client.adapters.clear()
    sync_http_client.mount("https://", HTTPAdapter(max_retries=retry_strategy))


    hop_scraper = HopScraper(request_client=sync_http_client)
    yeast_scraper = YeastScraper(request_client=sync_http_client)

    ##################################################################
    ########################## Hops parsing ##########################
    ##################################################################

    # Retrieving Hops from cache
    hops : list[Hop] = []
    hops_filepath = Directories.CACHE_DIR.joinpath("hops.json")

    if not force:
        hops = read_hops_from_cache(hops_filepath)
        for hop in hops :
            hop_links.remove(hop.link)

    report_loop_thread : Thread
    if max_jobs != 1 :
        report_loop_thread = Thread(target=report_loop, args=(hop_scraper, yeast_links))
        report_loop_thread.start()

    # Only scrap what's necessary to limit load of the server
    if len(hop_links) > 0 :
        scraped_hops = scrap_hops_from_website(hop_links, hop_scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        hops += scraped_hops
    write_hops_json_to_disk(hops_filepath, hops)

    if max_jobs != 1 :
        report_loop_thread.join() #type: ignore


    ##################################################################
    ######################## Yeasts parsing ##########################
    ##################################################################

    # Share the session
    yeast_scraper.async_client = hop_scraper.async_client

    # Retrieving Yeasts from cache
    yeasts : list[Yeast] = []
    yeasts_filepath = Directories.CACHE_DIR.joinpath("yeasts.json")
    if not force :
        yeasts = read_yeasts_from_cache(yeasts_filepath)
        for yeast in yeasts :
            yeast_links.remove(yeast.link)


    report_loop_thread : Thread
    if max_jobs != 1 :
        report_loop_thread = Thread(target=report_loop, args=(yeast_scraper, yeast_links))
        report_loop_thread.start()

    # Only scrap what's necessary to limit load of the server
    if len(yeast_links) > 0 :
        scraped_yeasts = scrap_yeasts_from_website(yeast_links, yeast_scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        yeasts += scraped_yeasts
    write_yeasts_json_to_disk(yeasts_filepath, yeasts)

    if max_jobs != 1 :
        report_loop_thread.join() #type: ignore

    return 0




if __name__ == "__main__":
    exit(main(sys.argv))


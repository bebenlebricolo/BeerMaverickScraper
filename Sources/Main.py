#!/usr/bin/python3
from dataclasses import dataclass, field
import sys
import json
from pathlib import Path
import argparse

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
    EXTRACTED_DIR = CACHE_DIR.joinpath("extracted")
    PROCESSED_DIR = CACHE_DIR.joinpath("processed")

    @staticmethod
    def ensure_cache_directory_exists():
        Directories.ensure_directory_exists(Directories.CACHE_DIR)

    @staticmethod
    def ensure_directory_exists(dirpath : Path):
        if not dirpath.exists() :
            dirpath.mkdir(parents=True)


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

def scrap_hops(hops_links : list[str], scraper : HopScraper, use_threads : bool = False, max_jobs : int = 0, force : bool = False):
    # Retrieving Hops from cache
    hops : list[Hop] = []
    hops_filepath = Directories.EXTRACTED_DIR.joinpath("hops.json")

    if not force:
        hops = read_hops_from_cache(hops_filepath)
        for hop in hops :
            hops_links.remove(hop.link)

    report_loop_thread : Thread
    if max_jobs != 1 and len(hops_links) > 0 :
        report_loop_thread = Thread(target=report_loop, args=(scraper, hops_links))
        report_loop_thread.start()

    # Only scrap what's necessary to limit load of the server
    if len(hops_links) > 0 :
        scraped_hops = _scrap_hops_from_website(hops_links, scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        hops += scraped_hops
    else :
        print("Hop parsing : no hop to parse, all done !")
    write_hops_json_to_disk(hops_filepath, hops)

    if max_jobs != 1 and len(hops_links) > 0:
        report_loop_thread.join() #type: ignore

def _scrap_hops_from_website(hop_links : list[str], scraper : HopScraper, multi_threaded : bool = False, max_jobs : int = -1) -> list[Hop] :
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


def scrap_yeasts(yeasts_links : list[str], scraper : YeastScraper, use_threads : bool = False, max_jobs : int = 0, force : bool = False) :
    # Retrieving Yeasts from cache
    yeasts : list[Yeast] = []
    yeasts_filepath = Directories.EXTRACTED_DIR.joinpath("yeasts.json")
    if not force :
        yeasts = read_yeasts_from_cache(yeasts_filepath)
        for yeast in yeasts :
            yeasts_links.remove(yeast.link)


    report_loop_thread : Thread
    if max_jobs != 1 and len(yeasts_links) > 0 :
        report_loop_thread = Thread(target=report_loop, args=(scraper, yeasts_links))
        report_loop_thread.start()

    # Only scrap what's necessary to limit load of the server
    if len(yeasts_links) > 0 :
        scraped_yeasts = _scrap_yeasts_from_website(yeasts_links, scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        yeasts += scraped_yeasts
    else :
        print("Yeast parsing : no yeast to parse, all done !")
    write_yeasts_json_to_disk(yeasts_filepath, yeasts)

    if max_jobs != 1 and len(yeasts_links) > 0:
        report_loop_thread.join() #type: ignore

def _scrap_yeasts_from_website(yeast_links : list[str], scraper : YeastScraper, multi_threaded : bool = False, max_jobs : int = -1) -> list[Yeast] :
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

@dataclass
class CategorizedLinks :
    hops : list[str]        = field(default_factory=list[str])
    yeasts : list[str]      = field(default_factory=list[str])
    fermentable : list[str] = field(default_factory=list[str])
    water : list[str]       = field(default_factory=list[str])
    styles : list[str]      = field(default_factory=list[str])


def split_links_by_category(links : list[str]) -> CategorizedLinks :
    cat_links = CategorizedLinks()
    for link in links :
        if "/hop/" in link:
            cat_links.hops.append(link)
            continue
        if "/fermentable/" in link:
            cat_links.fermentable.append(link)
            continue
        if "/beer-style/" in link:
            cat_links.styles.append(link)
            continue
        if "/water/" in link:
            cat_links.water.append(link)
            continue
        if "/yeast/" in link:
            cat_links.yeasts.append(link)
            continue
    return cat_links


def main(args : list[str]):
    bold_ansicode = "\033[1m"
    underline_ansicode = "\033[4m"
    bold_underline = f"{bold_ansicode}{underline_ansicode}"
    normal_ansicode = "\033[0m"
    italic_ansicode = "\033[3m"
    bold_underline_italic = f"{bold_underline}{italic_ansicode}"
    parser = argparse.ArgumentParser(description=f"{bold_underline_italic}BeerMaverick data scraping toolset{normal_ansicode} : "
                                     f"This program automatically performs http requests to the excellent {bold_underline_italic}https://beermaverick.com{normal_ansicode} website "
                                     f"(credits to {bold_ansicode}@Chris Cagle{normal_ansicode} for this) and tries to recover brewing data such as {italic_ansicode}yeasts, hops, water profiles, beer styles and fermentables{normal_ansicode}. "
                                     f"This is very helpful in order to analyse {italic_ansicode}data consistency, broken links, and perform statistical analysis later on{normal_ansicode}."
                                     "    ...   (Yes, I had fun with control characters !)")

    parser.add_argument("-j","--jobs",
                        default=0,
                        required=False,
                        help="Number of jobs to be run in parallel. Set to 0 by default. If let to 0, auto scaling will be performed.")

    parser.add_argument("-t","--thread",
                        required=False,
                        default="False",
                        help="If set, threading will be used instead of async loops.")

    parser.add_argument("-f","--force",
                        required=False,
                        default="False",
                        help="If set, cache directories won't be used and process will reprocess all data as if it was the first time using it.")
    params = parser.parse_args(args[1:])
    max_jobs = int(params.jobs)
    use_threads = params.thread.lower() == "true"
    force = params.force.lower() == "true"

    Directories.ensure_directory_exists(Directories.EXTRACTED_DIR)
    Directories.ensure_directory_exists(Directories.PROCESSED_DIR)

    link_cached_file = Directories.EXTRACTED_DIR.joinpath("links.json")
    links = read_links_from_cache(link_cached_file)

    if len(links) == 0 :
        links = retrieve_links_from_sitemap()
        cache_links(link_cached_file, links)

    # Preprocess links list
    categorized_links = split_links_by_category(links)

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

    scrap_hops(categorized_links.hops, hop_scraper, use_threads, max_jobs, force)

    ##################################################################
    ######################## Yeasts parsing ##########################
    ##################################################################

    # Share the session
    yeast_scraper.async_client = hop_scraper.async_client
    scrap_yeasts(categorized_links.yeasts, yeast_scraper, use_threads, max_jobs, force)

    return 0




if __name__ == "__main__":
    exit(main(sys.argv))


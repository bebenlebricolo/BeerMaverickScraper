#!/usr/bin/python3
from dataclasses import dataclass, field
import sys
import json
from pathlib import Path
import argparse
import uuid

import google.cloud.firestore as fstore         #type: ignore
from google.cloud.exceptions import Conflict    #type: ignore
from google.oauth2 import service_account       #type: ignore
from google.auth.credentials import Credentials #type: ignore

import time
import requests
from requests.adapters import HTTPAdapter

from bs4 import BeautifulSoup
import asyncio
from typing import Any, TypeVar

from threading import Thread

from urllib3 import Retry

from .Models.Hop import Hop
from .Models.Yeast import Yeast
# from .Models import Water
# from .Models import Fermentable

from .Utils.parallel import spread_load_for_parallel
from .Utils.directories import Directories
from .Utils.console import ConsoleChars

from .ProgressBar import draw_progress_bar, print_buffer

from .BaseScraper import BaseScraper
from .HopScraper import HopScraper
from .YeastScraper import YeastScraper
#from .FermentableScraper import FermentablePageScraper


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

def scrap_hops(hops_links : list[str], scraper : HopScraper, use_threads : bool = False, max_jobs : int = 0, force : bool = False) -> list[Hop]:
    # Retrieving Hops from cache
    hops : list[Hop] = []
    hops_filepath = Directories.EXTRACTED_DIR.joinpath("hops.json")

    if not force:
        hops = read_hops_from_cache(hops_filepath)
        for hop in hops :
            hops_links.remove(hop.link)

    # Only scrap what's necessary to limit load of the server
    if len(hops_links) > 0 :
        print("Parsing hops.")
        report_loop_thread : Thread
        progress_accessor = ScraperProgressAccessor(scraper)
        report_loop_thread = Thread(target=report_progress_threaded, args=(progress_accessor, len(hops_links)))
        report_loop_thread.start()

        scraped_hops = _scrap_hops_from_website(hops_links, scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        hops += scraped_hops

        report_loop_thread.join()
    else :
        print("Hop parsing : no hop to parse, all done !")
    write_hops_json_to_disk(hops_filepath, hops)


    return hops

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


class ProgressReportAccessor:
    def get(self) -> int :
        return 0

class ScraperProgressAccessor(ProgressReportAccessor):
    scraper : BaseScraper[Any]
    def __init__(self, scraper: BaseScraper[Any]) -> None:
        self.scraper = scraper

    def get(self) -> int:
        return self.scraper.treated_item

class AsyncSafeCounter(ProgressReportAccessor):
    data : int = 0
    locked : bool = False
    def __init__(self, data : int = 0) -> None:
        self.data = data

    def get(self) -> int:
        return self.data

    def increment(self) :
        while self.locked :
            pass
        self.locked = True
        self.data += 1
        self.locked = False


def scraper_elem_count_accessor(scraper : BaseScraper[Any]) -> int :
    return scraper.treated_item

def scrap_yeasts(yeasts_links : list[str], scraper : YeastScraper, use_threads : bool = False, max_jobs : int = 0, force : bool = False) -> list[Yeast]:
    # Retrieving Yeasts from cache
    yeasts : list[Yeast] = []
    yeasts_filepath = Directories.EXTRACTED_DIR.joinpath("yeasts.json")
    if not force :
        yeasts = read_yeasts_from_cache(yeasts_filepath)
        for yeast in yeasts :
            yeasts_links.remove(yeast.link)



    # Only scrap what's necessary to limit load of the server
    if len(yeasts_links) > 0 :
        print("Parsing yeasts.")

        report_loop_thread : Thread
        progress_accessor = ScraperProgressAccessor(scraper)
        report_loop_thread = Thread(target=report_progress_threaded, args=(progress_accessor, len(yeasts_links)))
        report_loop_thread.start()

        scraped_yeasts = _scrap_yeasts_from_website(yeasts_links, scraper, multi_threaded=use_threads, max_jobs=max_jobs)
        yeasts += scraped_yeasts

        report_loop_thread.join()
    else :
        print("Yeast parsing : no yeast to parse, all done !")
    write_yeasts_json_to_disk(yeasts_filepath, yeasts)


    return yeasts

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

def report_scrap_loop(scraper : BaseScraper[Any], links : list[str]) :
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

    parser = argparse.ArgumentParser(description=f"{ConsoleChars.bdunit_ansi}BeerMaverick data scraping toolset{ConsoleChars.no_ansi} : "
                                     f"This program automatically performs http requests to the excellent {ConsoleChars.bdunit_ansi}https://beermaverick.com{ConsoleChars.no_ansi} website "
                                     f"(credits to {ConsoleChars.bd_ansi}@Chris Cagle{ConsoleChars.it_ansi} for this) and tries to recover brewing data such as {ConsoleChars.it_ansi}yeasts, hops, water profiles, beer styles and fermentables{ConsoleChars.no_ansi}. "
                                     f"This is very helpful in order to analyse {ConsoleChars.it_ansi}data consistency, broken links, and perform statistical analysis later on{ConsoleChars.no_ansi}."
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

    parser.add_argument("-u","--upload",
                        required=False,
                        default="False",
                        help="If set, will try to upload data to distant database, if provided.")

    params = parser.parse_args(args[1:])
    max_jobs = int(params.jobs)
    use_threads = params.thread.lower() == "true"
    force = params.force.lower() == "true"
    upload = params.upload.lower() == "true"

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

    hops = scrap_hops(categorized_links.hops, hop_scraper, use_threads, max_jobs, force)

    ##################################################################
    ######################## Yeasts parsing ##########################
    ##################################################################

    # Share the session
    yeast_scraper.async_client = hop_scraper.async_client
    yeasts = scrap_yeasts(categorized_links.yeasts, yeast_scraper, use_threads, max_jobs, force)


    ##################################################################
    ######################## Yeasts parsing ##########################
    ##################################################################
    for hop in hops :
        if hop.id == "" :
            hop.id = str(uuid.uuid4())

    for hop in hops :
        for i in range(0, len(hop.substitutes)):
            # Data originally contains a link that points to the substitute hop,
            # we'll change that for its UUID instead, which is closer to what we'll find in a regular database
            linked_hop = hop.substitutes[i]
            target = [item for item in hops if item.link == linked_hop]
            if len(target) != 0 :
                hop.substitutes[i] = target[0].id

    write_hops_json_to_disk(Directories.PROCESSED_DIR.joinpath("hops.json"), hops)


    ##################################################################
    ##################### Yeasts post-processing #####################
    ##################################################################

    for yeast in yeasts :
        if yeast.id == "" :
            yeast.id = str(uuid.uuid4())

    for yeast in yeasts :
        for i in range(0, len(yeast.comparable_yeasts)):
            # Data originally contains a link that points to the substitute hop,
            # we'll change that for its UUID instead, which is closer to what we'll find in a regular database
            linked_yeast = yeast.comparable_yeasts[i]
            target = [item for item in yeasts if item.link == linked_yeast]
            if len(target) != 0 :
                yeast.comparable_yeasts[i] = target[0].id
            else :
                # We are stumbling on "bad" links : for some linked yeasts, there are issues with the website api calls and redirection did not work
                # So we'll need to find a solution for that.
                print(f"Yeast : \"{yeast.name}\" with link : {yeast.link} has issues in comparable yeasts : \"{linked_yeast}\"")

    print("Dumping post-processed yeasts to disk.")
    write_yeasts_json_to_disk(Directories.PROCESSED_DIR.joinpath("yeasts.json"), yeasts)
    print("-> Ok.")

    ##################################################################
    ########################### Data upload ##########################
    ##################################################################

    service_account_filepath = Directories.SECRETS_DIR.joinpath("service_account.json")
    if not service_account_filepath.exists() :
        print(f"/!\\ Warning : service account file does not exist at location : {service_account_filepath}. Cannot upload data to remote db.")
        return 1

    # Start bulk upload
    if upload :
        asyncio.run(upload_all_data_async(service_account_filepath,
                                        hops,
                                        yeasts,
                                        max_jobs))
    else :
        print("Upload phase skipped.")

    # Read back data from database
    # doc = asyncio.run(hopsDb.document(hops[0].id).get())
    # new_hop = Hop()
    # new_hop.from_json(doc.to_dict())

    print("Done.")
    return 0


async def upload_all_data_async(sa_filepath : Path, hops : list[Hop], yeasts : list[Yeast], max_jobs : int) :
    print(ConsoleChars.bd("\nData Upload") + ": Acquiring credentials for remote services ...")
    credentials = service_account.Credentials.from_service_account_file() #type: ignore
    fs_client = fstore.AsyncClient("druids-corner-cloud", credentials=credentials)
    hopsDb = fs_client.collection("bmHops")                                         #type: ignore
    yeastsDb = fs_client.collection("bmYeasts")

    print("Uploading data to remote database ...")
    print("Uploading hops ...")
    tasks_input_list = spread_load_for_parallel(hops, max_jobs)
    await upload_bulk_item_dispatch_async(tasks_input_list, hopsDb, len(hops))
    print("-> Ok.")

    print("Uploading yeasts ...")
    tasks_input_list = spread_load_for_parallel(yeasts, max_jobs)
    await upload_bulk_item_dispatch_async(tasks_input_list, yeastsDb, len(yeasts))
    print("-> Ok.")


def report_progress_threaded(accessor : ProgressReportAccessor, total_elem_count : int = 0) :
    old_treated_elem_count = 0

    # Give the scraper some time before it actually starts processing anything
    time.sleep(0.5)

    # Dummy line that'll get overwritten
    print("", end="")
    buffer = draw_progress_bar(0)
    print_buffer(buffer)
    while accessor.get() < total_elem_count :

        # New item added event !
        if old_treated_elem_count != accessor.get() :
            old_treated_elem_count = accessor.get()
            percentage = round(old_treated_elem_count * 100 / total_elem_count)
            buffer = draw_progress_bar(percentage)
            print_buffer(buffer)

T = TypeVar("T", Hop, Yeast)
async def upload_bulk_item_dispatch_async(tasks_input_list : list[list[T]], db : fstore.AsyncCollectionReference, total_elem_count : int = 0) :
    """Dispatches input task matrix to individual async tasks"""
    report_loop_thread : Thread
    async_progress_accessor = AsyncSafeCounter()
    report_loop_thread = Thread(target=report_progress_threaded, args=(async_progress_accessor, total_elem_count))
    report_loop_thread.start()

    async with asyncio.TaskGroup() as tg :
        for inputs in tasks_input_list :
            tg.create_task(upload_item_async(inputs, db, async_progress_accessor))

    report_loop_thread.join()

async def upload_item_async(items : list[T], db : fstore.AsyncCollectionReference, progress_accessor : AsyncSafeCounter) :
    """Uploads a single item list, using internal event loop."""

    # Hints : https://clemfournier.medium.com/make-crud-operations-on-firebase-firestore-in-python-d51ab6aa98af
    for item in items :
        try :
            doc_ref = await db.document(item.id).get()
            if doc_ref.exists :
                # Replace document
                write_option = fstore.WriteOption()
                await db.document(item.id).update(item.to_json(), option=write_option)
            else :
                await db.add(item.to_json(), document_id = item.id)

        except Exception as e:
            # Item already exist
            print(e)
            pass
        # Bump the uploaded items count safely (async safe)
        progress_accessor.increment()





if __name__ == "__main__":
    exit(main(sys.argv))


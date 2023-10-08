#!/usr/bin/python3
import sys
import json

from pathlib import Path
from .HopScraper import HopScraper
from .YeastScraper import YeastPageParser
from .FermentableScraper import FermentablePageScrapper

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

    all_links = []

    for link in soup.find_all("loc", href=False) :
        all_links.append(link.next)

    # table_node = soup.find("table", attrs={"id" : "sitemap"})
    # tbody_node = table_node.find("tbody")
    # for link in tbody_node.find_all("a", href=True) :
    #     all_links.append(link)
    
    return all_links

def cache_links(filepath: Path, links : list[str]) :
    with open(filepath, 'w') as file :
        json.dump({"links" : links}, file, indent=4)

def read_links_from_cache(filepath: Path) -> list[str]:
    links = []
    
    if filepath.exists() :
        with open(filepath, 'r') as file :
            links = json.load(file)["links"]

    return links


def main(args):
    max_threads = 20

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

    
    hop_scrapper = HopScraper()
    hop_scrapper.parse_page


        



    return 0




if __name__ == "__main__":
    exit(main(sys.argv))


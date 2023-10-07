#!/usr/bin/python3

# https://elitedatascience.com/python-web-scraping-libraries
from distutils.log import error
from shutil import ExecError
import requests
from bs4 import BeautifulSoup
import re
import json
from json import JSONEncoder
import sys
import threading



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



def main(args):
    max_threads = 20

    links = retrieve_links_from_sitemap()

    return 0




if __name__ == "__main__":
    exit(main(sys.argv))


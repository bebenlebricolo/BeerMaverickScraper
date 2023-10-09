import aiohttp
import requests
import datetime
from threading import Thread

from typing import Optional

import bs4



from .BaseScraper import BaseScraper, ItemPair
from .Models.Hop import Hop, HopAttribute, hop_attribute_from_str
from .Utils import parallel



class HopScraper(BaseScraper) :
    hops : list[Hop]
    requested_parallel_jobs : int = 1

    def __init__(self, async_client: Optional[aiohttp.ClientSession] = None,
                 request_client: Optional[requests.Session] = None) :
        super().__init__(async_client, request_client)
        self.hops = []
    
    def scrap(self, links: list[str], num_threads: int = -1) -> bool:
        if self.request_client == None :
            print("/!\\ Warning : no session found for synchronous http requests, creating a new one.")
            self.request_client = requests.Session()

        matrix = parallel.spread_load_for_parallel(links, num_threads)
        self.requested_parallel_jobs = len(matrix)
        
        if num_threads == 1 :
            start = datetime.datetime.now()
            print(f"Retrieving hops, running synchronously. Starting at : {self.get_formatted_time()}")
            try :
                error_list = []
                item_list = []
                self.atomic_scrap(links, error_list, item_list)
                if len(error_list) > 0 :
                    print("Caught some issues while retrieving hops from website.")

                self.hops = item_list
            except Exception as e :
                print(f"Caught exception while scraping hops : {e}")
                return False
            print(f"Successfully retrieved hops ! Finished at {self.get_formatted_time()}")
            print(f"Total execution time : {self.get_duration_formatted(start)}")
            return True
        
        error_item_collection : list[list[ItemPair[Hop]]] = []
        output_item_collection : list[list[Hop]] = []

        thread_list : list[Thread] = []
        for sublist in matrix :
            
            # Prepare output data lists and store them in both collections
            error_item_list = []
            output_item_list = []

            # Now that both collection contain the two lists created above, 
            # our thread will work on them (with side effects) and when the thread is joined, new values will be populated within each corresponding list in both collections
            error_item_collection.append(error_item_list)
            output_item_collection.append(output_item_list)

            new_thread = Thread(target=self.atomic_scrap, args=(sublist, error_item_list, output_item_list))
            new_thread.start()
            thread_list.append(new_thread)
            print(f"Spawning new thread with id : {new_thread.ident}.")
        
        joined_threads = []
        
        start_time = datetime.datetime.now()
        while len(joined_threads) != num_threads :
            for thread in thread_list :
                if not thread.is_alive() and not thread.ident in joined_threads:
                    thread.join()
                    joined_threads.append(thread.ident)
                    print(f"Thread {new_thread.ident} has finished its task in {self.get_duration_formatted(start_time)}.")

        # Flattening mislabled yeast collection
        hops : list[Hop] = [item for sublist in output_item_collection for item in sublist]
        error_items : list[Hop] = [item for sublist in error_item_collection for item in sublist]

        # Alert for errors
        if len(error_items) > 0 :
            print("Caught some errors while retrieving data from website : error list is not empty !")

        
        print(f"Retrieved hops from website ! Finished at {self.get_formatted_time()}")
        print(f"Total execution time : {self.get_duration_formatted(start, datetime.datetime.now())}")
        self.hops = hops

        return True

    async def scrap_async(self, links: list[str], num_tasks: int = -1) -> bool:
        matrix = parallel.spread_load_for_parallel(links, num_tasks)

    def atomic_scrap(self, links: list[str], out_error_item_list : list[ItemPair[str]], out_item_list : list[ItemPair[str]]) -> None:
        """Atomic function used by asynchronous executers (threads).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        
        hops : list[Hop] = []
        for link in links :
            error_list : list[str] = []
            
            # Critical error, reject data
            response = self.request_client.get(link)
            if response.status_code != 200 :
                out_error_item_list.append(ItemPair([str(response)], link))
                continue
            
            new_hop = Hop(orig_link=link)
            parser = bs4.BeautifulSoup(response.content, "html.parser")

            name_node = parser.find("h1", attrs={"class" : "entry-title"})
            if not name_node:
                error_list.append(f"Could not retrieve hop name for link {link}")
                # retrieving name from the link itself
                new_hop.name = link.split("/")[-1]

            result = True
            result &= self.parse_basics_section(parser, new_hop, error_list)
            result &= self.parse_origin_section(parser, new_hop, error_list)
            result &= self.parse_flavor_and_aroma_section(parser, new_hop, error_list)

            continue


    def parse_flavor_and_aroma_section(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        headers : list[bs4.Tag] = parser.find_all("h2")
        header = [x for x in headers if "Flavor & Aroma Profile" in x.contents[0]]
        if len(header) == 0 :
            error_list.append("Could not find Flavor & Aroma header")
            return False
        header = header[0]

        next_node = header.find_next_sibling()
        while True :
            # Found tags !
            if "Tags:" in next_node.text :
                break

            hop.flavor_txt += next_node.text + " "
            next_node = next_node.find_next_sibling()
        
        hop.flavor_txt = self.format_text(hop.flavor_txt)

        tags_node = next_node.find("em")
        if not tags_node :
            error_list.append()
            print(f"No tags found on hop {hop.name}")
            return False
        tags : list [bs4.Tag] = tags_node.find_all("a", attrs={"class": "text-muted"})
        for tag in tags :
            hop.tags.append(tag.text.replace("#", ""))

        return True

            
    
    def parse_origin_section(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        headers : list[bs4.Tag] = parser.find_all("h2")
        header = [x for x in headers if "Origin" in x.contents[0]]
        if len(header) == 0 :
            error_list.append("Could not find Origin header")
            return False
        header = header[0]

        nodes_list : list[bs4.Tag] =[]
        next_node = header.find_next()
        while next_node.name != "h2" :
            if next_node.name != "span" :
                nodes_list.append(next_node)
            next_node = next_node.find_next()

        for node in nodes_list :
            hop.origin_txt += node.text + " "
        hop.origin_txt = self.format_text(hop.origin_txt)


        return True

    def parse_basics_section(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        all_th : list[bs4.Tag] = parser.find_all("th")
        required = [
            "Purpose:",
            "Country:",
            "International Code:",
            "Cultivar/Brand ID:"
        ]

        for th in all_th :
            content = th.contents[0]
            if not content in required :
                continue

            parent_node = th.parent
            td_node = parent_node.find("td")
            if content == required[0] :
                a_node = td_node.find("a")
                txt = self.format_text(a_node.contents[0])
                hop.purpose = hop_attribute_from_str(txt)
            
            elif content == required[1] :
                hop.country = td_node.contents[0]
                hop.country = self.format_text(hop.country)
            
            elif content == required[2] :
                hop.international_code = td_node.contents[0]
                hop.international_code = self.format_text(hop.international_code)
            
            elif content == required[3] :
                hop.cultivar_id = td_node.contents[0]
                hop.cultivar_id = self.format_text(hop.cultivar_id)

        return True
            

    
    


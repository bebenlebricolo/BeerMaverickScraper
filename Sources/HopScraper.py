import aiohttp
import requests
import datetime
from threading import Thread
import traceback

from typing import Optional

import bs4



from .BaseScraper import BaseScraper, ItemPair
from .Models.Hop import Hop, HopAttribute, hop_attribute_from_str
from .Models.Ranges import RatioRange, NumericRange

from .Utils import parallel



class HopScraper(BaseScraper) :
    hops : list[Hop]
    error_items : list[ItemPair[str]]
    ok_items : list[ItemPair[Hop]]
    
    requested_parallel_jobs : int = 1

    def __init__(self, async_client: Optional[aiohttp.ClientSession] = None,
                 request_client: Optional[requests.Session] = None) :
        super().__init__(async_client, request_client)
        self.reset()
    
    def reset(self) :
        self.hops = []
        self.error_items = []
        self.ok_items = []
    
    def scrap(self, links: list[str], num_threads: int = -1) -> bool:
        self.reset()
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

            print(f"Successfully retrieved hops ! Finished at {self.get_formatted_time()}")
            print(f"Total execution time : {self.get_duration_formatted(start)}")
            return True
        
        error_item_collection : list[list[ItemPair[Hop]]] = []
        output_item_collection : list[list[Hop]] = []

        print(f"Spawning {num_threads} new threads ...")
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
            #print(f"Spawning new thread with id : {new_thread.ident}.")
        
        joined_threads = []
        
        start_time = datetime.datetime.now()
        while len(joined_threads) != num_threads :
            for thread in thread_list :
                if not thread.is_alive() and not thread.ident in joined_threads:
                    thread.join()
                    joined_threads.append(thread.ident)
                    #print(f"Thread {new_thread.ident} has finished its task.")
        
        print(f"All threads returned, time : {self.get_duration_formatted(start_time)}")

        # Flattening returned items yeast collection
        self.ok_items : list[ItemPair[Hop]] = [item for sublist in output_item_collection for item in sublist]
        self.error_items = [item for sublist in error_item_collection for item in sublist]
        self.hops = [pair.item for pair in self.ok_items]
        warnings = [error for item in self.ok_items for error in item.errors ]

        # Alert for errors
        if len(self.error_items) > 0 :
            print("Caught some errors while retrieving data from website : error list is not empty !")

        if len(warnings) > 0 :
            print("Caught some non critical errors (warnings) while retrieving data from website.")
        
        print(f"Retrieved hops from website ! Finished at {self.get_formatted_time()}")
        print(f"Total execution time : {self.get_duration_formatted(start_time)}")

        return True

    async def scrap_async(self, links: list[str], num_tasks: int = -1) -> bool:
        self.reset()
        matrix = parallel.spread_load_for_parallel(links, num_tasks)

    def atomic_scrap(self, links: list[str], out_error_item_list : list[ItemPair[str]], out_item_list : list[ItemPair[Hop]]) -> None:
        """Atomic function used by asynchronous executers (threads).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        
        for link in links :
            error_list : list[str] = []
            
            # Critical error, reject data
            response = self.request_client.get(link)
            if response.status_code != 200 :
                out_error_item_list.append(ItemPair(item=link, errors=[str(response)]))
                continue
            try:
                new_hop = Hop(orig_link=link)
                parser = bs4.BeautifulSoup(response.content, "html.parser")
                result = self.parse_hop_item_from_page(parser, new_hop, error_list)
                out_item_list.append(ItemPair(item=new_hop, errors=error_list))

            except Exception as e:
                out_error_item_list.append(ItemPair(item=new_hop, errors=error_list))
                traceback.print_exc()


    def parse_hop_item_from_page(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> None :
        name_node = parser.find("h1", attrs={"class" : "entry-title"})
        if not name_node:
            error_list.append(f"Could not retrieve hop name for link {hop.orig_link}")
            # retrieving name from the link itself
            hop.name = hop.orig_link.split("/")[-1]
        else:
            hop.name = self.format_text(name_node.text)

        success = True
        success &= self.parse_basics_section(parser, hop, error_list)
        success &= self.parse_origin_section(parser, hop, error_list)
        success &= self.parse_flavor_and_aroma_section(parser, hop, error_list)
        success &= self.parse_brewing_values(parser, hop, error_list)
        success &= self.parse_beer_style(parser, hop, error_list)
        success &= self.parse_hop_substitution(parser, hop, error_list)

        if not success :
            error_list.append("Some parts of this Hop failed to be read")

    def parse_hop_substitution(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        header_name = "Hop Substitutions"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False
        
        p_node : list[bs4.Tag] = header.find_all_next("p")
        experienced_brewer_node : list[bs4.Tag] = [x for x in p_node if "Experienced brewers have chosen the following hop" in x.text]
        if not experienced_brewer_node :
            error_list.append("Could not isolate substitution list")
            return False
        experienced_brewer_node : bs4.Tag = experienced_brewer_node[0]

        substitution_list_node = experienced_brewer_node.find_next_sibling("ul")
        bullet_nodes : list[bs4.Tag] = substitution_list_node.find_all("li")
        for bullet in bullet_nodes :
            a_node = bullet.find("a")
            hop.substitutes.append(self.format_text(a_node.text))

        return True

    def parse_beer_style(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        header_name = "Beer Styles"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False
        
        style_node = header.find_next_sibling("p")
        
        # Styles seem to be consistently highlighted in bold text
        styles : list[bs4.Tag] = style_node.find_all("b")
        for style in styles :
            hop.beer_styles.append(style.text)
        return True

    def find_header_by_name(self, parser : bs4.BeautifulSoup, name : str) -> Optional[bs4.Tag] :
        headers : list[bs4.Tag] = parser.find_all("h2")
        header = [x for x in headers if name in x.text]
        if len(header) == 0 :
            return None
        return header[0]

    def parse_numeric_range(self, td : bs4.Tag, range : NumericRange, unit_char = "%") -> bool :
        values = td.contents[0].text.rstrip(unit_char).split("-")
        
        if len(values) < 1 or len(values) > 2:
            return False
        
        try :
            if len(values) == 1 :
                range.min = float(values[0].strip())
                range.max = range.min
            if len(values) == 2 :
                range.min = float(values[0].strip())
                range.max = float(values[1].strip())
        # Might fail if one of the values is not convertible to float (happens with some default values)
        # In some cases, numerical values are replaced by the "Unknown" keyword, which makes parsing more difficult
        except :
            return False
        return True
    
    def parse_percentage_value(self, td : bs4.Tag, range : NumericRange) -> bool :
        return self.parse_numeric_range(td, range, "%")    

    def parse_brewing_values(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        table = parser.find("table", attrs={"class" : "brewvalues"})
        if not table :
            error_list.append("Could not parse brewing values")
            return False
    
        tr_nodes :list[bs4.Tag] = table.find_all("tr")
        for node in tr_nodes :
            th = node.find("th")
            td = node.find("td")

            if not th or not td :
                continue

            if  "Alpha Acid % (AA)" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.alpha_acids) :
                    error_list.append("Caught unexpected content for Alpha acids")
                continue
                
            if  "Beta Acid %" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.beta_acids): 
                    error_list.append("Caught unexpected content for Beta acids")
                continue

            if  "Alpha-Beta Ratio" in th.contents[0].text:
                values = td.contents[0].text.split("-")
                if len(values) < 1 or len(values) > 2 :
                    error_list.append("Caught unexpected content for Beta acids")
                    continue
                hop.alpha_beta_ratio.min = values[0].strip()
                hop.alpha_beta_ratio.max = values[1].strip()
                continue
                
            if  "Hop Storage Index (HSI)" in th.contents[0].text:
                values = td.contents[0].text.split("%")
                hop.hop_storage_index = float(values[0].strip())
                continue
            
            if  "Co-Humulone as % of Alpha" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.co_humulone_normalized) :
                    error_list.append("Caught unexpected content for Co-humulone")
                continue

            if "Total Oils (mL/100g)" in th.contents[0].text :
                if not self.parse_numeric_range(td, hop.total_oils, "mL") :
                    error_list.append("Caught invalid format for total oils")
                continue

            if "Myrcene" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.myrcene):
                    error_list.append("Caught invalid format for Myrcene")
                continue

            if "Humulene" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.humulene):
                    error_list.append("Caught invalid format for Humulene")
                continue
            
            if "Caryophyllene" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.caryophyllene):
                    error_list.append("Caught invalid format for Caryophyllene")
                continue
            
            if "Farnesene" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.farnesene):
                    error_list.append("Caught invalid format for Farnesene")
                continue
            
            if "All Others" in th.contents[0].text:
                if not self.parse_percentage_value(td, hop.other) :
                    error_list.append("Caught invalid format for other oils")
                continue
            

        return True

    def parse_flavor_and_aroma_section(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        header_name = "Flavor & Aroma Profile"
        
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        # Listing nodes until finding the next Header
        # because sometimes the Tags node can't be found (on old varieties)
        nodes_to_inspect : list[bs4.Tag] = []
        next_node = header.find_next_sibling()
        while next_node != None and next_node.name != "h2" :
            if next_node.name == "p" :
                nodes_to_inspect.append(next_node)
            next_node = next_node.find_next_sibling()

        for p_node in nodes_to_inspect :
            # Found tags !
            if "Tags:" in p_node.text :
                tags_node = p_node.find("em")
                if not tags_node :
                    error_list.append()
                    print(f"No tags found on hop {hop.name}")
                    return False
                tags : list [bs4.Tag] = tags_node.find_all("a", attrs={"class": "text-muted"})
                for tag in tags :
                    hop.tags.append(tag.text.replace("#", ""))
                break

            hop.flavor_txt += p_node.text + " "
        hop.flavor_txt = self.format_text(hop.flavor_txt)

        return True
    
    def parse_origin_section(self, parser : bs4.BeautifulSoup, hop : Hop, error_list : list[str]) -> bool :
        header_name = "Origin"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

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
            

    
    


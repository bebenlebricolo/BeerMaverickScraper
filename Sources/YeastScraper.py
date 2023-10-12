import requests
import asyncio
import aiohttp
import datetime
from typing import Optional
import traceback
from threading import Thread

import bs4

from .BaseScraper import BaseScraper, ItemPair
from .Models.Yeast import Yeast
from .Models.Ranges import NumericRange
from .Utils import parallel

class YeastScraper(BaseScraper[Yeast]) :
    Yeasts : list[Yeast]
    error_items : list[ItemPair[str]]
    ok_items : list[ItemPair[Yeast]]

    def __init__(self, async_client: Optional[aiohttp.ClientSession] = None,
                 request_client: Optional[requests.Session] = None) :
        super().__init__(async_client, request_client)
        self.reset()

    def reset(self) :
        self.Yeasts = []
        self.error_items = []
        self.ok_items = []

    def scrap(self, links: list[str], num_threads: int = -1) -> bool:
        self.reset()
        if self.request_client == None :
            print("/!\\ Warning : no session found for synchronous http requests, creating a new one.")
            self.request_client = requests.Session()

        matrix = parallel.spread_load_for_parallel(links, num_threads)
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

        error_item_collection : list[list[ItemPair[Yeast]]] = []
        output_item_collection : list[list[ItemPair[Yeast]]] = []

        print(f"Spawning {num_threads} new threads ...")
        thread_list : list[Thread] = []
        for sublist in matrix :
            # Empty list happen when we request e.g. 40 concurrent tasks but we only have 20 items to scrap
            # -> Some tasks have nothing to do ! So skip them.
            # Note : a break instruction might have done it here, but that only works on the assumption that empty lists are contiguous and the last
            # ones of the matrix. It's probably always true, but a continue does it just as well and takes care of unordered lists whereas the break does not.
            if len(sublist) == 0:
                continue

            # Prepare output data lists and store them in both collections
            error_item_list = []
            output_item_list = []

            # Now that both collection contain the two lists created above,
            # our thread will work on them (with side effects) and when the thread is joined, new values will be populated within each corresponding list in both collections
            error_item_collection.append(error_item_list)
            output_item_collection.append(output_item_list)

            new_thread = Thread(target=self.atomic_scrap, args=(sublist, error_item_list, output_item_list)) #type: ignore
            new_thread.start()
            thread_list.append(new_thread)
            #print(f"Spawning new thread with id : {new_thread.ident}.")

        joined_threads  : list[int] = []

        start_time = datetime.datetime.now()
        while len(joined_threads) != num_threads :
            for thread in thread_list :
                if not thread.is_alive() and not thread.ident in joined_threads:
                    thread.join()
                    joined_threads.append(thread.ident)  #type: ignore

        print(f"All threads returned, time : {self.get_duration_formatted(start_time)}")

        # Flattening returned items yeast collection
        self.ok_items : list[ItemPair[Yeast]] = [item for sublist in output_item_collection for item in sublist]
        self.error_items = [item for sublist in error_item_collection for item in sublist] #type: ignore
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
        if self.async_client == None :
            print("/!\\ Warning : no session found for async http requests, creating a new one.")
            self.async_client = aiohttp.ClientSession()

        matrix = parallel.spread_load_for_parallel(links, num_tasks)
        if num_tasks == 1 :
            start = datetime.datetime.now()
            print(f"Retrieving Yeasts, running synchronously. Starting at : {self.get_formatted_time()}")
            try :
                error_list = []
                item_list = []
                self.atomic_scrap(links, error_list, item_list)
                if len(error_list) > 0 :
                    print("Caught some issues while retrieving Yeasts from website.")

                self.Yeasts = item_list
            except Exception as e :
                print(f"Caught exception while scraping Yeasts : {e}")

            print(f"Successfully retrieved Yeasts ! Finished at {self.get_formatted_time()}")
            print(f"Total execution time : {self.get_duration_formatted(start)}")
            return True

        error_item_collection : list[list[ItemPair[Yeast]]] = []
        output_item_collection : list[list[ItemPair[Yeast]]] = []

        print(f"Spawning {num_tasks} new async tasks ...")
        start_time = datetime.datetime.now()
        async with asyncio.TaskGroup() as tg :
            for sublist in matrix :
                # Empty list happen when we request e.g. 40 concurrent tasks but we only have 20 items to scrap
                # -> Some tasks have nothing to do ! So skip them.
                # Note : a break instruction might have done it here, but that only works on the assumption that empty lists are contiguous and the last
                # ones of the matrix. It's probably always true, but a continue does it just as well and takes care of unordered lists whereas the break does not.
                if len(sublist) == 0:
                    continue

                # Prepare output data lists and store them in both collections
                error_item_list = []
                output_item_list = []

                # Now that both collection contain the two lists created above,
                # our thread will work on them (with side effects) and when the thread is joined, new values will be populated within each corresponding list in both collections
                error_item_collection.append(error_item_list)
                output_item_collection.append(output_item_list)

                tg.create_task(coro=self.atomic_scrap_async(sublist, error_item_list, output_item_list))
        await self.async_client.close()
        print(f"All tasks returned, time : {self.get_duration_formatted(start_time)}")

        # Flattening returned items yeast collection
        self.ok_items : list[ItemPair[Yeast]] = [item for sublist in output_item_collection for item in sublist]
        self.error_items = [item for sublist in error_item_collection for item in sublist] #type: ignore
        self.Yeasts = [pair.item for pair in self.ok_items]
        warnings = [error for item in self.ok_items for error in item.errors ]

        # Alert for errors
        if len(self.error_items) > 0 :
            print("Caught some errors while retrieving data from website : error list is not empty !")

        if len(warnings) > 0 :
            print("Caught some non critical errors (warnings) while retrieving data from website.")

        print(f"Retrieved Yeasts from website ! Finished at {self.get_formatted_time()}")
        print(f"Total execution time : {self.get_duration_formatted(start_time)}")

        return True

    async def atomic_scrap_async(self, links: list[str], out_error_item_list : list[ItemPair[Yeast]], out_item_list : list[ItemPair[Yeast]]) -> None:
        """Atomic function used by asynchronous executers (asyncio runner).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        for link in links :
            error_list : list[str] = []

            new_yeast = Yeast(link=link)

            # Critical error, reject data
            response = await self.async_client.get(link) #type: ignore
            if response.status != 200 :
                out_error_item_list.append(ItemPair(item=new_yeast, errors=[str(response)]))
                continue

            try:
                parser = bs4.BeautifulSoup(await response.content.read(), "html.parser")
                self.parse_yeast_item_from_page(parser, new_yeast, error_list)
                out_item_list.append(ItemPair(item=new_yeast, errors=error_list))

            except : # Exception as e :
                out_error_item_list.append(ItemPair(item=new_yeast, errors=error_list))
                traceback.print_exc()

    def atomic_scrap(self, links: list[str], out_error_item_list : list[ItemPair[Yeast]], out_item_list : list[ItemPair[Yeast]]) -> None:
        """Atomic function used by asynchronous executers (threads).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        for link in links :
            error_list : list[str] = []

            new_Yeast = Yeast(link=link)

            # Critical error, reject data
            response = self.request_client.get(link) #type: ignore
            if response.status_code != 200 :
                out_error_item_list.append(ItemPair(item=new_Yeast, errors=[str(response)]))
                continue

            try:
                parser = bs4.BeautifulSoup(response.content, "html.parser")
                self.parse_yeast_item_from_page(parser, new_Yeast, error_list)
                out_item_list.append(ItemPair(item=new_Yeast, errors=error_list))

            except : # Exception as e :
                out_error_item_list.append(ItemPair(item=new_Yeast, errors=error_list))
                traceback.print_exc()

    def parse_yeast_item_from_page(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> None :
        name_node = parser.find("h1", attrs={"class" : "entry-title"})
        if not name_node:
            error_list.append(f"Could not retrieve Yeast name for link {yeast.link}")
            # retrieving name from the link itself
            yeast.name = yeast.link.split("/")[-1]
        else:
            yeast.name = self.format_text(name_node.text)

        success = True
        success &= self.parse_basics_section(parser, yeast, error_list)
        success &= self.parse_description_section(parser, yeast, error_list)
        success &= self.parse_brewing_properties(parser, yeast, error_list)
        # success &= self.parse_flavor_and_aroma_section(parser, Yeast, error_list)
        # success &= self.parse_beer_style(parser, Yeast, error_list)
        # success &= self.parse_Yeast_substitution(parser, Yeast, error_list)

        if not success :
            error_list.append("Some parts of this Yeast failed to be read")

    def parse_Yeast_substitution(self, parser : bs4.BeautifulSoup, Yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Yeast Substitutions"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        p_node : bs4.ResultSet[bs4.PageElement] = header.find_all_next("p")
        experienced_brewer_node : list[bs4.PageElement] = [x for x in p_node if "Experienced brewers have chosen the following Yeast" in x.text] # type: ignore
        if not experienced_brewer_node :
            error_list.append("Could not isolate substitution list")
            return False
        experienced_brewer_node : bs4.PageElement = experienced_brewer_node[0] #type: ignore

        substitution_list_node : bs4.Tag = experienced_brewer_node.find_next_sibling("ul") # type: ignore
        bullet_nodes : list[bs4.Tag] = substitution_list_node.find_all("li")
        for bullet in bullet_nodes :
            a_node : bs4.Tag = bullet.find("a") # type: ignore
            yeast.substitutes.append(self.format_text(a_node.text))

        return True

    def parse_beer_style(self, parser : bs4.BeautifulSoup, Yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Beer Styles"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        style_node = header.find_next_sibling("p")

        # Styles seem to be consistently highlighted in bold text
        styles : list[bs4.Tag] = style_node.find_all("b") #type: ignore
        for style in styles :                             #type: ignore
            Yeast.beer_styles.append(style.text)            #type: ignore
        return True

    def find_header_by_name(self, parser : bs4.BeautifulSoup, name : str) -> Optional[bs4.Tag] :
        headers : list[bs4.Tag] = parser.find_all("h2")
        header = [x for x in headers if name in x.text]
        if len(header) == 0 :
            return None
        return header[0]

    def parse_numeric_range(self, td : bs4.Tag, range : NumericRange, unit_char : str = "%") -> bool :
        values = td.contents[0].text.rstrip(unit_char).split("-")

        if len(values) < 1 or len(values) > 2:
            return False

        try :
            if len(values) == 1 :
                range.min.value = float(values[0].strip())
                range.max.value = range.min.value
            if len(values) == 2 :
                range.min.value = float(values[0].strip())
                range.max.value = float(values[1].strip())
        # Might fail if one of the values is not convertible to float (happens with some default values)
        # In some cases, numerical values are replaced by the "Unknown" keyword, which makes parsing more difficult
        except :
            return False
        return True

    def parse_percentage_value(self, td : bs4.Tag, range : NumericRange) -> bool :
        return self.parse_numeric_range(td, range, "%")

    def parse_brewing_properties(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        table = parser.find("table", attrs={"class" : "brewvalues"})
        if not table :
            error_list.append("Could not parse brewing values")
            return False

        tr_nodes :list[bs4.Tag] = table.find_all("tr") #type: ignore
        for node in tr_nodes :                         #type: ignore
            th : bs4.Tag = node.find("th")             #type: ignore
            td : bs4.Tag = node.find("td")             #type: ignore

            if not th or not td :
                continue

            text = th.contents[0].text
            if  "Alcohol Tolerance" in text :
                value = text
                if "Unknown" == value :
                    yeast.add_parsing_error("No available values for alcohol tolerance.")

                if not self.parse_percentage_value(td, Yeast.alpha_acids) :
                    error_list.append("Caught unexpected content for Alpha acids")
                continue

            if  "Beta Acid %" in text:
                if not self.parse_percentage_value(td, Yeast.beta_acids):
                    error_list.append("Caught unexpected content for Beta acids")
                continue

            if  "Alpha-Beta Ratio" in text:
                values = td.contents[0].text.split("-")
                if len(values) < 1 or len(values) > 2 :
                    error_list.append("Caught unexpected content for Beta acids")
                    continue
                Yeast.alpha_beta_ratio.min.value = values[0].strip()
                Yeast.alpha_beta_ratio.max.value = values[1].strip()
                continue

            if  "Yeast Storage Index (HSI)" in text:
                values = td.contents[0].text.split("%")
                Yeast.Yeast_storage_index = float(values[0].strip())
                continue

            if  "Co-Humulone as % of Alpha" in text:
                if not self.parse_percentage_value(td, Yeast.co_humulone_normalized) :
                    error_list.append("Caught unexpected content for Co-humulone")
                continue

            if "Total Oils (mL/100g)" in text :
                if not self.parse_numeric_range(td, Yeast.total_oils, "mL") :
                    error_list.append("Caught invalid format for total oils")
                continue

            if "Myrcene" in text:
                if not self.parse_percentage_value(td, Yeast.myrcene):
                    error_list.append("Caught invalid format for Myrcene")
                continue

            if "Humulene" in text:
                if not self.parse_percentage_value(td, Yeast.humulene):
                    error_list.append("Caught invalid format for Humulene")
                continue

            if "Caryophyllene" in text:
                if not self.parse_percentage_value(td, Yeast.caryophyllene):
                    error_list.append("Caught invalid format for Caryophyllene")
                continue

            if "Farnesene" in text:
                if not self.parse_percentage_value(td, Yeast.farnesene):
                    error_list.append("Caught invalid format for Farnesene")
                continue

            if "All Others" in text:
                if not self.parse_percentage_value(td, Yeast.other_oils) :
                    error_list.append("Caught invalid format for other oils")
                continue


        return True

    def parse_flavor_and_aroma_section(self, parser : bs4.BeautifulSoup, Yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Flavor & Aroma Profile"

        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        # Listing nodes until finding the next Header
        # because sometimes the Tags node can't be found (on old varieties)
        nodes_to_inspect : list[bs4.Tag] = []
        next_node : bs4.Tag = header.find_next_sibling()     #type: ignore
        while next_node != None and next_node.name != "h2" : #type: ignore
            if next_node.name == "p" :
                nodes_to_inspect.append(next_node)
            next_node = next_node.find_next_sibling()        #type: ignore

        for p_node in nodes_to_inspect :
            # Found tags !
            if "Tags:" in p_node.text :
                tags_node = p_node.find("em")
                if not tags_node :
                    error_list.append(f"No tags found on Yeast {Yeast.name}")
                    return False
                tags : bs4.ResultSet[bs4.PageElement] = tags_node.find_all("a", attrs={"class": "text-muted"}) #type: ignore
                for tag in tags :                                   #type: ignore
                    Yeast.tags.append(tag.text.replace("#", ""))      #type: ignore
                break

            Yeast.flavor_txt += p_node.text + " "
        Yeast.flavor_txt = self.format_text(Yeast.flavor_txt)

        return True

    def parse_description_section(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Description"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        nodes_list : list[bs4.Tag] =[]
        next_node : bs4.Tag = header.find_next_sibling() #type: ignore
        while next_node.name != "h2" :
            if next_node.name == "p" or next_node.find("strong") != None:
                nodes_list.append(next_node)
            next_node = next_node.find_next_sibling() #type: ignore

        for node in nodes_list :
            tags_node = node.find("strong")
            if tags_node != None :
                tags = tags_node.text.replace("#", "").replace("\xa0", "").strip().split()
                for tag in tags :
                    yeast.tags.append(tag.strip())
            else :
                yeast.description += node.text + " "
        yeast.description = self.format_text(yeast.description)


        return True

    def parse_basics_section(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        all_th : list[bs4.Tag] = parser.find_all("th")
        required = [
            "Brand:",
            "Type:",
            "Packet:",
            "Species:",
            "Contains Bacteria?"
        ]

        for th in all_th :
            content = th.contents[0].text
            if not content in required :
                continue

            parent_node = th.parent
            td_node : bs4.Tag = parent_node.find("td") #type: ignore
            if content == "Brand:" :
                yeast.brand = self.format_text(td_node.contents[0].text)

            elif content == "Type:" :
                yeast.type = self.format_text(td_node.contents[0].text)

            elif content == "Packet:" :
                yeast.packaging = self.format_text(td_node.contents[0].text)

            elif content == "Species:" :
                yeast.species = []
                splitted = td_node.contents[0].text.split(",")
                for item in splitted :
                    yeast.species.append(item.strip())

            elif content == "Contains Bacteria?" :
                if td_node.contents[0].text == "Yes" :
                    yeast.has_bacterias = True
                else :
                    yeast.has_bacterias = False

        return True
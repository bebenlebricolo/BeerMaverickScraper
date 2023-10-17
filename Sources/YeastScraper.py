from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from aiohttp_retry import RetryClient, ExponentialRetry

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
    yeasts : list[Yeast]
    error_items : list[ItemPair[str]]
    treated_item : int = 0

    def __init__(self, async_client: Optional[aiohttp.ClientSession] = None,
                 request_client: Optional[requests.Session] = None) :
        super().__init__(async_client, request_client)
        self.reset()

    def reset(self) :
        self.yeasts = []
        self.error_items = []
        self.treated_item = 0

    def scrap(self, links: list[str], num_threads: int = -1) -> bool:
        self.reset()
        if self.request_client == None :
            print("/!\\ Warning : no session found for synchronous http requests, creating a new one.")
            self.request_client = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        self.request_client.adapters.clear()
        self.request_client.mount("https://", HTTPAdapter(max_retries=retry_strategy))

        matrix = parallel.spread_load_for_parallel(links, num_threads)
        if num_threads == 1 :
            start = datetime.datetime.now()
            print(f"Retrieving hops, running synchronously. Starting at : {self.get_formatted_time()}")
            try :
                error_list = []
                item_list = []
                self.atomic_scrap(links, error_list, item_list, monothread=True)
                if len(error_list) > 0 :
                    print("Caught some issues while retrieving hops from website.")

                self.hops = item_list
            except Exception as e :
                print(f"Caught exception while scraping hops : {e}")

            print(f"Successfully retrieved hops ! Finished at {self.get_formatted_time()}")
            print(f"Total execution time : {self.get_duration_formatted(start)}")
            return True

        error_item_collection : list[list[Yeast]] = []
        output_item_collection : list[list[Yeast]] = []

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

        joined_threads  : list[int] = []

        start_time = datetime.datetime.now()
        while len(joined_threads) != num_threads :
            for thread in thread_list :
                if not thread.is_alive() and not thread.ident in joined_threads:
                    thread.join()
                    joined_threads.append(thread.ident)  #type: ignore

        print(f"All threads returned, time : {self.get_duration_formatted(start_time)}")

        # Flattening returned items yeast collection
        self.yeasts : list[Yeast] = [item for sublist in output_item_collection for item in sublist]
        self.error_items = [item for sublist in error_item_collection for item in sublist] #type: ignore

        # Alert for errors
        if len(self.error_items) > 0 :
            print("Caught some errors while retrieving data from website : error list is not empty !")

        if self.has_warnings(self.yeasts):
            print("Caught some non critical errors (warnings) while retrieving data from website.")

        print(f"Retrieved hops from website ! Finished at {self.get_formatted_time()}")
        print(f"Total execution time : {self.get_duration_formatted(start_time)}")

        return True

    def has_warnings(self, yeasts : list[Yeast]) -> bool:
        for yeast in yeasts :
            if yeast.parsing_errors != None :
                return True
        return False

    async def scrap_async(self, links: list[str], num_tasks: int = -1) -> bool:
        self.reset()
        if self.async_client == None :
            print("/!\\ Warning : no session found for async http requests, creating a new one.")
            self.async_client = aiohttp.ClientSession()

        # retry_strategy = Retry(
        #     total=3,
        #     backoff_factor=5,
        #     status_forcelist=[429, 500, 502, 503, 504],
        #     allowed_methods=["HEAD", "GET", "OPTIONS"]
        # )
        # self.async_client.
        # self.request_client.mount("https://", HTTPAdapter(max_retries=retry_strategy))

        matrix = parallel.spread_load_for_parallel(links, num_tasks)
        if num_tasks == 1 :
            start = datetime.datetime.now()
            print(f"Retrieving Yeasts, running synchronously. Starting at : {self.get_formatted_time()}")
            try :
                error_list = []
                item_list : list[Yeast] = []
                await self.atomic_scrap_async(links, error_list, item_list, monothread=True)
                if len(error_list) > 0 :
                    print("Caught some issues while retrieving Yeasts from website.")

                self.yeasts = item_list
            except Exception as e :
                print(f"Caught exception while scraping Yeasts : {e}")

            print(f"Successfully retrieved Yeasts ! Finished at {self.get_formatted_time()}")
            print(f"Total execution time : {self.get_duration_formatted(start)}")
            return True

        error_item_collection : list[list[Yeast]] = []
        output_item_collection : list[list[Yeast]] = []

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
        self.yeasts : list[Yeast] = [item for sublist in output_item_collection for item in sublist]
        self.error_items = [item for sublist in error_item_collection for item in sublist] #type: ignore

        # Alert for errors
        if len(self.error_items) > 0 :
            print("Caught some errors while retrieving data from website : error list is not empty !")

        if self.has_warnings(self.yeasts):
            print("Caught some non critical errors (warnings) while retrieving data from website.")

        print(f"Retrieved Yeasts from website ! Finished at {self.get_formatted_time()}")
        print(f"Total execution time : {self.get_duration_formatted(start_time)}")

        return True

    async def atomic_scrap_async(self, links: list[str], out_error_item_list : list[Yeast], out_item_list : list[Yeast], monothread : bool = False) -> None:
        """Atomic function used by asynchronous executers (asyncio runner).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        for link in links :
            error_list : list[str] = []

            new_yeast = Yeast(link=link)

            if monothread :
                print(f"Parsing link : {link}")

            # Critical error, reject data
            response = await self.async_client.get(link) #type: ignore
            if response.status != 200 :
                new_yeast.add_parsing_error(str(response))
                out_error_item_list.append(new_yeast)

                if monothread :
                    print("-> Failed.")
                self.treated_item += 1
                continue

            try:
                parser = bs4.BeautifulSoup(await response.content.read(), "html.parser")
                self.parse_yeast_item_from_page(parser, new_yeast, error_list)
                out_item_list.append(new_yeast)

                if len(new_yeast.comparable_yeasts) > 0 :
                    for i in range(0, len(new_yeast.comparable_yeasts)) :
                        url = f"https://beermaverick.com{new_yeast.comparable_yeasts[i]}"

                        # This call is being redirected by server, we just want to map the redirected address in lieu and place of
                        # the short url; so that we can use the unique url as a key later to replace each yeast per a unique id in the catalogue.
                        response = await self.async_client.get(url, allow_redirects=False) #type: ignore
                        new_yeast.comparable_yeasts[i] = self.recover_comparable_yeast_link(await response.content.read(),
                                                                    response.headers, #type: ignore
                                                                    response.status,
                                                                    new_yeast.comparable_yeasts[i],
                                                                    new_yeast)
                if monothread :
                    print("-> Success.")
                self.treated_item += 1

            except : # Exception as e :
                out_error_item_list.append(new_yeast)
                traceback.print_exc()
                self.treated_item += 1

    def recover_comparable_yeast_link(self, content : str | bytes, headers : dict[str, str], status_code : int, comparable_yeast : str, yeast : Yeast) -> str:
        if status_code == 301 :
            location = headers["Location"]
            url = f"https://beermaverick.com{location}"
            return url

        # Some of them are not redirected an land on the realpage ... for some reason
        elif status_code == 200 :
            soup = bs4.BeautifulSoup(content, "html.parser")
            raw_link = soup.find("link", attrs={"rel" : "canonical"})
            # We also have false positives here !
            if raw_link.attrs["href"] !=  "https://beermaverick.com/yeasts/" :#type: ignore
                url =  raw_link.attrs["href"] #type: ignore
                return url #type: ignore
            else :
                yeast.add_parsing_error("Caught broken link in comparable yeasts !")
                return comparable_yeast.split("yid=")[-1]
        else :
            yeast.add_parsing_error("Caught broken link in comparable yeasts !")
            return comparable_yeast.split("yid=")[-1]

    def atomic_scrap(self, links: list[str], out_error_item_list : list[Yeast], out_item_list : list[Yeast], monothread : bool = False) -> None:
        """Atomic function used by asynchronous executers (threads).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        for link in links :
            error_list : list[str] = []

            new_yeast = Yeast(link=link)

            if monothread :
                print(f"Parsing link : {link}")

            # Critical error, reject data
            response = self.request_client.get(link) #type: ignore
            if response.status_code != 200 :
                new_yeast.add_parsing_error(str(response))
                out_error_item_list.append(new_yeast)

                if monothread :
                    print("-> Failed.")
                self.treated_item += 1
                continue

            try:
                parser = bs4.BeautifulSoup(response.content, "html.parser")
                self.parse_yeast_item_from_page(parser, new_yeast, error_list)
                out_item_list.append(new_yeast)

                if len(new_yeast.comparable_yeasts) > 0 :
                    for i in range(0, len(new_yeast.comparable_yeasts)) :
                        url = f"https://beermaverick.com{new_yeast.comparable_yeasts[i]}"

                        # This call is being redirected by server, we just want to map the redirected address in lieu and place of
                        # the short url; so that we can use the unique url as a key later to replace each yeast per a unique id in the catalogue.
                        response = self.request_client.get(url, allow_redirects=False) #type: ignore
                        new_yeast.comparable_yeasts[i] = self.recover_comparable_yeast_link(response.content,
                                                                                            response.headers, #type: ignore
                                                                                            response.status_code,
                                                                                            new_yeast.comparable_yeasts[i],
                                                                                            new_yeast)
                if monothread :
                    print("-> Success.")
                self.treated_item += 1

            except : # Exception as e :
                out_error_item_list.append(new_yeast)
                traceback.print_exc()
                self.treated_item += 1


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
        success &= self.parse_beer_style(parser, yeast, error_list)
        success &= self.parse_comparable_yeast(parser, yeast, error_list)

        if not success :
            error_list.append("Some parts of this Yeast failed to be read")

    def parse_comparable_yeast(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Comparable Beer Yeast"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        substitution_list_node : bs4.Tag = header.find_next_sibling("ul") # type: ignore
        bullet_nodes : list[bs4.Tag] = substitution_list_node.find_all("li")
        for bullet in bullet_nodes :
            a_node : bs4.Tag = bullet.find("a") # type: ignore
            yeast.comparable_yeasts.append(self.format_text(a_node.attrs["href"]))

        return True

    def parse_beer_style(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Common Beer Styles"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        textual_node : bs4.Tag = header.find_next_sibling("p") #type: ignore
        style_node :bs4.Tag = textual_node.find_next_sibling("p") #type: ignore

        styles = style_node.text.split(",")
        for style in styles :
            if styles.index(style) == len(styles) - 1 :
                last_ones = style.split("&")
                for last in last_ones :
                    yeast.common_beer_styles.append(last.strip())
            else :
                yeast.common_beer_styles.append(style.strip())

        return True

    def find_header_by_name(self, parser : bs4.BeautifulSoup, name : str) -> Optional[bs4.Tag] :
        headers : list[bs4.Tag] = parser.find_all("h2")
        header = [x for x in headers if name in x.text]
        if len(header) == 0 :
            return None
        return header[0]

    def parse_numeric_range(self, node : bs4.Tag, range : NumericRange, unit_char : str = "%") -> bool :
        values = node.contents[0].text.rstrip(unit_char).split("-")

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

    def farenheit_to_degrees(self, farenheit : float) -> float :
        return (farenheit - 32) * 5/9

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
            value_node : Optional[bs4.Tag] = td.find("small", attrs={"class" : "text-muted bold"}) #type: ignore
            if value_node == None :
                continue

            if  "Alcohol Tolerance" in text :
                value = value_node.text.replace("%", "")
                if "Unknown" == value :
                    yeast.add_parsing_error("No available values for alcohol tolerance.")
                elif "-" in value:
                    yeast.add_parsing_error(f"Caught wrong format for alcohol tolerance : {value}.Trying to cope with it.")
                else:
                    try:
                        yeast.alcohol_tolerance = float(value)
                    except :
                        yeast.add_parsing_error(f"Cannot convert value to float : {value}")

            elif  "Attenuation" in text:
                value = value_node.text.replace("%", "")
                if "Unknown" == value :
                    yeast.add_parsing_error("No available values for attenuation.")
                else :
                    if not self.parse_percentage_value(value_node, yeast.attenuation):
                        error_list.append("Caught unexpected content for attenutation")

            elif "Flocculation" in text:
                yeast.flocculation = value_node.text

            elif "Optimal Temperature" in text:
                if not self.parse_numeric_range(value_node, yeast.optimal_temperature, "° F") :
                    error_list.append("Caught unexpected content for optimal temperatures")
                    continue
                yeast.optimal_temperature.max.value = round(self.farenheit_to_degrees(yeast.optimal_temperature.max.value))
                yeast.optimal_temperature.min.value = round(self.farenheit_to_degrees(yeast.optimal_temperature.min.value))

        return True

    def is_readmore_node(self, tag : bs4.Tag) -> bool:
        if "class" in tag.attrs and "readmore" in tag.attrs["class"] :
            return True
        return False


    def parse_description_section(self, parser : bs4.BeautifulSoup, yeast : Yeast, error_list : list[str]) -> bool :
        header_name = "Description"
        header = self.find_header_by_name(parser, header_name)
        if not header :
            error_list.append(f"Could not find {header_name} header")
            return False

        description_node : bs4.Tag = header.find_next_sibling("p") #type: ignore
        yeast.description = self.format_text(description_node.text)

        tags_node : bs4.Tag = description_node.find_next_sibling("p") #type: ignore
        if self.is_readmore_node(tags_node):
            tags_node = tags_node.find_next_sibling("p") #type: ignore
        tags_strong_node : Optional[bs4.Tag] = tags_node.find("strong") #type: ignore

        # No tags here
        if tags_strong_node == None :
            return False

        tags :list[str] = tags_strong_node.text.replace("#", "").replace("\xa0", "").strip().split()
        for tag in tags :
            yeast.tags.append(tag.strip())

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
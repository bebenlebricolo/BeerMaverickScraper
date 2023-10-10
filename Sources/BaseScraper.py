import aiohttp
import requests

from dataclasses import dataclass, field
from typing import Optional, TypeVar, Generic
from datetime import datetime, timedelta

T= TypeVar("T")

@dataclass
class ItemPair(Generic[T]) :
    item : T
    errors : list[str] = field(default_factory=list)


class BaseScraper(Generic[T]) :
    async_client : Optional[aiohttp.client.ClientSession] = None
    request_client : Optional[requests.Session] = None

    def __init__(self, async_client : Optional[aiohttp.client.ClientSession],
                       request_client : Optional[requests.Session]) -> None:
        self.async_client = async_client
        self.request_client = request_client

    def scrap(self, links : list[str], num_threads : int = -1) -> bool:
        return False

    async def scrap_async(self, links : list[str], num_tasks : int = -1) -> bool:
        return False


    def atomic_scrap(self, links : list[str], out_error_item_list : list[ItemPair[T]], out_item_list : list[ItemPair[T]]) -> None :
        """Atomic function used by asynchronous executers (threads).
           Returns two output lists :
           * out_error_item_list : list of rejected objects (caused by a hard issue, like http connection failing/etc)
           * out_item_list : list of item that could be parsed. Check for the internal error list to see non-critical parsing warnings"""
        return

    def get_time(self) -> datetime :
        return datetime.now()

    def get_formatted_time(self) -> str :
        return datetime.now().strftime("%H:%M:%S")

    def get_duration(self, start : datetime, end : Optional[datetime] = None) -> timedelta :
        if end == None :
            end = datetime.now()
        return end - start

    def get_duration_formatted(self, start : datetime, end : Optional[datetime] = None) -> str :
        if end == None :
            end = datetime.now()

        duration = self.get_duration(start, end)
        minutes = duration.seconds // 60
        seconds = duration.seconds - minutes * 60

        # microseconds in the datetime / timedelta world is another individual counter
        # So there is no disturbance between seconds/minutes/days, etc (because it's based on a singe int counter for seconds and up and another counter for microseconds)
        milliseconds = (duration.microseconds / 1000)
        return f"{minutes} minutes, {seconds} seconds, {milliseconds} milliseconds"

    def format_text(self, text : str) -> str :
        vec = text.split()
        out = " ".join(vec)

        out = out.replace("\n", "")
        out = out.strip()

        return out

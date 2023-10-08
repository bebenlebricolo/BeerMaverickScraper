import aiohttp
import requests

from typing import Optional

class BaseScraper :
    async_client : Optional[aiohttp.client.ClientSession] = None
    request_client : Optional[requests.Session] = None

    def __init__(self, async_client : Optional[aiohttp.client.ClientSession],
                       request_client : Optional[aiohttp.client.ClientSession]) -> None:
        self.async_client = async_client
        self.request_client = request_client

    def scrap(self, links : list[str], num_threads : int = -1) -> bool:
        pass

    async def scrap_async(self, links : list[str], num_tasks : int = -1) -> bool:
        pass


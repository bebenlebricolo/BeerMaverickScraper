from .BaseScraper import BaseScraper
from .Models.Hop import Hop

class HopScraper(BaseScraper) :
    hops : list[Hop]

    def __init__(self) -> None:
        super().__init__()
        self.hops = []
    
    def scrap(self, links: list[str], num_threads: int = -1) -> bool:
        pass

    
    


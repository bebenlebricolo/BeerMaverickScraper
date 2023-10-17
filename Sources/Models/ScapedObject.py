from dataclasses import dataclass, field
from typing import Any, Optional, cast
from .Jsonable import Jsonable

@dataclass
class ScrapedObject(Jsonable) :
    # Id retains the unique identifier of the object and is used later on
    # to map objects on one another
    id : str                             = field(default_factory=str)
    parsing_errors : Optional[list[str]] = None

    def to_json(self) -> dict[str, Any]:
        return {
            "parsingErrors" : self.parsing_errors
        }

    def from_json(self, content: dict[str, Any]) -> None:
        self.parsing_errors = self._read_prop("parsingErrors", content, None)

    def add_parsing_error(self, message : str) :
        if self.parsing_errors == None :
            self.parsing_errors = []

        self.parsing_errors.append(message)

    def __eq__(self, other: object) -> bool:
        other = cast(ScrapedObject,other)
        self = cast(ScrapedObject, self)
        return self.parsing_errors == other.parsing_errors
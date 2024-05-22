from dataclasses import dataclass, field
from enum import Enum, auto


class TrackerType(Enum):
    UDP = auto()
    HTTP = auto()


@dataclass(slots=True)
class ScrapeResult:
    peers: int = 0
    error: str | None = None


@dataclass(slots=True)
class Tracker:
    url: str
    hex_hash_info: str
    type: TrackerType = field(init=False)
    scrape_result: ScrapeResult | None = None

    def __post_init__(self):
        if self.url.startswith("udp"):
            self.type = TrackerType.UDP
        elif self.url.startswith("http"):
            self.type = TrackerType.HTTP
        else:
            raise ValueError(f"Передан неправильный url у трекера. URL - {self.url}")


@dataclass(slots=True)
class Torrent:
    path: str
    trackers: list[Tracker] | None = None

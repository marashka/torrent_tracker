import asyncio

from torrentool.torrent import Torrent as TorrentParser

from torrent_tracker.log_conf import logging
from torrent_tracker.schemas import Torrent, Tracker, TrackerType
from torrent_tracker.scrapers import HTTPScraper, UDPScraper

logger = logging.getLogger(__name__)


class TorrentTracker:

    def __init__(self, http_scraper: HTTPScraper, udp_scraper: UDPScraper):
        self.http_scraper = http_scraper
        self.udp_scraper = udp_scraper

    @staticmethod
    def update_trackers_info(torrent: Torrent) -> None:
        torrent_pars = TorrentParser.from_file(torrent.path)  # TODO сомнительная библиотека(147 звезд)
        trackers = []
        for urls_list in torrent_pars.announce_urls:  # сначала идет основной список, потом back-up списки
            for url in urls_list:
                if "http://retracker.local/" in url:
                    continue
                trackers.append(Tracker(url=url, hex_hash_info=torrent_pars.info_hash))
        torrent.trackers = trackers

    async def update_torrent_info(self, torrent: Torrent) -> None:
        self.update_trackers_info(torrent)

        tasks = []
        if torrent.trackers:
            logger.info(f"У торрента {torrent.path} найдено {len(torrent.trackers)} трекеров")
            for tracker in torrent.trackers:
                logger.info(f"Начинаем scraping трекера {tracker.url} у торрента {torrent.path}")
                if tracker.type == TrackerType.HTTP:
                    http_scrap_task = asyncio.create_task(self.http_scraper.update_scrape_info(tracker))
                    tasks.append(http_scrap_task)
                if tracker.type == TrackerType.UDP:
                    udp_scrap_task = asyncio.create_task(self.udp_scraper.update_scrape_info(tracker))
                    tasks.append(udp_scrap_task)
            await asyncio.gather(*tasks)
        return

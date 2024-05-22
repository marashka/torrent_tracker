import argparse
import asyncio
import pathlib
from pprint import pprint

from httpx import AsyncClient

from torrent_tracker.log_conf import logging
from torrent_tracker.schemas import Torrent
from torrent_tracker.scrapers import HTTPScraper, UDPScraper
from torrent_tracker.torrent_tracker import TorrentTracker

logger = logging.getLogger(__name__)


def create_final_message(torrents: list[Torrent]) -> None:
    # TODO доделать, может к json привести
    with open("result.txt", 'w') as file:
        file.write("=" * 100 + "\n")
        for torrent in torrents:
            pprint(torrent, stream=file)
            file.write("=" * 100 + "\n")


def is_torrent_file(filename: str) -> str:
    """ Функция для проверки файла с расширением .torrent """
    extension = pathlib.Path(filename).suffix
    if extension != '.torrent':
        raise argparse.ArgumentTypeError(f"Файл '{filename}' не является файлом с расширением .torrent")
    return filename


async def main():
    parser = argparse.ArgumentParser(description="Скрипт для трекинга 'torrent' файлов")

    parser.add_argument(
        'torrent_files',
        nargs='+',  # указание, что должен быть передан как минимум один "torrent_file"
        type=is_torrent_file
    )
    args = parser.parse_args()

    torrent_files = args.torrent_files

    tasks = []
    torrents = []
    async with AsyncClient() as client:
        for torrent_file in torrent_files:
            torrent = Torrent(path=torrent_file)
            torrents.append(torrent)
            torrent_tracker = TorrentTracker(
                http_scraper=HTTPScraper(client=client),
                udp_scraper=UDPScraper()
            )
            tasks.append(asyncio.create_task(torrent_tracker.update_torrent_info(torrent=torrent)))

        await asyncio.gather(*tasks)
        create_final_message(torrents)


if __name__ == "__main__":
    logger.info("Старт скрипта")
    asyncio.run(main())
    logger.info("Конец скрипта. Результаты в result.txt")

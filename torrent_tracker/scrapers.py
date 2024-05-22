import asyncio
import random
import re
import struct
from asyncio import DatagramTransport, Event
from asyncio.exceptions import TimeoutError
from typing import Type
from urllib.parse import quote

import bencodepy
from httpx import AsyncClient, HTTPError

from torrent_tracker.log_conf import logging
from torrent_tracker.schemas import ScrapeResult, Tracker
from torrent_tracker.udp_protocols import EchoClientProtocol

logger = logging.getLogger(__name__)


class HTTPScraper:
    RETRY_REQUEST = 5  # TODO вынести в env
    RETRY_INTERVAL_SEC = 2  # TODO вынести в env

    def __init__(self, client: AsyncClient):
        self.client = client

    async def update_scrape_info(self, tracker: Tracker) -> None:
        try:
            params = {"info_hash": quote(bytes.fromhex(tracker.hex_hash_info))}
            url = f"{tracker.url}/scrape"  # https://www.bittorrent.org/beps/bep_0048.html

            for retry in range(self.RETRY_REQUEST):
                try:
                    response = await self.client.get(url=url, params=params)
                    if response.status_code == 200:
                        response = await self.client.get(url=url, params=params)
                        content: bytes = response.content
                        scrape_result = self.parse_scrape_content(content, tracker.hex_hash_info)
                        tracker.scrape_result = scrape_result
                        return
                    else:
                        tracker.scrape_result = ScrapeResult(error=f"Сервер вернул статус={response.status_code}")
                        return
                except HTTPError as exception:
                    logger.warning(f"Возникла ошибка {exception.__class__.__name__} "
                                   f"при запросе к серверу при скрапинге трекера: {tracker}. "
                                   f"Через {self.RETRY_INTERVAL_SEC} секунды будет выполнен повторный запрос.")
                    if retry + 1 == self.RETRY_REQUEST:
                        tracker.scrape_result = ScrapeResult(error=f"Ошибка во время запроса к серверу: "
                                                                   f"{exception.__class__.__name__}. "
                                                                   f"Было {self.RETRY_REQUEST} попыток. С интервалом "
                                                                   f"в {self.RETRY_INTERVAL_SEC} секунды")
        except Exception as exception:
            logger.exception(f"Ошибка во время скрапинга трекера: {tracker}")
            tracker.scrape_result = ScrapeResult(error=f"Ошибка во время скрапинга {exception.__class__.__name__}")

    @staticmethod
    def parse_scrape_content(content: bytes, hex_hash_info: str) -> ScrapeResult:
        result = ScrapeResult()
        decoded_data = bencodepy.decode(content)
        # TODO Покрасивее сделать. Подумать как уйти от байтовых строк.
        files = decoded_data.get(b"files")
        if files:
            try:
                tracker_info = files[bytes.fromhex(hex_hash_info)]
            except KeyError:
                result.error = f"Хеш в ответе не совпал. response = {files}"
                return result
            try:
                seeders: int = tracker_info[b"complete"]
                leechers: int = tracker_info[b"incomplete"]
                peers = seeders + leechers
                result.peers = peers
                return result
            except KeyError:
                result.error = f"Неверный формат ответа от сервера. response = {tracker_info}"
                return result
        result.error = f"Ответ не содержит информации о трекере. response - {content}"
        return result


class UDPScraper:
    RETRY_REQUEST = 5  # TODO вынести в env
    RETRY_INTERVAL_SEC = 2  # TODO вынести в env

    def __init__(self):
        pass

    async def update_scrape_info(self, tracker: Tracker):
        try:
            url, port = self.parse_udp_url(tracker.url)
            transport, protocol = await self.create_echo_client(
                addr=(url, port),
                datagram_protocol=EchoClientProtocol,
                connect_event=Event(),
                scrape_event=Event()
            )

            protocol.connect_transaction_id = random.getrandbits(32)

            for retry in range(self.RETRY_REQUEST):

                transport.sendto(self.create_connect_message(protocol.connect_transaction_id))

                try:
                    await asyncio.wait_for(protocol.connect_event.wait(), timeout=self.RETRY_INTERVAL_SEC)
                    break
                except TimeoutError:
                    logger.warning(f"Сервер не отвечает на connect запрос трекера: {tracker}."
                                   f"Через {self.RETRY_INTERVAL_SEC} секунд будет выполнен повторный запрос.")
            else:
                tracker.scrape_result = ScrapeResult(
                    error=f"Сервер не отвечает на connect запрос, было сделано {self.RETRY_REQUEST} попыток"
                )
                return

            protocol.scrape_transaction_id = random.getrandbits(32)

            scrape_message = self.create_scrape_message(
                protocol.scrape_transaction_id,
                protocol.connection_id,
                tracker.hex_hash_info
            )

            for retry in range(self.RETRY_REQUEST):

                transport.sendto(scrape_message)

                try:
                    await asyncio.wait_for(protocol.scrape_event.wait(), timeout=self.RETRY_INTERVAL_SEC)
                    tracker.scrape_result = protocol.scrape_result
                    return
                except TimeoutError:
                    logger.warning(f"Сервер не отвечает на scrape запрос при скрапинге трекера: {tracker}."
                                   f"Через {self.RETRY_INTERVAL_SEC} секунд будет выполнен повторный запрос.")
            else:
                tracker.scrape_result = ScrapeResult(
                    error=f"Сервер не отвечает на scrape запрос, было сделано {self.RETRY_REQUEST} попыток"
                )
                return

        except Exception as exception:
            logger.exception(f"Ошибка во время скрапинга трекера: {tracker}")
            tracker.scrape_result = ScrapeResult(error=f"Ошибка во время скрапинга {exception.__class__.__name__}")

    @staticmethod
    def create_connect_message(transaction_id: int):
        """
        https://www.bittorrent.org/beps/bep_0015.html#:~:text=size%20of%20packets.-,Connect,-Before%20announcing%20or
        """
        protocol_id = 0x41727101980
        action = 0

        # >: порядок байтов big-endian (порядок байтов, при котором старший байт расположен первым).
        # Q: беззнаковое 64-битное целое число (8 байт).
        # L: беззнаковое 32-битное целое число (4 байта).
        # // transaction_id = transaction_id.to_bytes(4, byteorder="big")

        packet = struct.pack(">QLL", protocol_id, action, transaction_id)
        return packet

    @staticmethod
    def create_scrape_message(transaction_id: int, connection_id: int, hex_hash_info: str) -> bytes:
        """https://www.bittorrent.org/beps/bep_0015.html#:~:text=the%20two%20announces.-,Scrape,-Up%20to%20about"""

        action = 2  # scrape
        hash_info = bytes.fromhex(hex_hash_info)

        packet = struct.pack(">QLL", connection_id, action, transaction_id) + hash_info
        return packet

    @staticmethod
    async def create_echo_client(
            addr,
            datagram_protocol: Type[EchoClientProtocol],
            connect_event: Event,
            scrape_event: Event
    ) -> tuple[DatagramTransport, EchoClientProtocol]:

        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: datagram_protocol(connect_event, scrape_event),
            remote_addr=addr
        )
        return transport, protocol

    @staticmethod
    def parse_udp_url(url_string: str) -> tuple[str, int]:
        pattern = re.compile(r'udp://([^:]+):(\d+)')
        match = pattern.match(url_string)
        url, port = match.group(1), int(match.group(2))
        return url, port

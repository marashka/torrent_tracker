import asyncio
import random
import re
import struct
from asyncio import DatagramTransport, Event
from asyncio.exceptions import TimeoutError
from typing import Type
from urllib.parse import quote, urlparse

import bencodepy
from httpx import AsyncClient, HTTPError

from torrent_tracker.log_conf import logging
from torrent_tracker.schemas import ScrapeResult, Tracker
from torrent_tracker.udp_torrent import EchoClientProtocol

logger = logging.getLogger(__name__)


class HTTPScraper:
    RETRY_REQUEST = 5  # TODO вынести в env
    RETRY_INTERVAL_SEC = 2  # TODO вынести в env

    DEFAULT_PORT = 6881
    ONE_PEER_LEN = 6

    def __init__(self, client: AsyncClient):
        self.client = client

    async def update_scrape_info(self, tracker: Tracker) -> None:
        url = tracker.url
        port = urlparse(url).port if urlparse(url).port else self.DEFAULT_PORT
        params = {
            "info_hash":  quote(bytes.fromhex(tracker.hex_hash_info)),
            "uploaded": 0,
            "downloaded": 0,
            "left": 0,
            "compact": 1,
            "peer_id": '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)]),
            "port": port
        }

        for retry in range(self.RETRY_REQUEST):
            try:
                response = await self.client.get(url=url, params=params)
                if response.status_code == 200:
                    content: bytes = response.content
                    scrape_result = self.parse_scrape_content(content)
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
                    return
                await asyncio.sleep(self.RETRY_INTERVAL_SEC)
            except Exception as exception:
                tracker.scrape_result = ScrapeResult(error=f"Ошибка во время скрапинга {exception.__class__.__name__}")
                return

    def parse_scrape_content(self, content: bytes) -> ScrapeResult:
        result = ScrapeResult()
        decoded_data = bencodepy.decode(content)
        failure_reason = decoded_data.get(b"failure reason")
        if failure_reason:
            result.error = f"Не удалось получить информацию о пирах. Причина - {failure_reason}"
            return result
        peers = decoded_data.get(b"peers")
        if peers:
            result.peers = len(peers) // self.ONE_PEER_LEN  # 6 - размер одного пира в компактном виде
            return result
        peers6 = decoded_data.get(b"peers6")  # для IPv6
        if peers6:
            result.peers = len(peers6) // self.ONE_PEER_LEN
            return result
        result.error = f"Ответ не содержит корректной информации о пирах трекера. response - {decoded_data}"
        return result


class UDPScraper:
    RETRY_REQUEST = 5  # TODO вынести в env
    RETRY_INTERVAL_SEC = 2  # TODO вынести в env

    PROTOCOL_ID = 0x41727101980
    CONNECT_ACTION = 0
    ANNOUNCE_ACTION = 1

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

            try:
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
                    tracker.hex_hash_info,
                    port
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
            finally:
                transport.close()

        except Exception as exception:
            logger.exception(f"Ошибка во время скрапинга трекера: {tracker}")
            tracker.scrape_result = ScrapeResult(error=f"Ошибка во время скрапинга {exception.__class__.__name__}")

    def create_connect_message(self, transaction_id: int):
        """
        https://www.bittorrent.org/beps/bep_0015.html#:~:text=size%20of%20packets.-,Connect,-Before%20announcing%20or
        """

        # >: порядок байтов big-endian (порядок байтов, при котором старший байт расположен первым).
        # Q: беззнаковое 64-битное целое число (8 байт).
        # L: беззнаковое 32-битное целое число (4 байта).
        # // transaction_id = transaction_id.to_bytes(4, byteorder="big")

        packet = struct.pack(">QLL", self.PROTOCOL_ID, self.CONNECT_ACTION, transaction_id)
        return packet

    def create_scrape_message(self, transaction_id: int, connection_id: int, hex_hash_info: str, port: int) -> bytes:
        """https://www.bittorrent.org/beps/bep_0015.html#:~:text=the%20two%20announces.-,Scrape,-Up%20to%20about"""

        hash_info = bytes.fromhex(hex_hash_info)

        peer_id = '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)])
        downloaded = 0
        left = 0
        uploaded = 0
        event = 0
        ip = 0
        key = 0
        num_want = -1

        packet = struct.pack(
            ">QLL20s20sQQQLLLlH",
            connection_id, self.ANNOUNCE_ACTION, transaction_id, hash_info, bytes(peer_id, 'utf-8'),
            downloaded, left, uploaded, event, ip, key, num_want, port)

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

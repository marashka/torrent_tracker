import asyncio
import struct
from asyncio import Event

from torrent_tracker.log_conf import logging
from torrent_tracker.schemas import ScrapeResult

logger = logging.getLogger(__name__)


class EchoClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, connect_event: Event, scrape_event: Event):
        self.connection_id: int | None = None
        self.connect_transaction_id: int | None = None
        self.scrape_transaction_id: int | None = None
        self.connect_event = connect_event
        self.scrape_event = scrape_event
        self.scrape_result: ScrapeResult | None = None

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        logger.info(f"Пришло сообщение на сокет addr:{addr} - data:{data}")
        if self.connection_id is None:
            # https://www.bittorrent.org/beps/bep_0015.html#:~:text=size%20of%20packets.-,Connect,-Before%20announcing%20or
            try:
                _, transaction_id, connection_id = struct.unpack(">LLQ", data)
                if transaction_id == self.connect_transaction_id:
                    self.connection_id = connection_id
                    logger.info(
                        f"Подтверждено подключение transaction_id={transaction_id}. "
                        f"Создано connection_id={self.connection_id}"
                    )
                    self.connect_event.set()
                else:
                    logger.warning(f"Не совпал transaction_id в ответе во время connect запроса"
                                   f"{transaction_id} != {self.connect_transaction_id}")
            except Exception as exception:
                logger.warning(f"Ошибка во время обработки ответа сервера {data}: {exception.__class__.__name__}")
                self.scrape_result = ScrapeResult(error=f"Ошибка во время обработки ответа сервера {data}")
                self.connect_event.set()
                self.scrape_event.set()
        else:
            # https://www.bittorrent.org/beps/bep_0015.html#:~:text=the%20two%20announces.-,Scrape,-Up%20to%20about
            try:
                _, transaction_id, seeders, _, leechers = struct.unpack(">LLLLL", data)
                if transaction_id == self.scrape_transaction_id:
                    self.scrape_result = ScrapeResult(peers=seeders+leechers)
                    self.scrape_event.set()
                else:
                    logger.warning(f"Не совпал transaction_id в ответе во время scrape запроса"
                                   f"{transaction_id} != {self.connect_transaction_id}")
            except Exception as exception:
                logger.warning(f"Ошибка во время обработки ответа сервера {data}: {exception.__class__.__name__}")
                self.scrape_result = ScrapeResult(error=f"Ошибка во время обработки ответа сервера {data}")
                self.scrape_event.set()

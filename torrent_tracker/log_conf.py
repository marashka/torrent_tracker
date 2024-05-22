import logging
from datetime import datetime

from pytz import timezone

timezone = timezone("Europe/Moscow")


logging.Formatter.converter = lambda *args: datetime.now(timezone).timetuple()
log_level = logging.INFO
log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
logging.basicConfig(level=log_level, format=log_format)


print(datetime.now())

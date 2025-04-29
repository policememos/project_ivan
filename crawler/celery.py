import logging
from celery import Celery
from crawler.conf import settings



app = Celery(
    'spiders',
    include=[
        'spiders.tasks',
    ],
)

app.config_from_object(settings)

formatter = logging.Formatter(
    '[%(asctime)s: %(levelname)s/%(name)s] %(message)s'
)
logger = logging.getLogger()

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(settings.LOG_LEVEL)  # type: ignore
logger.addHandler(stream_handler)

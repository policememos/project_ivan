import logging
import random
import yaml

from datetime import datetime
from spiders.mongo_utils import get_mongo_bots_client

logger = logging.getLogger('scrapy.spiders.ticketland.helpers')

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def get_free_account():
    if client := get_mongo_bots_client():
        sources = client[cfg['mongo_bots']['sources_db']]
        try:
            return random.choice(list(sources['ticketland'].find({})))
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_free_account:', exc_info=True)
        finally:
            client.close()
    return None


def dump_cookies(cookies, acc):
    if client := get_mongo_bots_client():
        sources = client[cfg['mongo_bots']['sources_db']]
        try:
            sources['ticketland'].update_one(
                {'email': acc['email']},
                {'$set': {'cookies': cookies, 'init_time': datetime.now()}}
            )
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в dump_cookies:', exc_info=True)
        finally:
            client.close()

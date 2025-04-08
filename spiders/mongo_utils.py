import logging
import pymongo
import yaml
from uuid import uuid4

cfg = yaml.safe_load(open("config.yml"))
logger = logging.getLogger('spiders.mongo_utils')

def get_client():
    try:
        return pymongo.MongoClient(cfg["mongo"]["uri"])
    except Exception as err:
        logger.error('Ошибка mongodb', exc_info=True)
    return None


def save_data():
    if client := get_client():
        try:
            db = client['test']
            cl = db['col_test']
            cl.insert_one({
                '_id': str(uuid4()),
                'data': 'writed by TUF'
            })
        except Exception as err:
            logger.error('Ошибка записи mongo: %s', err)
        finally:
            client.close()


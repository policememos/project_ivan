import logging
from datetime import datetime, timedelta
import pymongo

from crawler.conf import settings

logger = logging.getLogger('scrapy.mongo_utils')
logging.getLogger('pika').setLevel(logging.INFO)


def get_mongo_bots_client():
    try:
        return pymongo.MongoClient(
            settings.MONGO_BOTS_URI,
            ssl=settings.MONGO_BOTS_SSL,
            tlsAllowInvalidCertificates=True,
        )
    except Exception:  # pylint: disable=W0703
        logger.error('Ошибка при инициализации клиента монги:', exc_info=True)
    return None


def get_mongo_parsers_client():
    try:
        return pymongo.MongoClient(
            settings.MONGO_PARSERS_URI,
            ssl=settings.MONGO_PARSERS_SSL,
            tlsAllowInvalidCertificates=True,
        )
    except Exception:  # pylint: disable=W0703
        logger.error('Ошибка при инициализации клиента монги:', exc_info=True)
    return None


def dump_event_changes(event_id, **fields):
    if client := get_mongo_bots_client():
        try:
            database = client[settings.BOTS_DB]
            database[settings.EVENTS_COLLECTION].update_one(
                {'_id': event_id},
                {'$set': fields}
            )
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в dump_event_changes:',
                         exc_info=True)
        finally:
            client.close()


def get_bot_admins():
    if client := get_mongo_parsers_client():
        try:
            bots_db = client[settings.PARSERS_DB]
            admins = bots_db['contacts'].find(
                {'role': 'admin'}
            ).distinct('tg_id')
            return admins
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_bot_admins:',
                         exc_info=True)
        finally:
            client.close()
    return []


def get_accounter_sources(used=None):  # noqa: C901
    if client := get_mongo_bots_client():
        try:
            now = datetime.now()
            bots_cl = client[settings.BOTS_DB][settings.EVENTS_COLLECTION]
            sources = bots_cl.distinct(
                'source',
                {'last_parse': {'$gt': now - timedelta(days=1)},
                 'when': {'$gt': now}}
            )
            sources = [s for s in sources if
                       settings.SPIDERS[s].get('accounter')]
            if used is None:
                return {k: None for k in sources}
            to_run = {}
            sources_cl = client[settings.BOTS_DB]['sources']
            bots = list(sources_cl.find(
                {'source': {'$in': sources}}
            ))
            for bot in bots:
                interval = timedelta(minutes=bot.get('order_interval', 90))
                last_run = bot.get('last_accounter_run', now - interval)
                if now >= last_run + interval:
                    to_run[bot['source']] = interval
            sources_cl.update_many(
                {'source': {'$in': list(to_run.keys())}},
                {'$set': {'last_accounter_run': now}}
            )
            return to_run

        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_accounter_sources:',
                         exc_info=True)
        finally:
            client.close()
    return {}


def get_bot_events():
    if client := get_mongo_bots_client():
        try:
            bots_cl = client[settings.BOTS_DB][settings.EVENTS_COLLECTION]
            return list(bots_cl.find({
                'bot_status': 2,
                'when': {'$gt': datetime.now()},
                'url': {'$exists': True, '$nin': [None, '']},
                'bot_params': {'$exists': True, '$nin': [None, {}]},
                'bot_params.users': {'$exists': True, '$nin': [None, []]},
            }))
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_bot_events:',
                         exc_info=True)
        finally:
            client.close()
    return []


def get_wait_events():
    if client := get_mongo_bots_client():
        try:
            bots_cl = client[settings.BOTS_DB][settings.EVENTS_COLLECTION]
            return list(bots_cl.find({
                'wait_params': {'$exists': True},
                'bot_params': {'$exists': True},
            }))
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_wait_events:',
                         exc_info=True)
        finally:
            client.close()
    return []


def get_captcha_token_from_datebase(source, version='v2'):
    if client := get_mongo_bots_client():
        try:
            captcha_coll = client[settings.SOURCES_DB][f'{source}_captcha']
            time_thresh = datetime.now() - timedelta(seconds=90)
            token = captcha_coll.find_one_and_delete({
                'init_time': {'$gt': time_thresh},
                'version': version
            })
            if token:
                logger.info('captcha токен найден в базе')
                return token['token']
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_wait_events:',
                         exc_info=True)
        finally:
            client.close()
    return None


def remove_old_orders(days):
    if client := get_mongo_bots_client():
        try:
            event_cl = client[settings.BOTS_DB][settings.EVENTS_COLLECTION]
            time_thresh = datetime.now() - timedelta(days=days)
            e_urls = event_cl.find(
                {'when': {'$lt': time_thresh}}
            ).distinct('url')

            orders_coll = client[settings.BOTS_DB][settings.ORDERS_CL]
            orders_coll.delete_many({'event_url': {'$in': e_urls}})
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в remove_old_orders:',
                         exc_info=True)
        finally:
            client.close()


def remove_old_captchas(minutes):
    if client := get_mongo_bots_client():
        try:
            c_collections = client[settings.SOURCES_DB].list_collection_names(
                filter={'name': {'$regex': '.*_captcha'}}
            )
            time_thresh = datetime.now() - timedelta(minutes=minutes)
            for col in c_collections:
                client[settings.SOURCES_DB][col].delete_many(
                    {'init_time': {'$lt': time_thresh}}
                )
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в remove_old_captchas:',
                         exc_info=True)
        finally:
            client.close()


def get_proxy_settings():
    if client := get_mongo_bots_client():
        try:
            bots_cl = client[settings.BOTS_DB]['sources']
            return bots_cl.find({'proxy': True}).distinct('source')
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_bot_events:',
                         exc_info=True)
        finally:
            client.close()
    return []

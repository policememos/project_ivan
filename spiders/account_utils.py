import logging
import random
from datetime import datetime

from crawler.conf import settings
from spiders.mongo_utils import get_mongo_bots_client

logger = logging.getLogger('scrapy.account_utils')
logging.getLogger('pika').setLevel(logging.INFO)


def get_fun_card():
    if client := get_mongo_bots_client():
        try:
            fun = client[settings.SOURCES_DB]['fun_cards']
            fun_cards = fun.find({}).distinct('fun_card')
            return random.choice(fun_cards) if fun_cards else None  # nosec
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_fun_card:',
                         exc_info=True)
        finally:
            client.close()
    return None


def get_all_accounts(source):
    if client := get_mongo_bots_client():
        try:
            source_db = client[settings.SOURCES_DB][source]
            return list(source_db.find({}))
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_all_accounts:',
                         exc_info=True)
        finally:
            client.close()
    return []


def get_email(source):
    if client := get_mongo_bots_client():
        try:
            source_db = client[settings.SOURCES_DB][source]
            emails = source_db.find({}).distinct('email')
            return random.choice(emails)  # nosec
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_email:',
                         exc_info=True)
        finally:
            client.close()
    return None


def get_accounts(source, used=None, interval=None):
    if client := get_mongo_bots_client():
        try:
            query = {}
            if used is not None:
                emails = get_order_emails(client, source, interval)
                query['email'] = {'$in': emails} if used else {'$nin': emails}
            source_db = client[settings.SOURCES_DB][source]
            return list(source_db.find(query))
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_accounts:',
                         exc_info=True)
        finally:
            client.close()
    return []


def get_order_emails(client, source, interval):
    start_time = datetime.now() - interval * 3
    orders_db = client[settings.BOTS_DB]['orders']
    return orders_db.distinct(
        'email',
        {'source': source, 'init_time': {'$gt': start_time}}
    )


def get_available_accounts(source, client, url, acc_max_cnt, query):
    orders = get_orders(source, client, url, acc_max_cnt)
    accounts = prepare_accounts(source, client, orders, acc_max_cnt, query)
    if not accounts:
        logger.error('В БД нет аккаунтов, которые не использовались '
                     'в этом мероприятии')
    accounts = [ac for ac in accounts if not ac.get('busy')]
    random.shuffle(accounts)
    return accounts


def get_orders(source, client, url, acc_max_cnt):
    orders = {}
    orders_db = client[settings.BOTS_DB]['orders']
    raw_orders = list(orders_db.find({
        'source': source,
        'event_url': url,
        'email': {'$exists': True, '$nin': [None, '']},
    }))
    for order in raw_orders:
        email = order['email']
        orders[email] = orders.get(email, 0) + order.get('count', acc_max_cnt)
    return orders


def prepare_accounts(source, client, orders, acc_max_cnt, query):
    accounts = []
    source_db = client[settings.SOURCES_DB][source]
    acc_query = {'email': {'$exists': True, '$nin': [None, '']}}
    acc_query.update(query or {})
    raw_accounts = list(source_db.find(acc_query))
    for account in raw_accounts:
        count = orders.get(account['email'], 0)
        if count < acc_max_cnt:
            account['count'] = count
            accounts.append(account)
    return accounts


def get_count_tickets(tickets, max_tickets):
    if len(tickets) == 1 and tickets[0].get('stand'):
        return max_tickets
    return len(tickets)


def choice_account(accs, tickets, acc_max_cnt, max_tickets):
    count_tickets = get_count_tickets(tickets, max_tickets)
    exists_units = tickets[0].get('units')
    account = bad = None
    for acc in accs:
        acc['count'] = count = acc_max_cnt - acc['count']
        if count == count_tickets:
            account = acc
            break
        if count > count_tickets:
            account = acc
        if count < count_tickets:
            bad = acc
    if exists_units and not account:
        logger.error('В БД нет подходящего под units аккаунта')
        bad = None
    return account or bad


def get_free_account_with_limits(spider, query=None):  # noqa: C901
    event = getattr(spider, 'event', None)
    tickets = getattr(spider, 'tickets', None)
    if not all([event, tickets]):
        return None
    if client := get_mongo_bots_client():
        try:
            acc_max_cnt = getattr(event, 'account_max_tickets', None) or 4
            max_tickets = getattr(event, 'max_tickets', None) or 4
            accs = get_available_accounts(
                event.source, client, event.url, acc_max_cnt, query
            )
            if account := choice_account(
                    accs, tickets, acc_max_cnt, max_tickets):
                count = account.pop('count', None)
                if not tickets[0].get('units') and count:
                    if len(tickets) > count:
                        logger.info('Все билеты на аккаунт не влезут,'
                                    ' обрезаем лишнее')
                        setattr(spider, 'tickets', tickets[:count])
                client[settings.SOURCES_DB][event.source].update_one(
                    {'_id': account['_id']},
                    {'$set': {'busy': True}}
                )
                logger.info('Начинаем с аккаунтом %s', account['email'])
            else:
                logger.error('В БД нет подходящего аккаунта')
            return account
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в get_free_account_with_limits:',
                         exc_info=True)
        finally:
            client.close()
    return None


def release_account(source, account, **fields):
    logger.debug('Освободождаем аккаунт: %s', account['email'])
    if client := get_mongo_bots_client():
        try:
            sources = client[settings.SOURCES_DB]
            fields['busy'] = False

            sources[source].update_one(
                {'_id': account['_id']},
                {'$set': fields}
            )
        except Exception:  # pylint: disable=W0703
            logger.error('Ошибка в release_account:',
                         exc_info=True)
        finally:
            client.close()

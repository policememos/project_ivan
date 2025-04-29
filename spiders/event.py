import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict

import yaml

from crawler.conf import settings
from spiders.mongo_utils import get_mongo_bots_client
from spiders.ticket import Ticket
from spiders.enum import BotMode

logger = logging.getLogger('scrapy.spiders.event')


class Event:  # pylint: disable=R0902

    def __init__(self, event: dict, spider):
        event.update(event.pop('bot_params', {}))
        self.id_event = event.pop('_id')
        self.source = event.pop('source', None)
        self.url = event.pop('url', None)
        self.source_url = event.pop('source_url', None)
        self.when = event.pop('when', None)
        self.name = event.pop('name', None)
        max_tickets = event.pop('max_tickets', 4)
        self.max_tickets = getattr(spider, 'max_tickets', None) or max_tickets  # TODO: Лучше перенести в настройки сурса в базе монго
        self.min_tickets = event.pop('min_tickets', 1)
        self.conditions = event.pop('conditions', [{}, ]) or [{}, ]
        self.count = event.pop('count', None)
        self.users = self.prepare_users(event.pop('users', []))
        self.clients = event.pop('clients', [])
        self.order_tickets = []
        self.skip_check = event.pop('skip_check', False)

        for key, value in event.items():
            setattr(self, key, value)

        self.prepare_account_max_tickets()

        if not self.skip_check or spider.mode == BotMode.PARSE:
            orders = self.__init_orders()

            count_orders = self.__get_count_orders(orders)

            if self.count or self.__check_exists_count():
                self.__update_event_count(count_orders)
                # self.__set_index_conditions()
                # self.__set_count_conditions(count_orders)

            # if event.get('use_priority', False):
            #     self.prepare_conditions()

    @staticmethod
    def prepare_users(users):
        if client := get_mongo_bots_client():
            try:
                contacts_cl = client[settings.BOTS_DB]['contacts']
                return contacts_cl.find(
                    {'name': {'$in': users}}
                ).distinct('tg_id')
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в prepare_users:',
                             exc_info=True)
            finally:
                client.close()
        return []

    def prepare_account_max_tickets(self):
        acc_limit = getattr(self, 'account_max_tickets', None)
        if os.path.exists(f'spiders/{self.source}/config.yml'):
            with open(f'spiders/{self.source}/config.yml', 'r',
                      encoding='utf-8') as lfile:
                lcfg = yaml.safe_load(lfile)
                acc_limit = acc_limit or lcfg.get('account_max_tickets')
        if acc_limit:
            setattr(self, 'account_max_tickets', acc_limit)
            self.max_tickets = min([self.max_tickets, acc_limit])

    def prepare_conditions(self):
        for ind, cond in enumerate(self.conditions, 1):
            cond['priority'] = ind

    def __str__(self):
        return f'{self.id_event} - "{self.name}" - {self.url}'

    def __check_exists_count(self) -> bool:
        return any('count' in cond for cond in self.conditions)

    def __init_orders(self):
        if client := get_mongo_bots_client():
            try:
                orders_cl = client[settings.BOTS_DB][settings.ORDERS_CL]
                orders = list(orders_cl.find({'event_url': self.url}))
                self.order_tickets = self.__prepare_orders_tickets(orders)
                return orders
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в __init_orders:',
                             exc_info=True)
            finally:
                client.close()
        return []

    @staticmethod
    def __prepare_orders_tickets(orders):
        orders_tickets = []
        for order in orders:
            if order['init_time'] > datetime.now() - timedelta(minutes=10):
                for ticket in order.get('tickets', []):
                    if not ticket.get('stand'):
                        orders_tickets.append(Ticket(ticket).get_sid())
        return set(orders_tickets)

    @staticmethod
    def __get_count_orders(orders) -> Dict[str, int]:
        counts = {}
        for order in orders:
            if not (index := order.get('condition_index')):
                index = {'all': order.get('count', 0)}
            if isinstance(index, int):
                index = {str(index): order.get('count', 0)}
            for index, count in index.items():
                counts[index] = counts.get(index, 0) + count
        return counts

    def __update_event_count(self, count_orders: Dict[str, int]):
        if self.count is not None:
            self.count = max(self.count - count_orders.get('all', 0), 0)

    # def __set_index_conditions(self):  # noqa: C901
    #     changed = False
    #     start_index = int(datetime.now().timestamp())
    #     for ind, cond in enumerate(self.conditions):
    #         if cond.get('count') is not None and not cond.get('index'):
    #             changed = True
    #             cond['index'] = str(start_index + ind)
    #
    #     if changed:
    #         if client := get_mongo_bots_client():
    #             try:
    #                 bot_db = client[settings.BOTS_DB]
    #                 bot_db[settings.EVENTS_COLLECTION].update_one(
    #                     {'_id': self.id_event},
    #                     {'$set': {'bot_params.conditions': self.conditions}},
    #                 )
    #             except Exception:  # pylint: disable=W0703
    #                 logger.error('Ошибка в __set_index_conditions:',
    #                              exc_info=True)
    #             finally:
    #                 client.close()

    def __set_count_conditions(self, count_orders: Dict[str, int]):
        for cond in self.conditions:
            if cond.get('count'):
                cond_orders = count_orders.get(cond['index'], 0)
                cond['count'] = max(cond['count'] - cond_orders, 0)

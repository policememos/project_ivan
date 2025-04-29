import logging
import uuid
from datetime import datetime

import yaml

from spiders.enum import BotMode
from spiders.helpers import send_sms_message
from spiders.mongo_utils import get_mongo_bots_client, get_bot_admins
from spiders.pipelines import BotParseMode
logger = logging.getLogger('scrapy.spiders.etihadarena.pipelines')

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)


class BotEtihadArenaQuickBuyPipeline:

    def process_item(self, item, spider):
        if spider.mode != BotMode.PARSE or not item.get('payment_url'):
            return item
        order = self.dump_order(item, spider)
        message = self.prepare_message(spider, order, item['tickets'])
        logger.debug(message)
        for user in spider.event.users + get_bot_admins():
            send_sms_message(str(user), message)
        return None

    def dump_order(self, item, spider):
        if client := get_mongo_bots_client():
            try:
                bots_db = client[cfg['mongo_bots']['bots_db']]
                new_order = {
                    '_id': str(uuid.uuid4()),
                    'event_url': spider.event.url,
                    'source': spider.custom_settings['source'],
                    'init_time': datetime.now(),
                    'payment_url': item['payment_url'],
                    'tickets': self.prepare_tickets(item['tickets'])
                }
                if condition_index := self.prepare_condition_index(item):
                    new_order['condition_index'] = condition_index
                new_order['count'] = self.get_item_count(item)
                bots_db['orders'].insert_one(new_order)
                return new_order
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в dump_order:',
                             exc_info=True)
            finally:
                client.close()
        return item

    @staticmethod
    def prepare_tickets(raw_tickets):
        tickets = []
        added_sids = []
        for ticket in raw_tickets:
            if ticket.get_sid() not in added_sids:
                added_sids.append(ticket.get_sid())
                tickets.append(ticket.get_dict())
        return tickets

    @staticmethod
    def prepare_message(spider, order, tickets):
        payment_url = order['payment_url']
        _id = order.get('_id')
        message = (f'ЗАКУПКА\n{spider.event.name} '
                   f'({spider.event.when})\n{spider.event.source_url}\n'
                   f"Билеты:\n")
        for ticket in tickets:
            message += f'{ticket}\n'
        if (p_url := cfg.get('proxy_payment_url')) and _id:
            payment_url = p_url + _id
        message += f"\nСсылка для оплаты: {payment_url}"
        return message

    @staticmethod
    def prepare_condition_index(item):
        tickets = item.get('tickets') or []
        cond_dict = {}
        exist_index = any(t.get('cond_index') for t in tickets)
        for ticket in tickets:
            if (index := ticket.get('cond_index')) or exist_index:
                if not index:
                    index = 'all'
                if ticket.get('stand'):
                    return {str(index): ticket['count']}
                cond_dict[str(index)] = cond_dict.get(str(index), 0) + 1
        return cond_dict

    @staticmethod
    def get_item_count(item):
        if (l_tickets := len(item['tickets'])) == 1:
            return item['tickets'][0].get('count') or 1
        return l_tickets


class BotEtihadArenaParseMode(BotParseMode):

    def init_tickets_buy_tasks(self, spider):
        if not self.tickets:
            return
        sectors = self.sort_by_sectors(self.tickets)
        for s_tickets in sectors.values():
            for tickets in self.get_parts_tickets(s_tickets, spider):
                self.count_orders += 1
                self.count_tickets += len(tickets)
                self.run_bot_delay(
                    id_event=spider.id_event,
                    source=spider.custom_settings['source'],
                    tickets=tickets,
                    max_tickets=spider.event.max_tickets,
                )

    # def init_units_buy_tasks(self, spider):
    #     if self.tickets_bunches:
    #         self.tickets_bunches = sorted(
    #             self.tickets_bunches,
    #             key=lambda b: - max(len(t) for t in b['units'])
    #         )
    #         clients = spider.event.clients
    #         for tickets_bunch in self.tickets_bunches:
    #             units = tickets_bunch['units']
    #             t_sorted = self.find_neighbours(tickets_bunch['tickets'])
    #             distr_tickets = self.distribute(units, t_sorted, clients)
    #             complete_tickets = self.check_count_units(distr_tickets)
    #             for tickets in complete_tickets:
    #                 sectors = self.sort_by_sectors(tickets)
    #                 for s_tickets in sectors.values():
    #                     for part in self.get_parts_tickets(s_tickets, spider):
    #                         self.count_orders += 1
    #                         self.count_tickets += len(part)
    #                         self.run_bot_delay(
    #                             id_event=spider.id_event,
    #                             source=spider.custom_settings['source'],
    #                             tickets=part,
    #                             max_tickets=spider.event.max_tickets,
    #                         )

    @staticmethod
    def sort_by_sectors(tickets):
        sectors = {}
        for ticket in tickets:
            s_name = ticket['product_id']
            sectors[s_name] = sectors.get(s_name, []) + [ticket]
        return sectors

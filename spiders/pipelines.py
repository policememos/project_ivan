import logging
import re
from datetime import datetime
from operator import itemgetter
from itertools import groupby

import uuid
from scrapy.exceptions import DropItem

from crawler.conf import settings
from spiders.event import Event
from spiders.condition import Condition
from spiders.enum import BotMode

from spiders.helpers import send_sms_message
from spiders.mongo_utils import get_bot_admins, get_mongo_bots_client
from spiders.tasks import run_bot_quick_from_spider
from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.pipelines')


class CheckTicket:
    '''
    Второй пайплайн в настройках
    1. open_spider - если паук в BotMode.PARSE назначает себе conditions от паука
    2. process_item - если паук в BotMode.PARSE и item это Ticket сверяет что этого билета нет в self.orders_tickets
    '''
    def __init__(self):
        self.conditions = []
        self.orders_tickets = []
        self.all_tickets = []
        self.record_tickets = False

    def open_spider(self, spider):
        logger.info(f'2. Pipeline CheckTicket начал open_spider, {spider.mode=}')
        if spider.mode == BotMode.PARSE:
            logger.info(f'Pipeline CheckTicket назначаю self.conditions, {spider.mode=}')
            self.orders_tickets = getattr(spider.event, 'order_tickets', [])
            self.record_tickets = getattr(spider.event, 'record_tickets', False)
            self.conditions = spider.conditions

    def process_item(self, item, spider):  # noqa: C901
        # logger.info(f'2. pipeline CheckTicket начал process_item')
        if spider.mode == BotMode.PARSE and isinstance(item, Ticket):
            drop_message = str(item)
            if item.get_sid() not in self.orders_tickets:
                for cond in self.conditions:
                    if cond.check(item):
                        item['cond_index'] = cond.index
                        item['units'] = cond.units
                        item['priority'] = cond.priority
                        item['sort'] = cond.sort
                        item['sort_index'] = cond.sort_index
                        if item.stand or cond.units:
                            item['cond_count'] = cond.count
                        self.add_ticket(item, suit=True)
                        return item
            else:
                drop_message += ' уже выкуплен'
            self.add_ticket(item)
            raise DropItem(drop_message)
        # logger.info(f'2. pipeline CheckTicket начал process_item это "buy" просто ретёрню итем')
        return item

    def add_ticket(self, ticket, suit=False):
        if self.record_tickets:
            obj = {'_id': ticket.get_sid(),
                 'ticket': str(ticket),
                 'suit': suit}
            logger.info(f'2. pipeline CheckTicket process_item -> Есть {self.record_tickets=} ,добавляю в self.all_tickets {obj}')
            self.all_tickets.append(obj)


class InitParamsAndCheckActuality:

    def open_spider(self, spider):
        logger.info('1. Pipeline InitParamsAndCheckActuality начал')
        logger.info('starting %s',  spider.custom_settings['source'])
        if event := self.get_event_from_db(spider.id_event):
            logger.info('event_url: %s', event['url'])
            setattr(spider, 'event', Event(event, spider))
            if spider.mode == BotMode.BUY and spider.event.skip_check: # сюда не заходил, убрать?
                logger.info('Закупка без продготовки параметров!')
                tickets = [Ticket(t) for t in getattr(spider, 'tickets', [])]
                setattr(spider, 'tickets', tickets)
                self.log_tickets(tickets)
                return
            max_tickets = getattr(spider.event, 'max_tickets', 4)
            conditions = [
                Condition(cond, max_tickets)
                for cond in spider.event.conditions
                if cond.get('count') != 0
            ]
            if spider.event.count != 0 and conditions:
                if spider.mode == BotMode.PARSE:
                    setattr(spider, 'conditions', conditions)
                    return
                if tickets := self.prepare_tickets(conditions, spider):
                    setattr(spider, 'tickets', tickets)
                    self.log_tickets(tickets)
                    return
                logger.info('все билеты больше не актуальны')
            else:
                logger.info('исчерпаны лимиты по count или пуст conditions')
                self.pause_event_and_send_message(spider.event)
        else:
            logger.info('Мероприятие %s больше не в работе', spider.id_event)

        # обнуляем mode бота, что бы он ничего не делал.
        setattr(spider, 'mode', None)

    @staticmethod
    def pause_event_and_send_message(event):
        if client := get_mongo_bots_client():
            try:
                bots_db = client[settings.BOTS_DB]
                bots_db[settings.EVENTS_COLLECTION].update_one(
                    {
                        '_id': event.id_event,
                        'bot_status': 2,
                        'when': {'$gt': datetime.now()}},
                    {'$set': {'bot_status': 4}})
                logger.info('Мероприятие %s (%s) %s поставлено на паузу, '
                            'так как исчерпаны лимиты',
                            event.name, event.when, event.url)
                message = (f'У Мероприятия исчерпаны лимиты: {event.source} '
                           f'{event.name} ({event.when}) {event.url}\n\n'
                           'Оно поставлено на паузу!')
                for user in get_bot_admins():
                    send_sms_message(str(user), message)
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в pause_event_and_send_message:',
                             exc_info=True)
            finally:
                client.close()

    def prepare_tickets(self, conditions, spider):
        tickets = []
        if bot_tickets := getattr(spider, 'tickets', None):
            orders_tickets = spider.event.order_tickets
            for ticket in bot_tickets:
                if not isinstance(ticket, Ticket):
                    ticket = Ticket(ticket)
                if self.is_actual_ticket(ticket, conditions, orders_tickets):
                    tickets.append(ticket)
                elif ticket['units']:
                    logger.info('Не все билеты актуальны при закупке по units')
                    return []
                else:
                    logger.info('Билет больше не актуален: %s', ticket)
        return tickets

    @staticmethod
    def log_tickets(tickets):
        message = 'Идем закупать билеты:\n'
        for ticket in tickets:
            message += f'{ticket}\n'
        logger.info(message)

    @staticmethod
    def check_conditions(conditions, ticket):
        for cond in conditions:
            if cond.check(ticket):
                return True
        return False

    def is_actual_ticket(self, ticket, conditions, orders_tickets):
        if ticket.get_sid() in orders_tickets:
            logger.info('Билет уже выкуплен')
            return False
        return self.check_conditions(conditions, ticket)

    @staticmethod
    def get_event_from_db(id_event):  # берет из базы bot-events 1 ивент по _id, если он в статусе 2 и when>now
        if client := get_mongo_bots_client():
            try:
                bots_db = client[settings.BOTS_DB]
                return bots_db[settings.EVENTS_COLLECTION].find_one({
                    '_id': id_event,
                    'bot_status': 2,
                    'when': {'$gt': datetime.now()},
                })
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в get_event_from_db:',
                             exc_info=True)
            finally:
                client.close()
        return None


class BotParseMode:
    def __init__(self):
        self.tickets = []
        self.stand_tickets = []
        self.tickets_bunches = []
        self.count_tickets = self.count_orders = 0
        self.quick_buy = True
        self.use_priority = False

    def open_spider(self, spider):
        logger.info(f'3. Pipeline BotParseMode начал open_spider {spider.mode}')
        if spider.mode == BotMode.PARSE:
            self.use_priority = getattr(spider.event, 'use_priority', False)
            self.quick_buy = getattr(
                spider.event, 'quick_buy', not self.use_priority
            )
            if any(getattr(cond, 'sort', None)
                   for cond in getattr(spider, 'conditions', [])):
                self.quick_buy = False

            print(f'запустился в режиме {self.quick_buy=}, {self.use_priority=}')

    def process_item(self, item, spider):
        # logger.info(f'3. pipeline BotParseMode начал process_item если мод=parse, если {self.quick_buy=} and {item.get("stand")=}, то')
        if spider.mode == BotMode.PARSE and item:
            if item.get('units'):
                self.add_item_to_tickets_bunch(item)
            elif self.quick_buy and item.get('stand'):
                self.stand_tickets = [item]
                self.init_stand_buy_tasks(spider)
                self.stand_tickets = []
            else:
                self.tickets.append(item)
            if self.quick_buy and len(self.tickets) == spider.event.max_tickets:
                self.divide_tickets(spider)
                self.init_tickets_buy_tasks(spider)
                self.tickets = []
        return item

    def close_spider(self, spider):
        logger.info(f'3. pipeline BotParseMode начал close_spider Тут передача данных на закупку')
        """Передача данных закупщику."""
        if spider.mode == BotMode.PARSE:
            if self.tickets and self.has_sort_in_tickets(self.tickets):
                self.tickets = sorted(self.tickets, key=self.recursion_sort)
            self.divide_tickets(spider)
            if self.use_priority:
                self.init_priority_buy_tasks(spider)
            else:
                self.init_tickets_buy_tasks(spider)
                self.init_stand_buy_tasks(spider)
                self.init_units_buy_tasks(spider)
            self.send_start_message(spider)

    @staticmethod
    def has_sort_in_tickets(tickets):
        return any(x.sort for x in tickets)

    def init_priority_buy_tasks(self, spider):
        all_tickets = self.tickets + self.stand_tickets
        for tickets_bunch in self.tickets_bunches:
            all_tickets.extend(tickets_bunch['tickets'])
        all_tickets = sorted(all_tickets, key=lambda t: t['priority'])
        for _, group in groupby(all_tickets, key=itemgetter('priority')):
            group = list(group)
            if units := group[0].get('units'):
                self.tickets_bunches = [{
                    'units': units,
                    'tickets': group,
                }]
                self.init_units_buy_tasks(spider)
            elif group[0].get('stand'):
                self.stand_tickets = group
                self.init_stand_buy_tasks(spider)
            else:
                self.tickets = group
                self.init_tickets_buy_tasks(spider)
            self.tickets = self.stand_tickets = self.tickets_bunches = []

    def add_item_to_tickets_bunch(self, item):
        units = item['units']
        for bunch in self.tickets_bunches:
            if bunch['sort_index'] == item['sort_index']:
                bunch['tickets'].append(item)
                return
        self.tickets_bunches.append({
            'sort_index': item.sort_index,
            'units': units.copy(),
            'tickets': [item],
        })

    @staticmethod
    def get_parts_tickets(raw_tickets, spider, min_tickets=None):
        max_tickets = getattr(spider.event, 'max_tickets') or 4
        min_tickets = min_tickets or getattr(spider.event, 'min_tickets') or 1
        min_tickets = min(min_tickets, max_tickets)
        if min_tickets > 1:
            remain = len(raw_tickets) % max_tickets
            for denominator in range(min_tickets, max_tickets):
                if len(raw_tickets) % denominator < remain:
                    remain = len(raw_tickets) % denominator
                    max_tickets = denominator
        for ind in range(0, len(raw_tickets), max_tickets):
            tickets = raw_tickets[ind:(ind + max_tickets)]
            if len(tickets) < min_tickets:
                logger.info('Количество билетов (%s) меньше минимального '
                            'порога (%s) Билеты: %s',
                            len(tickets), min_tickets, tickets)
                continue
            yield tickets

    @staticmethod
    def run_bot_delay(**kwargs):
        run_bot_quick_from_spider.delay(**kwargs)


    def init_tickets_buy_tasks(self, spider):
        if not self.tickets:
            return
        for tickets in self.get_parts_tickets(self.tickets, spider):
            self.count_orders += 1
            self.count_tickets += len(tickets)
            logger.info('init_tickets_buy_tasks ОТПРАВИЛ В ЗАКУПКУ БИЛЕТЫ: %s', tickets)
            self.run_bot_delay(
                id_event=spider.id_event,
                source=spider.custom_settings['source'],
                tickets=tickets,
                max_tickets=spider.event.max_tickets,
                solve_captcha=spider.solve_captcha,
            )

    def init_units_buy_tasks(self, spider):  # noqa: C901
        if self.tickets_bunches:
            if len(self.tickets_bunches) > 1:
                self.tickets_bunches = sorted(
                    self.tickets_bunches,
                    key=lambda b: - max(len(t) for t in b['units'])
                )
            clients = spider.event.clients
            for tickets_bunch in self.tickets_bunches:
                units = tickets_bunch['units']
                min_tk = min(len(x) for x in units)
                pairs_bunch = self.find_neighbours(tickets_bunch['tickets'])
                distr_tickets = self.distribute(units, pairs_bunch, clients)
                if self.has_sort_in_tickets(tickets_bunch['tickets']):
                    distr_tickets = sorted(
                        distr_tickets, key=lambda x: self.recursion_sort(x[0])
                    )
                complete_tickets = self.check_count_units(distr_tickets)
                for tickets in complete_tickets:
                    for part in self.get_parts_tickets(tickets, spider, min_tk):
                        self.count_orders += 1
                        self.count_tickets += len(part)
                        self.run_bot_delay(
                            id_event=spider.id_event,
                            source=spider.custom_settings['source'],
                            tickets=part,
                            max_tickets=spider.event.max_tickets,
                            solve_captcha=spider.solve_captcha,
                        )

    def check_count_units(self, distr_tickets):
        tickets = []
        conditions_counts = self.get_conditions_counts(distr_tickets)
        for ticket_group in distr_tickets:
            if cond_index := ticket_group[0].get('cond_index'):
                if conditions_counts[cond_index] <= 0:
                    continue
                conditions_counts[cond_index] -= len(ticket_group)
            tickets.append(ticket_group)
        return tickets

    @staticmethod
    def get_conditions_counts(tickets):
        cond_counts = {}
        for ticket_group in tickets:
            for ticket in ticket_group:
                if cond_index := ticket.get('cond_index'):
                    cond_counts[cond_index] = ticket.get('cond_count') or 0
        return cond_counts

    def send_start_message(self, spider):
        if self.count_tickets:
            message = (f'ОТПРАВЛЕНО {self.count_orders} ЗАКУПОК '
                       f'{spider.custom_settings["source"]} '
                       f'{spider.event.name} ({spider.event.when}) '
                       f'{spider.event.url}: {self.count_tickets} билетов')
            logger.info(message)
            for user in get_bot_admins():
                send_sms_message(str(user), message)
            self.count_tickets = self.count_orders = 0

    def init_stand_buy_tasks(self, spider):
        if self.stand_tickets:
            for ticket in self.stand_tickets:
                ticket = self.get_stand_ticket(spider, ticket)
                count_tasks = ticket['count'] // spider.event.max_tickets
                max_tickets = min([ticket['count'], spider.event.max_tickets])
                for _ in range(count_tasks or 1):
                    self.count_orders += 1
                    self.count_tickets += max_tickets
                    logger.info('init_stand_buy_tasks ОТПРАВИЛ В ЗАКУПКУ БИЛЕТЫ: %s', ticket)
                    self.run_bot_delay(
                        id_event=spider.id_event,
                        source=spider.custom_settings['source'],
                        tickets=[ticket],
                        max_tickets=max_tickets,
                        solve_captcha=spider.solve_captcha,
                    )

    def divide_tickets(self, spider):  # noqa: C901
        if not self.tickets:
            return
        self.tickets = sorted(self.tickets, key=lambda t: t['priority'])
        seat_tickets = []
        for ticket in self.tickets:
            if ticket.get('stand'):
                self.stand_tickets.append(ticket)
            else:
                if ticket.get('cond_index') or spider.event.count is None:
                    seat_tickets.append(ticket)
                elif spider.event.count:
                    spider.event.count -= 1
                    seat_tickets.append(ticket)
        self.tickets = seat_tickets

    @staticmethod
    def get_stand_ticket(spider, ticket):
        ticket['count'] = ticket.get('count') or spider.event.max_tickets
        if cond_count := ticket['cond_count']:
            ticket['count'] = min([cond_count, ticket['count']])
        return ticket

    @staticmethod
    def sort_len_ords(text, direction=1):
        if not isinstance(text, str):
            text = str(text)
        ords = [ord(ch) * direction for ch in text]
        return not text.isdigit(), len(text) * direction, ords

    def recursion_sort(self, ticket, use_sorting=True):
        sorting = ticket.sort if ticket.sort else None
        if sorting and use_sorting:
            return [self.sort_len_ords(ticket[attr], direction)
                for attr, direction in sorting]
        attr = ['sector', 'row', 'seat']
        return [self.sort_len_ords(ticket[x]) for x in attr]

    def find_neighbours(self, tickets):  # noqa: C901
        def compare_parts(curr_part, prev_part, last_part):
            if not last_part:
                return curr_part == prev_part
            if curr_part == prev_part:
                return True
            if isinstance(curr_part, int) and isinstance(prev_part, int):
                return abs(curr_part - prev_part) == 1
            if isinstance(curr_part, str) and isinstance(prev_part, str):
                return abs(ord(curr_part) - ord(prev_part)) == 1
            return False

        def is_neighbour(prev_seat, curr_seat):
            if len(prev_seat) != len(curr_seat):
                return False
            for i, prev in enumerate(prev_seat):
                is_last = i == len(prev_seat) - 1
                if not compare_parts(curr_seat[i], prev, is_last):
                    return False
            return True

        def seat_compare(_ticket):
            parts = []
            for part in re.findall(r'\d+|\D', _ticket.seat):
                if part.isdigit():
                    parts.append(int(part))
                else:
                    parts.append(part)
            return parts

        def process_group(_group):
            neighbours = []
            for ticket in _group:
                if not neighbours:
                    neighbours.append(ticket)
                    continue
                if is_neighbour(
                        seat_compare(neighbours[-1]), seat_compare(ticket)):
                    neighbours.append(ticket)
                else:
                    if len(neighbours) > 1:
                        all_neighbours.append(neighbours)
                    neighbours = [ticket]
            if len(neighbours) > 1:
                all_neighbours.append(neighbours)

        tickets = sorted(
            tickets, key=lambda x: self.recursion_sort(x, use_sorting=False)
        )
        all_neighbours = []
        for _, group in groupby(tickets, key=itemgetter('sector', 'row')):
            process_group(group)
        return all_neighbours

    @staticmethod
    def distribute(units, tickets, clients):  # noqa: C901
        def get_part(cnt):
            for chunk in tickets:
                if len(chunk) >= cnt:
                    t_unit = chunk[:cnt]
                    del chunk[:cnt]
                    return t_unit
            return None

        complete_tickets = []
        ind = 0
        while units:
            cnt = len(units[ind])
            if unit_tickets := get_part(cnt):
                if clients:
                    clients_tickets = []
                    for client_ind, ticket in zip(units[ind], unit_tickets):
                        ticket['client'] = clients[client_ind]
                        clients_tickets.append(ticket)
                    complete_tickets.append(clients_tickets)
                else:
                    complete_tickets.append(unit_tickets)
                ind += 1
            else:
                del units[ind]

            if ind >= len(units):
                ind = 0
        return sorted(complete_tickets, key=len, reverse=True)


class BotBuyMode:

    def __init__(self):
        self.success_tickets = []
        self.failed_tickets = []
        self.order_fields = {}
        self.count = 0

    def open_spider(self, spider):
        logger.info(f'4. Pipeline BotBuyMode начал open_spider, {spider.mode} (проверка есть ли spider.tickets, spider.mode=buy)')
        tickets = getattr(spider, 'tickets', None)
        if spider.mode == BotMode.BUY and tickets:
            if tickets[0].get('stand'):
                self.count = getattr(spider.event, 'max_tickets', 4)

    def process_item(self, item, spider):
        if spider.mode == BotMode.BUY:
            if isinstance(item, Ticket):
                if item.success is True:
                    self.success_tickets.append(item)
                elif item.success is False:
                    logger.error(f'pipeline BotBuyMode process_item у {item=} success==False')
                    self.failed_tickets.append(item)
            elif item.get('payment_url'):
                logger.info(f'pipeline BotBuyMode process_item у item словаря есть payment_url, добавляем объект в self.order_fields')
                self.order_fields.update(item)
            else:
                logger.info(f'pipeline BotBuyMode process_item {item=} не Ticket() и без payment_url, иду в dump_order')
                self.dump_order(item, spider)
        return item

    def close_spider(self, spider):
        logger.info(f'pipeline BotBuyMode начал close_spider')
        if self.failed_tickets:
            logger.info(f'Билеты ФЕЙЛ pipeline BotBuyMode дамплю self.failed_tickets')
            self.dump_order(
                {'tickets': self.prepare_tickets(self.failed_tickets)},
                spider
            )
        if self.order_fields and self.success_tickets:
            self.order_fields.update(
                {'tickets': self.prepare_tickets(self.success_tickets)}
            )
            _id = self.dump_order(self.order_fields, spider)
            message = self.prepare_message(spider, _id)
            logger.debug(message)
            for user in get_bot_admins():
                send_sms_message(str(user), message)

    @staticmethod
    def prepare_tickets(raw_tickets):
        tickets = []
        added_sids = []
        for ticket in raw_tickets:
            if ticket.get_sid() not in added_sids:
                added_sids.append(ticket.get_sid())
                tickets.append(ticket.get_dict())
        return tickets

    def prepare_message(self, spider, _id):
        message = (f'ЗАКУПКА\n{spider.event.name} '
                   f'({spider.event.when})\n{spider.event.source_url}\n'
                   f"Билеты:\n")
        for ticket in self.success_tickets:
            message += f'{ticket}\n'
        message += self.order_fields.get('message') or ''
        message += f'\nНОМЕР ЗАКАЗА: {_id}'
        message += f"\nСсылка для оплаты: {self.order_fields['payment_url']}"
        return message

    def dump_order(self, item, spider):
        if client := get_mongo_bots_client():
            try:
                bots_db = client[settings.BOTS_DB]
                item['_id'] = str(uuid.uuid4())
                item['event_url'] = spider.event.url
                item['source'] = spider.custom_settings['source']
                item['init_time'] = datetime.now()

                if item.get('payment_url'):
                    if condition_index := self.prepare_condition_index(item):
                        item['condition_index'] = condition_index
                    item['count'] = self.get_item_count(item)
                logger.info(f'pipeline BotBuyMode dump_order вставляю в базу orders {item=}')
                bots_db['orders'].insert_one(item)
                return item['_id']
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в dump_order:',
                             exc_info=True)
            finally:
                client.close()
        return None

    def prepare_condition_index(self, item):
        tickets = item.get('tickets') or []
        cond_dict = {}
        exist_index = any(t.get('cond_index') for t in tickets)
        for ticket in tickets:
            if (index := ticket.get('cond_index')) or exist_index:
                if not index:
                    index = 'all'
                if ticket.get('stand'):
                    return {str(index): self.count}
                cond_dict[str(index)] = cond_dict.get(str(index), 0) + 1
        return cond_dict

    def get_item_count(self, item):
        if item.get('count'):
            return item['count']
        if self.count:
            return self.count
        return len(self.success_tickets)

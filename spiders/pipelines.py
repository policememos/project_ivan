import html
import logging
import re
from datetime import datetime, date
from operator import itemgetter
from itertools import groupby

import uuid
import unicodedata
from pymongo.errors import BulkWriteError
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
                        # item['cond_index'] = cond.index
                        # item['units'] = cond.units
                        item['priority'] = cond.priority
                        # item['sort'] = cond.sort
                        # item['sort_index'] = cond.sort_index
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

    def close_spider(self, spider):  # noqa: C901
        logger.info(f'2. pipeline CheckTicket начал close_spider')
        if spider.mode == BotMode.PARSE and self.all_tickets: #TODO: проверить правильно ли работает? почему то не захожу сюда
            init_time = datetime.now()
            for ticket in self.all_tickets:
                ticket['init_time'] = init_time

            if client := get_mongo_bots_client():
                count = 0
                try:
                    e_id = spider.event.id_event
                    count = len(client[settings.TICKETS_DB][e_id].insert_many(
                        self.all_tickets, ordered=False
                    ).inserted_ids)
                except BulkWriteError as error:
                    count = error.details['nInserted']
                except Exception:  # pylint: disable=W0703
                    logger.error('Ошибка в CheckTicket.close_spider:',
                                 exc_info=True)
                finally:
                    client.close()

                logger.info('В БД Вставлено %s билетов', count)


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
            # self.use_priority = getattr(spider.event, 'use_priority', False)
            # self.quick_buy = getattr(
            #     spider.event, 'quick_buy', not self.use_priority
            # )
            # if (conds := getattr(spider, 'conditions', [])) and self.quick_buy:  # Если есть кондишены с сортировкой, то выключает quick_buy
            #     for cond in conds:
            #         if getattr(cond, 'sort', None):
            #             self.quick_buy = False
            #             break

            print(f'запустился в режиме {self.quick_buy=}, {self.use_priority=}')

    def process_item(self, item, spider):
        logger.info(f'3. pipeline BotParseMode начал process_item если мод=parse, если {self.quick_buy=} and {item.get("stand")=}, то')
        if spider.mode == BotMode.PARSE:
            if self.quick_buy and item.get('stand'):
                self.stand_tickets = [self.get_stand_ticket(spider, item)]
                self.init_stand_buy_tasks(spider)
                self.stand_tickets = []
            else:
                self.tickets.append(item)
        return item

    def close_spider(self, spider):
        logger.info(f'3. pipeline BotParseMode начал close_spider Тут передача данных на закупку')
        """Передача данных закупщику."""
        if spider.mode == BotMode.PARSE:
            self.divide_tickets(spider)
            if self.use_priority:
                self.init_priority_buy_tasks(spider)
            else:
                self.init_tickets_buy_tasks(spider)
                self.init_stand_buy_tasks(spider)
            self.send_start_message(spider)

    def init_priority_buy_tasks(self, spider):
        all_tickets = self.tickets + self.stand_tickets
        for tickets_bunch in self.tickets_bunches:
            all_tickets.extend(tickets_bunch['tickets'])
        all_tickets = sorted(all_tickets, key=lambda t: t['priority'])  #TODO: тут сортировка билетов по priority!
        for _, group in groupby(all_tickets, key=itemgetter('priority')):
            group = list(group)
            if units := group[0].get('units'):
                self.tickets_bunches = [{
                    'units': units,
                    'tickets': group,
                }]
                # self.init_units_buy_tasks(spider)
            elif group[0].get('stand'):
                self.stand_tickets = group
                self.init_stand_buy_tasks(spider)
            else:
                self.tickets = group
                self.init_tickets_buy_tasks(spider)
            self.tickets = self.stand_tickets = self.tickets_bunches = []

    @staticmethod
    def get_parts_tickets(raw_tickets, spider):  #TODO: тут делим на пачки
        min_tickets = getattr(spider.event, 'min_tickets') or 1
        max_tickets = getattr(spider.event, 'max_tickets') or 4
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
        if settings.SPIDERS[kwargs['source']]['queue'] == 'quick':
            # run_bot_quick_from_spider(**kwargs)
            run_bot_quick_from_spider.delay(**kwargs)
        else:
            logger.info('run_bot_dealy run_bot_long_from_spider.delay')
            ...
            # run_bot_long_from_spider.delay(**kwargs)

    def init_tickets_buy_tasks(self, spider):
        if not self.tickets:
            return

        # self.tickets = self.sort_by_rules(self.tickets)

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
        '''
        назначаем self.stand_tickets стоячку
        назначаем self.tickets сидячку
        '''
        if not self.tickets:
            return
        self.tickets = sorted(self.tickets, key=lambda t: t['priority']) #TODO: !тут сортировка билетов по priority!
        seat_tickets = []
        for ticket in self.tickets:
            if ticket.get('stand'):
                self.stand_tickets.append(self.get_stand_ticket(spider, ticket))
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
        # logger.info(f'pipeline BotBuyMode начал process_item. Проверка что {spider.mode=}[buy]')
        if spider.mode == BotMode.BUY:
            if isinstance(item, Ticket):
                # logger.info(f'pipeline BotBuyMode начал process_item. {item=} -> смотрю его success и или в self.success_tickets или в self.failed_tickets добавляю')
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
        # logger.info(f'pipeline BotBuyMode process_item -> return {item=}')
        return item

    def close_spider(self, spider):
        logger.info(f'pipeline BotBuyMode начал close_spider')
        if self.failed_tickets:
            logger.info(f'Билеты ФЕЙЛ pipeline BotBuyMode дамплю self.failed_tickets')
            self.dump_order(
                {'tickets': self.prepare_tickets(self.failed_tickets)},
                spider
            )
        if self.order_fields:
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
                logger.error('Ошибка в get_event_from_db:',
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


# class TrimWhitespace:
#
#     @staticmethod
#     def process_item(item, spider):
#         logger.info(f'?. (нет в custom_settings) pipeline TrimWhitespace начал process_item')
#         for key, value in item.items():
#             if isinstance(value, str):
#                 item[key] = html.unescape(value)
#                 item[key] = unicodedata.normalize('NFKD', value)
#                 item[key] = re.sub(r'\s+', ' ', value.strip())
#         return item
#
#
# class ParseWhen:
#
#     MONTHS = [
#         ['янв'], ['фев'], ['мар'], ['апр'], ['май', 'мая'],
#         ['июн'], ['июл'], ['авг'], ['сен'], ['окт'], ['ноя'],
#         ['дек']
#     ]
#
#     NOW = datetime.now()
#     CURRENT_YEAR = NOW.year
#     CURRENT_MONTH = NOW.month
#
#     def get_event_month(self, head):
#         for index, variants in enumerate(self.MONTHS):
#             for var in variants:
#                 if var.lower() in head.lower():
#                     return str(index + 1).zfill(2)
#         return None
#
#     @staticmethod
#     def format_when(year, month, day, hour=None, minute=None):
#         if (hour is not None) and (minute is not None):
#             return datetime(
#                 year=int(year), month=int(month), day=int(day),
#                 hour=int(hour), minute=int(minute))
#         return date(
#             year=int(year), month=int(month), day=int(day))
#
#     def process_item(self, item, spider):  # noqa:C901
#         logger.info(f'?. pipeline ParseWhen начал process_item')
#         """
#         Возможные варианты написания даты и времени:
#
#         "2019-01-01_19:00",
#         "2019-01-01 19:00",
#         "2019-01-01T19:00",
#         "2019-01-01_1900",
#         "2019-01-01 1900",
#         "11.01.2019 19:00",
#         "1.01.2019 19:00",
#         "9 Дек. 17:00",
#         "29 Нояб.чт 21:00",
#         "22 ноября 2019пт 20:00",
#         "16 Май 2019 20:00",
#         "31 мая 2019пт 19:00",
#         "5 декабря                     ср 20:00",
#         "11 августа ВС, 16:30",
#         "сб, 03 августа 2019 16:00"
#
#         """
#
#         if not item['when']:
#             item['when'] = None
#             return item
#
#         if isinstance(item['when'], datetime):
#             return item
#
#         item['when'] = re.sub(r'\s+', ' ', item['when']).strip()
#
#         matched = re.match(
#             r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[\s_T]'
#             r'(?P<hour>\d\d?):?(?P<minute>\d{2})',
#             item['when']
#         )
#         if matched:
#             item['when'] = self.format_when(**matched.groupdict())
#             return item
#
#         matched = re.match(
#             r'^(?P<day>\d\d?)[.\-](?P<month>\d{2})[.\-](?P<year>\d{4})\s'
#             r'(?P<hour>\d\d?):(?P<minute>\d{2})',
#             item['when']
#         )
#         if matched:
#             item['when'] = self.format_when(**matched.groupdict())
#             return item
#
#         matched = re.match(r'^[а-яА-Я,.\s]*(\d+)(.+)$', item['when'])
#         if not matched:
#             raise DropItem('Дата не найдена')
#         head, tail = matched.groups()
#         event_day = head.zfill(2)
#
#         matched = re.match(r'^\s+([а-яА-Я]+)(\D+)(.+)$', tail)
#         if not matched:
#             raise DropItem('Дата не найдена')
#         head, tail = matched.groups()
#
#         event_month = self.get_event_month(head)
#         if event_month is None:
#             raise DropItem('Месяц не определен')
#
#         if matched := re.match(r'^(20\d{2})(.+)$', tail):
#             head, tail = matched.groups()
#             event_year = head
#         else:
#             if int(event_month) >= self.CURRENT_MONTH:
#                 event_year = self.CURRENT_YEAR
#             else:
#                 event_year = self.CURRENT_YEAR + 1
#
#         matched = re.match(r'^.*\b(\d\d?)[:.](\d{2}).*$', tail)
#         if not matched:
#             raise DropItem('Дата не найдена')
#         event_hour, event_minute = matched.groups()
#
#         item['when'] = self.format_when(
#             event_year,
#             event_month,
#             event_day,
#             event_hour,
#             event_minute
#         )
#         return item
#
#
# class CheckWhen:
#
#     def __init__(self):
#         self.date_when = None
#
#     def open_spider(self, spider):
#         logger.info('?.(нет в custom settungs) Pipeline CheckWhen начал open_spider')
#         if when := spider.wait_params.get('when'):
#             if '-' in when:
#                 self.date_when = [
#                     datetime.strptime(w, '%d.%m.%Y')
#                     for w in when.split('-')
#                 ]
#             else:
#                 self.date_when = datetime.strptime(when, '%d.%m.%Y')
#
#     def process_item(self, item, spider):
#         logger.info(f'?. pipeline CheckWhen начал process_item')
#         if self.date_when and item['when']:
#             when = datetime(
#                 year=item['when'].year,
#                 month=item['when'].month,
#                 day=item['when'].day
#             )
#             if isinstance(self.date_when, datetime):
#                 if self.date_when == when:
#                     return item
#             if isinstance(self.date_when, list):
#                 if self.date_when[0] <= when <= self.date_when[1]:
#                     return item
#             raise DropItem(f'не подходящая дата {item["when"]}')
#         return item
#
#
# class CheckName:
#
#     def __init__(self):
#         self.check_name = None
#
#     def open_spider(self, spider):
#         logger.info('?.(нет в custom settungs) Pipeline CheckName начал open_spider')
#         if re_name := spider.wait_params.get('re_name'):
#             self.check_name = re_name
#         elif name := spider.wait_params.get('name'):
#             self.check_name = fr'(?i:{name})'
#
#     def process_item(self, item, spider):
#         logger.info(f'?. pipeline CheckName начал process_item')
#         if self.check_name and item['name']:
#             if not re.search(self.check_name, item['name']):
#                 raise DropItem(f'не подходящее имя {item["name"]}')
#         return item
#
#
# class MongoBotsEvents:
#
#     def __init__(self):
#         self.exist_urls = []
#         self.new_urls = []
#         self.bulk = []
#         self.message = ''
#
#     def open_spider(self, spider):
#         logger.info('?.(нет в custom settungs) Pipeline MongoBotsEvents начал open_spider')
#         if client := get_mongo_bots_client():
#             try:
#                 database = client[settings.BOTS_DB]
#                 self.exist_urls = database[settings.EVENTS_COLLECTION].find(
#                     {'source': spider.custom_settings['source']}
#                 ).distinct('url')
#             except Exception:  # pylint: disable=W0703
#                 logger.error('Ошибка в get_event_from_db:',
#                              exc_info=True)
#             finally:
#                 client.close()
#
#     def close_spider(self, spider):
#         logger.info(f'pipeline . начал close_spider')
#         if self.bulk:
#             if client := get_mongo_bots_client():
#                 try:
#                     database = client[settings.BOTS_DB]
#                     database[settings.EVENTS_COLLECTION].delete_one({
#                         '_id': spider.main_event['_id']
#                     })
#                     database[settings.EVENTS_COLLECTION].insert_many(self.bulk)
#                     logger.info('Добавлено  %s  мероприятий.', len(self.bulk))
#                     del self.bulk
#                     users = (spider.main_event['bot_params']['users']
#                              + get_bot_admins())
#                     for user in users:
#                         send_sms_message(str(user), self.message)
#                 except Exception:  # pylint: disable=W0703
#                     logger.error('Ошибка в MongoBotsEvents.close_spider:',
#                                  exc_info=True)
#                 finally:
#                     client.close()
#
#     def process_item(self, item, spider):
#         logger.info(f'?. pipeline CheckName начал process_item')
#         self.validate_item(item)
#
#         if item['url'] in self.new_urls:
#             logger.debug('Пропуск мероприятия. Ссылка уже добавлена '
#                          'в этой сборке: %s', item['url'])
#             return item
#         if item['url'] in self.exist_urls:
#             logger.debug('Мероприятие есть в базе, ссылка: %s', item['url'])
#         else:
#             item['_id'] = str(uuid.uuid4())
#             item['source'] = spider.custom_settings['source']
#             item['bot_params'] = spider.main_event['bot_params']
#             item['bot_status'] = 2
#             item['after_wait'] = True
#             self.message += (f'Появилось ожидаемое мероприятие на '
#                              f'{spider.custom_settings["source"]}. '
#                              f'{item["name"]} Ссылка: {item["url"]}\n')
#             self.bulk.append(dict(item))
#         self.new_urls.append(item['url'])
#         return item
#
#     @staticmethod
#     def is_empty(field: str, item) -> bool:
#         if not item.get(field):
#             return True
#         if isinstance(item[field], str):
#             if re.match(r'\s*$', item[field]):
#                 return True
#         return False
#
#     def validate_item(self, item):
#         important_fields = ['name', 'url', 'when']
#         for field in important_fields:
#             if self.is_empty(field, item):
#                 raise DropItem(f'Отсутствует поле {field} в Event')
#         if item['when'] < datetime.now():
#             raise DropItem('мероприятие прошло.')

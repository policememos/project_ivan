import logging
import re
import json
from urllib.parse import urlparse
from itertools import islice

import scrapy
import yaml

from spiders.ticketland.helpers import get_free_account, dump_cookies
from spiders.enum import BotMode
from spiders.base import BotSpider as BaseBotSpider
from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.spiders.ticketland.bot')

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)


class BotSpider(BaseBotSpider):  # pylint: disable=R0902,R0904
    """
    event_url:
        https://krasn.ticketland.ru/koncertnye-zaly/krasnoyarskaya-kraevaya-filarmoniya/muzykoterapiya-dlya-buduschikh-mam-risuem-muzyku/20211211_1200-4363196/
    """

    custom_settings = {
        'ITEM_PIPELINES': {
            'spiders.pipelines.InitParamsAndCheckActuality': 100,
            'spiders.pipelines.CheckTicket': 110,
            'spiders.ticketland.pipelines.BotTicketlandParseMode': 125,
            'spiders.ticketland.pipelines.AccountPipeline': 150,
            'spiders.pipelines.BotBuyMode': 200,
        },
        'source': 'ticketland',
        'tls_session': True,
        'recreate_tls_session': True,
        'DOWNLOAD_DELAY': 0.5,
    }
    handle_httpstatus_list = [403, 404, 500]

    def __init__(self, **kwargs):
        self.csrf = None
        with open('spiders/ticketland/config.yml', 'r',
                  encoding='utf-8') as file:
            self.user_agent = yaml.safe_load(file)['user_agent']
        self.headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json',
            'User-Agent': self.user_agent,
            'Referer': 'https://bdt.spb.ru/',
        }
        self.is_full_stand = False
        self.selected_tickets = []
        self.login_attempts = 3

        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):
        if not getattr(self.event, 'url', None):
            logger.error('Не передан event_url')
            return
        if referer := getattr(self.event, 'referer', None):
            self.headers['Referer'] = referer
        if self.mode == BotMode.PARSE:
            yield scrapy.Request(
                url=self.event.url,
                headers=self.headers,
                cookies={'tlsid': ''},
                callback=self.parse_event,
                dont_filter=True,
            )
        elif self.mode == BotMode.BUY and self.account:
            self.headers['Cookie'] = self.account.get('cookies', '')
            yield scrapy.Request(
                url='https://www.ticketland.ru/private/orders/',
                headers=self.headers,
                callback=self.check_auth,
                dont_filter=True,
                meta={'dont_merge_cookies': True}
            )

    def login_ticketland(self):
        logger.info('Авторизовываем аккаунт %s', self.account['email'])
        yield scrapy.Request(
            method='POST',
            url='https://www.ticketland.ru/spa-api/login/email',
            headers=self.headers,
            body=json.dumps({
                'email': self.account['email'],
                'password': self.account['password']
            }),
            meta={'dont_merge_cookies': True},
            callback=self.dump_login_cookie,
            dont_filter=True,
        )

    def dump_login_cookie(self, response):
        cookies = ''
        for _cookie in response.headers.getlist('Set-Cookie'):
            _cookie = _cookie.decode('utf-8')
            cookie = str(_cookie).split(';', maxsplit=1)[0].split('=')
            cookies += f'{cookie[0]}={cookie[1]}; '
        cookies = re.sub(r'tlsid=.*;\s?', '', cookies).strip()
        self.account['cookies'] = cookies
        dump_cookies(cookies, self.account)
        yield from self.start_requests()

    @staticmethod
    def get_cookie(response):
        for _cookie in response.headers.getlist('Set-Cookie'):
            _cookie = _cookie.decode('utf-8')
            if 'tlsid' in str(_cookie):
                cookie = str(_cookie).split(';', maxsplit=1)
                return cookie[0].split('=')[1]
        return ''

    def check_auth(self, response):
        if response.status == 403 or 'login' in response.url:
            if self.login_attempts > 0:
                self.login_attempts -= 1
                logger.info('Аккаунт не авторизован на сайте')
                yield from self.login_ticketland()
                return
            logger.info('Все попытки авторизоваться провалены')
            yield from self.switch_account()
            return
        if tlsid := self.get_cookie(response):
            logger.debug('Получил tlsid: %s', tlsid)
            self.headers['Cookie'] = f'tlsid={tlsid}'
        else:
            logger.error('Не удалось получить tlsid')
            yield from self.switch_account()
            return

        yield scrapy.Request(
            url=self.event.url,
            headers=self.headers,
            callback=self.parse_csrf,
            dont_filter=True,
            meta={'dont_merge_cookies': True}
        )

    def switch_account(self):
        self.switch_accounts -= 1
        if self.switch_accounts >= 0:
            self.account = get_free_account()
            self.login_attempts = 3
            yield from self.start_requests()
        else:
            logger.error('Перепробовали разные аккаунты и авторизацию')
            return

    def parse_csrf(self, response):
        self.base_url = urlparse(response.url).netloc
        self.csrf = response.xpath('//meta[@name="csrf-token"]/@content').get()
        yield self.create_select_request()

    def create_select_request(self):
        ticket = self.tickets.pop()
        self.selected_tickets.append(ticket)
        if ticket['stand']:
            if ticket.get('performance_basis_id'):
                self.is_full_stand = True
                url = f'https://{self.base_url}/hallPlace/futureTicketAdd/'
                body = self.get_full_stand_body(ticket)
            else:
                url = f'https://{self.base_url}/hallPlace/entranceTicketsAdd/'
                body = self.get_stand_body(ticket)
        else:
            url = f'https://{self.base_url}/hallPlace/select/'
            body = self.get_seat_body(ticket)
        ticket.success = True
        headers = self.headers
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
        return scrapy.Request(
            url=url,
            method='POST',
            body=body,
            headers=headers,
            callback=self.select_ticket,
            dont_filter=True,
            meta={'dont_merge_cookies': True, 'last_ticket': ticket}
        )

    @staticmethod
    def get_stand_body(ticket):
        return (f'count=1'
                f'&performanceId={ticket["performance_id"]}'
                f'&sectionId={ticket["section_id"]}'
                f'&taxId={ticket["tax_id"]}'
                f'&ticketPrice={ticket["price"]}'
                f'&isSpecialSale=0')

    @staticmethod
    def get_full_stand_body(ticket):
        return (f'performanceId={ticket["performance_id"]}'
                f'&performanceBasisId={ticket["performance_basis_id"]}'
                f'&sectionId={ticket["section_id"]}'
                f'&count=1'
                f'&taxId={ticket["tax_id"]}'
                f'&ticketPrice={ticket["price"]}'
                f'&sectionName={ticket["sector"]}'
                f'&fundId={ticket["fund_id"]}'
                f'&organizerId={ticket["organizer_id"]}'
                f'&showId={ticket["show_id"]}'
                f'&buildingId={ticket["building_id"]}')

    def get_seat_body(self, ticket):
        return (f'cypher={ticket["cypher"]}&tax={ticket["tax"]}'
                f'&tl-csrf={self.csrf}')

    def select_ticket(self, response):
        logger.debug('def select_ticket: %s', response.text)
        if 'в одном заказе' in response.text:
            logger.error('В корзине достигнут лимит, не удалось закупить')
            return
        yield response.meta['last_ticket']
        if self.tickets:
            yield self.create_select_request()
        elif self.is_full_stand:
            yield scrapy.Request(
                url=f'https://{self.base_url}/shopcart/',
                headers=self.headers,
                callback=self.parse_cart,
                dont_filter=True,
                meta={'dont_merge_cookies': True}
            )
        else:
            yield self.set_payment_request()

    def set_payment_request(self):
        headers = self.headers
        headers['Content-Type'] = 'application/json'
        method = getattr(self.event, 'payment_method', None) or "BANK_CARD"
        return scrapy.Request(
            url=f'https://{self.base_url}/api/shopcart/paymentMethodChange',
            method='POST',
            body=f'{{"paymentMethodType":"{method}"}}',
            headers=headers,
            callback=self.set_payment_method,
            dont_filter=True,
            meta={'dont_merge_cookies': True}
        )

    def set_payment_method(self, response):
        logger.debug('def set_payment_method, %s', response.text)
        if response.status == 200:
            yield self.create_order_request(response)
        else:
            logger.error('Ошибка при выборе метода оплаты: %s', response.text)

    def create_order_request(self, response):
        self.headers['Content-Type'] = 'application/json'
        self.headers['Referer'] = f'https://{self.base_url}/shopcart/'
        return scrapy.Request(
            url=f'https://{self.base_url}/api/order/create',
            method='POST',
            body='{}',
            headers=self.headers,
            callback=self.create_order,
            errback=self.log_error,
            dont_filter=True,
            meta={'dont_merge_cookies': True, 'shopcart_items': response.text}
        )

    @staticmethod
    def log_error(fail):
        response = fail.value.response
        if response.status == 500:
            logger.error('Ошибка при создании заказа: %s', response.text)
        else:
            logger.error('Код не 200, текст ошибки: %s', response.text)

    def parse_cart(self, response):
        if 'корзина пуста' in response.text:
            logger.info('Корзина пуста')
        else:
            yield self.set_payment_request()

    @staticmethod
    def make_order_list(data):
        json_obj = json.loads(data.replace('\'', '"'))
        items = json_obj['items']
        return items

    def create_order(self, response):
        logger.debug('def create_order: %s', response.text)
        try:
            j_data = json.loads(response.text)
        except json.decoder.JSONDecodeError:
            yield self._retry(response.request, ValueError, self)
            return

        if order_id := j_data.get('orderId'):
            self.headers['Referer'] = (f'https://{self.base_url}'
                                       f'/shopcart/pay/{order_id}')
            yield scrapy.Request(
                url=(f'https://{self.base_url}/api'
                     f'/payment/instructions/{order_id}'),
                headers=self.headers,
                callback=self.parse_payment_url,
                dont_filter=True,
                meta={
                    'dont_merge_cookies': True,
                    'orderId': order_id,
                    **response.meta,
                }
            )
        else:
            logger.error('Не удалось создать заказ')

    def parse_payment_url(self, response):  # noqa: C901
        try:
            j_data = json.loads(response.text)
        except json.decoder.JSONDecodeError:
            yield self._retry(response.request, ValueError, self)
            return
        logger.debug('def parse_payment_url: %s', j_data)
        if pay_type := j_data.get('type', {}):
            match pay_type:
                case 'BANK_IFRAME':
                    payment_url = j_data['details']['bankIframeUrl']
                case 'QRCODE':
                    payment_url = j_data['details']['qrCode']['paymentUrl']
                case 'WIDGET':
                    payment_url = (f'https://{self.base_url}/shopcart/pay/'
                                   f'{response.meta["orderId"]}/?repeated=true')
                case _:
                    logger.error('Неизвестный тип оплаты: %s', pay_type)
                    return
            yield {
                'payment_url': payment_url,
                'message': f'EMAIL: {self.account["email"]}',
                'email': self.account["email"],
            }
        else:
            logger.error('Не найден URL для оплаты: %s', response.text)

    @staticmethod
    def get_stand_info(response):
        script = response.xpath(
            '//script[contains(text(), "performance_basis_id")]/text()'
        ).get('')
        matched = re.search(r'performanceInfo:\s(\{\".*}),', script)
        return json.loads(matched.group(1)) if matched else None

    def parse_event(self, response):
        tlsid, stand_info = self.init_params(response)
        if tlsid is None and stand_info is None:
            return
        self.event_id = self.event_id_perf(response
                                           ) or self.event_id_obj(response)
        if not self.event_id:
            logger.error('Не найден event_id')
            return

        if sections := self.extract_sections(response):
            for sections_chunk in self.chunk(sections.items(), 10):
                yield scrapy.Request(
                    url=response.url,
                    headers=self.headers,
                    cookies={'tlsid': ''},
                    callback=self.parse_section_chunk,
                    dont_filter=True,
                    meta={'sections': sections_chunk,
                          'stand_info': stand_info},
                )
        else:
            yield scrapy.Request(
                url=self.get_map_url(),
                headers=self.headers,
                cookies={'tlsid': tlsid},
                callback=self.parse_tickets,
                dont_filter=True,
                meta={'stand_info': stand_info},
            )

    def init_params(self, response):
        if response.status == 404:
            logger.info('Страница вернулась 404 кодом')
            return None, None

        self.base_url = urlparse(response.url).netloc
        self.csrf = response.xpath('//meta[@name="csrf-token"]/@content').get()
        tlsid = self.extract_tlsid(response)
        stand_info = self.get_stand_info(response)
        if tlsid is None and stand_info is None:
            logger.debug('def init_params: не найден tlsid и stand_info')
        return tlsid, stand_info

    def parse_section_chunk(self, response):
        tlsid, stand_info = self.init_params(response)
        if tlsid is None and stand_info is None:
            return
        for section, count in response.meta['sections']:
            if int(count) > 0:
                yield scrapy.Request(
                    url=self.get_map_section_url(section),
                    headers=self.headers,
                    cookies={'tlsid': tlsid},
                    callback=self.parse_tickets,
                    dont_filter=True,
                    meta={'stand_info': stand_info},
                )

    def extract_tlsid(self, response):
        if 'sessionId' in response.url:
            return ''
        for _cookie in response.headers.getlist('set-cookie'):
            if 'tlsid' in str(_cookie):
                return str(_cookie).split(';', maxsplit=1)[0].split('=')[1]
        if response.url != self.event.url:
            logger.info('Билеты на мероприятие не продаются')
        else:
            logger.error('Не найден tlsid')
        return None

    @staticmethod
    def event_id_perf(response):
        script = response.xpath(
            '//script[contains(text(), "performanceId:")]'
        ).get('')
        matched = re.search(r'performanceId:\s(?P<_id>\d+)', script)
        return matched['_id'] if matched else ''

    @staticmethod
    def event_id_obj(response):
        script = response.xpath(
            '//script[contains(text(), "TLand.Pages.Hall.params.objId")]'
        ).get('')
        matched = re.search(r'objId\s=\s(?P<_id>\d+)', script)
        return matched['_id'] if matched else ''

    def extract_sections(self, response):
        script = response.xpath(
            '//script[contains(text(), "TLand.Pages.Hall.params.sections")]'
        ).get(default='')
        sections = re.search(
            r'params.sections\s=\s(?P<sections>{.*?});',
            script,
        )
        if sections:
            try:
                return json.loads(sections['sections'])
            except json.decoder.JSONDecodeError:
                logger.error('Не получилось декодировать JSON в sections')
                self.crawler.stats.inc_value('broken_spider')
        return None

    def get_map_section_url(self, section):
        return (f'https://{self.base_url}/hallview/map/{self.event_id}/'
                f'{section}/?json=1&all=1&tl-csrf={self.csrf}')

    def get_map_url(self):
        return (f'https://{self.base_url}/hallview/map/'
                f'{self.event_id}/?json=1&all=1&tl-csrf={self.csrf}')

    @staticmethod
    def chunk(_it, size):
        return iter(lambda: tuple(islice(iter(_it), size)), ())

    @staticmethod
    def get_price(ticket):
        price = ticket.get('price')
        if not price and ticket.get('taxes'):
            base_tariff = ticket['taxes'][min(ticket['taxes'].keys())]
            price = base_tariff['price'] + base_tariff.get('serviceFee', 0)
        return price

    @staticmethod
    def get_tariff(ticket):
        price = ticket.get('price')
        tax_id = ticket['tariff']
        if not price and ticket.get('taxes'):
            tax_id = min(ticket['taxes'].keys())
            base_tariff = ticket['taxes'][tax_id]
            price = base_tariff['price'] + base_tariff.get('serviceFee', 0)
        return price, tax_id

    @staticmethod
    def is_stand_ticket(ticket):
        state = ticket.get('state')
        ttype = ticket.get('type')
        if isinstance(state, (int, str)) and int(state) != 0:
            return None
        if (isinstance(ttype, (int, str)) and int(ttype) == 1) or (
                state is None and ttype is None):
            return True
        return False

    def parse_places(self, json_data):
        stand_places = {}
        places = []
        for ticket in json_data.get('places', []):
            sector = ticket['section']['name']
            price, tax_id = self.get_tariff(ticket)
            if is_stand := self.is_stand_ticket(ticket):
                count = stand_places.get(sector, {}).get('count', 0) + 1
                stand_places[sector] = {
                    'stand': True,
                    'sector': sector,
                    'price': int(price),
                    'performance_id': self.event_id,
                    'tax_id': tax_id,
                    'section_id': ticket['section']['id'],
                    'count': count,
                }

            elif is_stand is False:
                row = ticket['row']
                seat = ticket['place']
                if all([sector, row, seat, price]):
                    places.append(Ticket({
                        'stand': False,
                        'sector': sector,
                        'row': row,
                        'seat': seat,
                        'price': price,
                        'cypher': ticket['cypher'],
                        'tax': tax_id,
                    }))
        return places, stand_places

    def parse_sections(self, json_data, meta):
        if 'places' in json_data:
            return
        if 'sections' in json_data and meta.get('stand_info'):
            s_info = meta['stand_info']
            for ticket in json_data['sections']:
                sector = ticket['sectionName']
                price = int(ticket['price'])
                yield Ticket({
                    'stand': True,
                    'sector': sector,
                    'price': price,
                    'performance_id': self.event_id,
                    'section_id': ticket['sectionId'],
                    'tax_id': ticket['tariffId'],
                    'count': ticket['placeCount'],
                    'fund_id': ticket['fundId'],
                    'performance_basis_id': s_info['performance_basis_id'],
                    'organizer_id': s_info['organizer_id'],
                    'show_id': s_info['show_id'],
                    'building_id': s_info['building_id'],
                })

    def parse_tickets(self, response):
        try:
            json_data = json.loads(response.text)
        except json.decoder.JSONDecodeError:
            yield self._retry(response.request, ValueError, self)
            return
        places, stand_places = self.parse_places(json_data)
        yield from places

        yield from self.parse_sections(json_data, response.meta)

        for stand in stand_places.values():
            yield Ticket(stand)

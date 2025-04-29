import json
import logging
import re

from urllib.parse import urlencode, urlparse, parse_qs, unquote

import scrapy
from scrapy.selector import Selector

from spiders.enum import BotMode
from spiders.base import BotSpider as BaseBotSpider
from spiders.helpers import send_sms_message
from spiders.mongo_utils import get_bot_admins
from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.spiders.etihadarena.bot')
logging.basicConfig(level=logging.DEBUG, filename="MYLOG.log",filemode="w")


class BotSpider(BaseBotSpider):  # pylint: disable=R0904
    """
    В ссылке обязательно должны быть performanceAk и return

    event_url:
        https://tickets.etihadarena.ae/yba_b2c/buy-tickets.html?performanceAk=YBA.EVN127.PRF1&return=https://www.etihadarena.ae/en/event-booking/modi-alshamrani-and-miami-band
    """

    custom_settings = {
        'ITEM_PIPELINES': {
            'spiders.pipelines.InitParamsAndCheckActuality': 100,
            'spiders.etihadarena.pipelines.BotEtihadArenaQuickBuyPipeline': 105,
            'spiders.pipelines.CheckTicket': 110,
            'spiders.etihadarena.pipelines.BotEtihadArenaParseMode': 125,
            'spiders.pipelines.BotBuyMode': 200,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 102,
            'spiders.middlewares.CookiesAndHeadersMiddleware': 103,
            # 'spiders.middlewares.ProxyMiddleware': 105,
            # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            'spiders.middlewares.TlsSessionMiddleware': 115,
            'spiders.middlewares.RandomUserAgentMiddleware': 600,
            'spiders.middlewares.TooManyRequestsRetryMiddleware': 630,
            'spiders.middlewares.SpiderRetryMiddleware': 640,
        },
        'source': 'etihadarena',
        'CONCURRENT_REQUESTS': 100,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        'CONCURRENT_ITEMS': 200,
        'DOWNLOAD_DELAY': 0.1,
        'tls_session': True,
    }

    def __init__(self, **kwargs):
        self.csrf = None
        self.ssid = None
        self.product_id = None
        self.cart_id = None
        self.return_url = ''
        self.queue_cookies = {}
        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):
        if not getattr(self.event, 'url', None):
            logger.error('Не передан event_url')
            return
        self.init_params()
        yield scrapy.Request(
            url=self.event.url,
            callback=self.parse_queue_url,
            dont_filter=True,
        )

    def parse_queue_url(self, response):
        if matched := re.search(r"decodeURIComponent\('([^']+)", response.text):
            url = unquote(matched.group(1))
            if 'https://' not in url:
                url = 'https://bestunion.queue-it.net' + url
            yield scrapy.Request(
                url=url,
                callback=self.parse_queue_cookies,
                dont_filter=True,
            )
        else:
            yield from self.parse_queue_cookies(response)

    @staticmethod
    def get_queue_cookies(response):
        queue_cookies = {}
        for _cookie in response.headers.getlist('Set-Cookie'):
            _cookie = _cookie.decode('utf-8')
            if 'Queue-it-token' in _cookie:
                cookie = _cookie.split(';', maxsplit=1)[0].split('=')
                queue_cookies['Queue-it-token'] = cookie[1]
            if 'QueueITAccepted' in _cookie:
                cookie = _cookie.split(';', maxsplit=1)
                cookie = cookie[0].split('=', maxsplit=1)
                queue_cookies[cookie[0]] = cookie[1]
        return queue_cookies

    def parse_queue_cookies(self, response):  # noqa: C901
        if 'decodeURIComponent' not in response.text:
            if self.mode == BotMode.PARSE:
                ind = 0
                count_orders = count_tickets = 0
                for sector in self.get_sectors(response):
                    tasks = 1
                    if sector.get('quick'):
                        tasks = sector['count'] // (self.event.max_tickets or 4)
                        count_tickets += sector['count']
                        count_orders += tasks
                    for _ in range(tasks or 1):
                        ind += 1
                        logger.info(f'Это парс, иду из parse_queue_cookies в self.start_new_parse_session')
                        yield scrapy.Request(
                            url=self.event.url,
                            callback=self.start_new_parse_session,
                            cookies=self.get_queue_cookies(response),
                            dont_filter=True,
                            meta={'sector': sector, 'cookiejar': ind},
                        )
                if count_orders and count_tickets:
                    message = (f'ОТПРАВЛЕНО {count_orders} ЗАКУПОК etihadarena '
                               f'{self.event.name} ({self.event.when}) '
                               f'{self.event.url}: {count_tickets} билетов')
                    logger.info(message)
                    for user in get_bot_admins():
                        send_sms_message(str(user), message)
            elif self.mode == BotMode.BUY:
                self.csrf = response.xpath(
                    '//meta[@name = "_csrf"]/@content'
                ).get('')
                quant = self.event.max_tickets if self.tickets[0].get(
                    'stand') else len(self.tickets)
                yield scrapy.Request(
                    method='POST',
                    url='https://tickets.etihadarena.ae/yba_b2c/add/tickets',
                    callback=self.add_ticket,
                    body=self.get_add_tickets_body(self.ssid, quant),
                    headers={
                        'accept': '*/*',
                        'accept-language': 'ru,en',
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    dont_filter=True,
                )
        else:
            logger.error('Не удалось получить куки очереди')

    def start_new_parse_session(self, response):
        csrf = response.xpath('//meta[@name = "_csrf"]/@content').get('')
        sector = response.meta['sector']
        ssid = re.search(r'ssId=(.*)$', sector['sector_url']).group(1)
        quant = 1
        if sector.get('quick'):
            quant = min(self.event.max_tickets, sector['count'])
        yield scrapy.Request(
            method='POST',
            url='https://tickets.etihadarena.ae/yba_b2c/add/tickets',
            callback=self.add_ticket,
            body=self.get_add_tickets_body(
                ssid, quant=quant, sector=sector, csrf=csrf
            ),
            headers={
                'accept': '*/*',
                'accept-language': 'ru,en',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            dont_filter=True,
            meta={
                'cookiejar': response.meta['cookiejar'],
                'sector': sector,
                'ssid': ssid,
                'csrf': csrf
            })

    def init_params(self):
        if self.tickets:
            self.ssid = self.tickets[0]['ssid']
            self.product_id = self.tickets[0]['product_id']
        if matched := re.search(r'performanceAk=(.*)&return', self.event.url):
            self.event_id = matched.group(1)
        if matched := re.search(r'return=(.*)$', self.event.url):
            self.return_url = matched.group(1)

    def get_ticket_body(self, body, ind, quant, product_id, ssid):
        body.update({
            f'addToCartRequests[{ind}].performanceAks': self.event_id,
            f'addToCartRequests[{ind}].product': product_id,
            f'addToCartRequests[{ind}].skipUpsell': 'true',
            f'addToCartRequests[{ind}].quantity': quant,
        })
        if ssid:
            body.update({
                f'addToCartRequests[{ind}].spaceStructureAk': ssid,
            })

    def get_add_tickets_body(self, ssid, quant=1, sector=None, csrf=None):
        ind = 0
        csrf = csrf or self.csrf
        product_id = self.product_id or sector.get('seat_id')
        body = {
            '_csrf': csrf,
            'availabilityPair': '0'
        }
        self.get_ticket_body(body, ind, quant, product_id, ssid)
        products = []
        if sector:
            products = sector.get('products') or []
        elif self.tickets:
            products = self.tickets[0].get('products') or []
        for product in products:
            ind += 1
            self.get_ticket_body(body, ind, 0, product, ssid)
        if ssid:
            body.update({
                'spaceStructureAk': ssid,
            })
        return urlencode(body)

    def add_ticket(self, response):
        cart_id = Selector(text=response.text).xpath(
            '//div[contains(@class, "prodrow")]/@id'
        ).get('')
        if self.mode == BotMode.PARSE:
            if response.meta['sector'].get('stand'):
                sector_data = response.meta['sector']
                count = min(self.event.max_tickets or 4, sector_data['count'])
                ticket = Ticket({
                    'stand': True,
                    'ssid': sector_data['ssid'],
                    'product_id': sector_data['seat_id'],
                    'product_name': sector_data['product_name'],
                    'sector_url': response.url,
                    'sector': sector_data['product_name'],
                    'price': sector_data['price'],
                    'products': sector_data['products'],
                    'count': count,
                    'cond_index': sector_data.get('cond_index'),
                    'priority': sector_data.get('priority'),
                })
                if sector_data.get('quick'):
                    response.meta['tickets'] = [ticket]
                    yield self.checkout_request(
                        response, sector_data['ssid']
                    )
                else:
                    yield ticket
            else:
                response.meta['cart_id'] = cart_id
                yield scrapy.Request(
                    url=response.meta['sector']['sector_url'],
                    callback=self.parse_sector,
                    dont_filter=True,
                    meta=response.meta
                )
        else:
            self.cart_id = cart_id
            if self.tickets[0].get('stand'):
                self.tickets[0].success = True
                yield self.tickets[0]
                yield self.checkout_request(response)
            else:
                yield scrapy.Request(
                    url=self.tickets[0]['sector_url'],
                    callback=self.parse_added_tickets,
                    dont_filter=True,
                    meta=response.meta
                )

    def parse_added_tickets(self, response):
        _, hold_seat_ids = self.get_seats(response)
        if hold_seat_ids:
            logger.info('Нам добавили %s билетов, идем чистить корзину',
                        len(hold_seat_ids))
            seat_id = hold_seat_ids.pop(0)
            response.meta['holds'] = hold_seat_ids
            yield self.release_request(self.ssid, seat_id, response.meta)
        else:
            logger.info('Не нашлось данных в bestSeatsList (автобронь), '
                        'пробуем закупаться без чистки корзины')
            yield scrapy.Request(
                method='POST',
                url=('https://tickets.etihadarena.ae/yba_b2c'
                     '/post/seats/holdandadd'),
                callback=self.hold_tickets,
                body=self.get_hold_body(),
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                dont_filter=True,
                meta=response.meta)

    def checkout_request(self, response, ssid=None):
        response.meta['tls_params'] = {'allow_redirects': False}
        return scrapy.Request(
            url=('https://tickets.etihadarena.ae/yba_b2c'
                 f'/checkout.html?ssId={self.ssid or ssid}'),
            callback=self.checkout,
            headers={'upgrade-insecure-requests': '1'},
            cookies={'returnUrl': self.return_url},
            dont_filter=True,
            meta={
                **response.meta,
                'handle_httpstatus_list': [302]
            }
        )

    @staticmethod
    def get_payment_url(response):
        if location := response.headers.getlist('Location'):
            location = location[0] or ''
            if isinstance(location, bytes):
                location = location.decode('utf-8')
            return location if 'sale=' in location else None
        return None

    def checkout(self, response):  # noqa: C901
        url = self.get_payment_url(response)
        if self.mode == BotMode.BUY:
            if url:
                yield {'payment_url': url}
            else:
                logger.error('Не удалось создать заказ, чистим корзину')
                for seat in self.tickets:
                    if seat_id := seat.get('seat_id'):
                        yield self.release_request(
                            self.ssid, seat_id, response.meta
                        )
        elif url:
            if tickets := response.meta.get('tickets'):
                yield {
                    'payment_url': url,
                    'tickets': tickets
                }
            else:
                url_parsed = urlparse(url)
                query = url_parsed.query
                if sale_id := parse_qs(query).get('sale'):
                    response.meta['payment_url'] = url
                    yield scrapy.Request(
                        url=('https://apis.farahexperiences.com/v1'
                             '/ordersinfo/seatInfo'
                             f'/{sale_id[0]}?tenantid=YAB2C'),
                        callback=self.parse_buy_tickets,
                        headers={
                            'Accept': 'application/json',
                            'Content-Type': 'application/json'
                        },
                        meta=response.meta
                    )
        else:
            logger.error('Не удалось создать заказ в quick режиме')

    @staticmethod
    def parse_buy_tickets(response):
        logger.debug(response.text)
        j_data = response.json().get('orderdetails') or {}
        tickets = []
        for item in j_data.get('order', {}).get('items') or []:
            if raw_tickets := item.get('tickets'):
                for ticket in raw_tickets:
                    seat = ticket.get('Seat') or {}
                    tickets.append(Ticket({
                        'stand': False,
                        'sector': response.meta['sector']['product_name'],
                        'price': response.meta['sector']['price'],
                        'row': seat.get('rowLabel') or seat.get('row'),
                        'seat': seat.get('colLabel') or seat.get('col'),
                        'cond_index': response.meta['sector']['cond_index'],
                        'count': 1
                    }))
            elif count := int(item.get('quantity') or 0):
                tickets.append(Ticket({
                    'stand': True,
                    'sector': response.meta['sector']['product_name'],
                    'price': response.meta['sector']['price'],
                    'count': count,
                    'cond_index': response.meta['sector']['cond_index'],
                }))
        if tickets:
            yield {
                'payment_url': response.meta['payment_url'],
                'tickets': tickets
            }

    def hold_tickets(self, response):  # noqa: C901
        if self.mode == BotMode.PARSE:
            yield self.checkout_request(
                response, response.meta['sector']['ssid']
            )
            return
        selected_seats = response.xpath('//li[@class="seatElem"]').getall()
        if len(selected_seats) == len(self.tickets):
            for ticket in self.tickets:
                ticket.success = True
            yield self.checkout_request(response)
        else:
            logger.error('Все наши билеты перехватили')
            if len(self.tickets) == 1:
                self.tickets[0].success = False

        for ticket in self.tickets:
            if ticket.success is not None:
                yield ticket

    def release_ticket(self, response):
        if holds := response.meta.get('holds'):
            seat_id = holds.pop(0)
            response.meta['holds'] = holds
            yield self.release_request(self.ssid, seat_id, response.meta)
        elif self.mode == BotMode.BUY:
            logger.info('Все билеты убраны из корзины, идем бронировать наши')
            yield scrapy.Request(
                method='POST',
                url=('https://tickets.etihadarena.ae/yba_b2c'
                     '/post/seats/holdandadd'),
                callback=self.hold_tickets,
                body=self.get_hold_body(),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                dont_filter=True,
                meta=response.meta
            )

    def get_hold_body(self, meta=None, holds=None):
        if meta and holds:
            csrf = meta['csrf']
            ssid = meta['sector']['ssid']
            cart_id = meta['cart_id']
            tickets = holds
        else:
            csrf = self.csrf
            ssid = self.ssid
            cart_id = self.cart_id
            tickets = [t['seat_id'] for t in self.tickets]
        body = f'_csrf={csrf}&spaceStructureAK={ssid}'
        for ind, ticket in enumerate(tickets):
            body += f'&seats%5B{ind}%5D={ticket}'
        body += f'&cartItemIndex={cart_id}&show3DView=true'
        return body

    @staticmethod
    def get_seats(response):
        script = response.xpath(
            '//script[contains(text(), "availableSeats")]/text()'
        ).get(default='')
        raw_seats = re.search(r'availableSeats:\s(\[.*]),\s', script)
        best_seats = re.search(r'bestSeatsList:\s(\[.*]),\s', script)
        try:
            seats = json.loads(raw_seats.group(1)) if raw_seats else []
            seats.sort(key=lambda x: (x['row'], x['col']))
            hold_seat_ids = json.loads(
                best_seats.group(1)) if best_seats else []
            return seats, [h['id'] for h in hold_seat_ids]
        except json.decoder.JSONDecodeError:
            return [], []

    def parse_sector(self, response):  # noqa: C901
        if ssid := re.search(r'ssId=(.*)$', response.url):
            ssid = ssid.group(1)
        else:
            logger.error('Не найден ssId')
            return
        sector_data = response.meta['sector']
        try:
            tickets, hold_seat_ids = self.get_seats(response)
        except Exception:  # pylint: disable=W0703
            yield self._retry(response.request, ValueError, self)
            return
        tickets = self.extract_seats(tickets, response, ssid)
        if sector_data.get('quick'):
            response.meta['tickets'] = []
            for ticket in tickets:
                if ticket['seat_id'] in hold_seat_ids:
                    response.meta['tickets'].append(ticket)
            yield scrapy.Request(
                method='POST',
                url=('https://tickets.etihadarena.ae/yba_b2c'
                     '/post/seats/holdandadd'),
                callback=self.hold_tickets,
                body=self.get_hold_body(response.meta, hold_seat_ids),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                dont_filter=True,
                meta=response.meta
            )
        else:
            yield from tickets
            for seat_id in hold_seat_ids:
                yield self.release_request(ssid, seat_id, response.meta)

    @staticmethod
    def extract_seats(tickets, response, ssid):
        seats = []
        for ticket in tickets:
            sector_data = response.meta['sector']
            seats.append(Ticket({
                'stand': False,
                'ssid': ssid,
                'product_id': sector_data['seat_id'],
                'product_name': sector_data['product_name'],
                'products': sector_data['products'],
                'sector_url': response.url,
                'sector': sector_data['product_name'],
                'row': ticket['rowLabel'],
                'seat': ticket['colLabel'],
                'price': sector_data['price'],
                'seat_id': ticket['id'],
                'cond_index': sector_data.get('cond_index'),
                'priority': sector_data.get('priority'),
            }))
        return seats

    def release_request(self, ssid, seat_id, meta):
        csrf = meta.get('csrf') or self.csrf
        cart_id = meta.get('cart_id') or self.cart_id
        return scrapy.Request(
            method='POST',
            url='https://tickets.etihadarena.ae/yba_b2c'
                '/post/seats/releaseandremove',
            body=urlencode({
                '_csrf': csrf,
                'show3DView': 'true',
                'spaceStructureAK': ssid,
                'cartItemIndex': cart_id,
                'seats': seat_id,
            }),
            headers={'Content-type': 'application/x-www-form-urlencoded;'},
            callback=self.release_ticket,
            dont_filter=True,
            meta=meta
        )

    def get_sectors(self, response):
        sectors = []
        for sector in self.extract_sectors(response):
            sector_name = re.sub(
                r'\s+', ' ', str(sector['product_name'])
            ).strip()
            for condition in self.conditions:
                if (condition.check_sector(sector_name)
                        and condition.check_price(sector['price'])):
                    sector['quick'] = condition.quick
                    if condition.quick and sector['count']:
                        count = (min(condition.count, sector['count'])
                                 if condition.count else sector['count'])
                        sector.update({
                            'count': count,
                            'cond_index': condition.index,
                            'priority': condition.priority,
                        })
                    sectors.append(sector)
                    break
        return sorted(sectors, key=lambda s: s.get('priority', 0))

    @staticmethod
    def extract_sectors(response):
        sectors = []
        raw_sectors = response.xpath('//div[@data-sector]').getall()
        for sector in raw_sectors:
            selector = Selector(text=sector)
            sector_url = selector.xpath('//a[@data-seatid]/@href').get('')
            sector_options = selector.xpath('//div[@class="products"]').getall()
            products = set(selector.xpath(
                '//div[@class="products"]'
                '//input[contains(@name, ".product")]/@value'
            ).getall())
            for option in sector_options:
                selector = Selector(text=option)
                seat_id = selector.xpath(
                    '//input[contains(@name, ".product")]/@value'
                ).get('')
                count = selector.xpath(
                    '//div[@data-availability]/@data-availability'
                ).get('')
                product_name = selector.xpath(
                    '//div[@data-analyticsname]/@data-analyticsname'
                ).get('')
                price = selector.xpath(
                    '//span[@class="product-price"]/text()').get('')
                price = re.sub(r'[^\d.]', '', price)
                if all([sector_url, seat_id, price]):
                    if ssid := re.search(r'ssId=(.*)$', sector_url):
                        ssid = ssid.group(1)
                    o_products = products.copy() - {seat_id}
                    sectors.append({
                        'stand': 'checkout' in sector_url,
                        'price': price,
                        'seat_id': seat_id,
                        'ssid': ssid,
                        'product_name': product_name,
                        'products': list(o_products),
                        'sector_url': 'https://tickets.'
                                      'etihadarena.ae' + sector_url,
                        'count': int(count) if count else 15
                    })
        return sectors

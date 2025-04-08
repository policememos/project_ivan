import json
import logging
import re
from urllib.parse import urlencode
import scrapy
from scrapy.selector import Selector

from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.spiders.etihadarena')


class EtihadarenaBot(scrapy.Spider):

    name = 'etihadarena'
    custom_settings = {
        'ITEM_PIPELINES': {
            'spiders.pipelines.InitParamsAndCheckActuality': 100,
            'spiders.pipelines.CheckTicket': 110,
            'spiders.etihadarena.pipelines.BotEtihadArenaParseMode': 125,
            'spiders.pipelines.BotBuyMode': 200,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 102,
            'spiders.middlewares.CookiesAndHeadersMiddleware': 103,
            'spiders.middlewares.ProxyMiddleware': 105,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            'spiders.middlewares.TlsSessionMiddleware': 115,
            'spiders.middlewares.RandomUserAgentMiddleware': 600,
            'spiders.middlewares.TooManyRequestsRetryMiddleware': 630,
            'spiders.middlewares.SpiderRetryMiddleware': 640,
        },
        'source': 'etihadarena',
        'tls_session': True,
    }

    def __init__(self, **kwargs):
        self.event_url = None
        self.conditions = []
        self.tickets = []
        self.csrf = None
        self.ssid = None
        self.product_id = None
        self.cart_id = None
        self.return_url = ''
        self.mode = 'parse'
        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):
        if not getattr(self.event, 'url', None):
            logger.error('Не передан event_url')
            return
        self.init_params()
        if self.mode == 'parse':
            yield scrapy.Request(
                url=self.event.url,
                callback=self.parse_sectors,
                dont_filter=True,
            )
        elif self.mode == BotMode.BUY:
            yield scrapy.Request(
                url=self.event.url,
                callback=self.start_new_buy_session,
                meta={'cookiejar': self.event_id},
                dont_filter=True,
            )

    def parse_sectors(self, response):
        for sector in self.get_sectors(response):
            if self.check_sector_name(sector['product_name']):
                yield scrapy.Request(
                    url=self.event.url,
                    callback=self.start_new_parse_session,
                    dont_filter=True,
                    meta={'sector': sector, 'cookiejar': sector['seat_id']},
                )

    def start_new_parse_session(self, response):
        csrf = response.xpath('//meta[@name = "_csrf"]/@content').get('')
        sector = response.meta['sector']
        ssid = re.search(r'ssId=(.*)$', sector['sector_url']).group(1)
        yield scrapy.Request(
            method='POST',
            url='https://tickets.etihadarena.ae/yba_b2c/add/tickets',
            callback=self.add_ticket,
            body=self.get_add_tickets_body(ssid, sector=sector, csrf=csrf),
            headers={
                'accept': '*/*',
                'accept-language': 'ru,en',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            dont_filter=True,
            meta={
                'cookiejar': sector['seat_id'],
                'sector': sector,
                'ssid': ssid,
                'csrf': csrf
            })

    def start_new_buy_session(self, response):
        self.csrf = response.xpath('//meta[@name = "_csrf"]/@content').get('')
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
            meta={'cookiejar': self.event_id},
        )

    def init_params(self):
        if self.tickets:
            self.ssid = self.tickets[0]['ssid']
            self.product_id = self.tickets[0]['product_id']
        if matched := re.search(r'performanceAk=(.*)&return', self.event.url):
            self.event_id = matched.group(1)
        if matched := re.search(r'return=(.*)$', self.event.url):
            self.return_url = matched.group(1)

    def get_add_tickets_body(self, ssid, quant=1, sector=None, csrf=None):
        _add = 'addToCartRequests[0]'
        body = {
            '_csrf': csrf or self.csrf,
            'availabilityPair': '0',
            f'{_add}.performanceAks': self.event_id,
            f'{_add}.product': self.product_id or sector.get('seat_id'),
            f'{_add}.skipUpsell': 'true',
            f'{_add}.quantity': quant,
        }
        if ssid:
            body.update({
                f'{_add}.spaceStructureAk': ssid,
                'spaceStructureAk': ssid,
            })
        return urlencode(body)

    def add_ticket(self, response):
        cart_id = Selector(text=response.text).xpath(
            '//div[contains(@class, "prodrow")]/@id'
        ).get('')
        if self.mode == BotMode.PARSE:
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
            seat_id = hold_seat_ids.pop(0).get('id')
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

    def checkout_request(self, response):
        response.meta['tls_params'] = {'allow_redirects': False}
        return scrapy.Request(
            url=('https://tickets.etihadarena.ae/yba_b2c'
                 f'/checkout.html?ssId={self.ssid}'),
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

    def checkout(self, response):
        if url := self.get_payment_url(response):
            yield {'payment_url': url}
        else:
            logger.error('Не удалось создать заказ, чистим корзину')
            for seat in self.tickets:
                if seat_id := seat.get('seat_id'):
                    yield self.release_request(
                        self.ssid, seat_id, response.meta
                    )

    def hold_tickets(self, response):
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
            seat_id = holds.pop(0).get('id')
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

    def get_hold_body(self):
        body = f'_csrf={self.csrf}&spaceStructureAK={self.ssid}'
        for ind, ticket in enumerate(self.tickets):
            body += f'&seats%5B{ind}%5D={ticket["seat_id"]}'
        body += f'&cartItemIndex={self.cart_id}&show3DView=true'
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
            return seats, hold_seat_ids
        except json.decoder.JSONDecodeError:
            return [], []

    def parse_sector(self, response):  # noqa: C901
        if ssid := re.search(r'ssId=(.*)$', response.url):
            ssid = ssid.group(1)
        else:
            logger.error('Не найден ssId')
            return
        if response.url != response.meta['sector']['sector_url']:
            sector = response.meta['sector']['product_name']
            price = response.meta['sector']['price']
            yield Ticket({
                'stand': True,
                'ssid': ssid,
                'product_id': response.meta['sector']['seat_id'],
                'product_name': response.meta['sector']['product_name'],
                'sector_url': response.url,
                'sector': sector,
                'price': price,
                'count': response.meta['sector']['count']
            })
            return
        try:
            tickets, hold_seat_ids = self.get_seats(response)
        except Exception:  # pylint: disable=W0703
            yield self._retry(response.request, ValueError, self)
            return
        yield from self.extract_seats(tickets, response, ssid)

        for seat in hold_seat_ids:
            if seat_id := seat.get('id'):
                yield self.release_request(ssid, seat_id, response.meta)

    @staticmethod
    def extract_seats(tickets, response, ssid):
        for ticket in tickets:
            sector = response.meta['sector']['product_name']
            row = ticket['rowLabel']
            seat = ticket['colLabel']
            price = response.meta['sector']['price']

            yield Ticket({
                'stand': False,
                'ssid': ssid,
                'product_id': response.meta['sector']['seat_id'],
                'product_name': response.meta['sector']['product_name'],
                'sector_url': response.url,
                'sector': sector,
                'row': row,
                'seat': seat,
                'price': price,
                'seat_id': ticket['id'],
            })

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

    @staticmethod
    def get_sectors(response):
        sectors = []
        raw_sectors = response.xpath('//div[@data-sector]').getall()
        for sector in raw_sectors:
            selector = Selector(text=sector)
            sector_url = selector.xpath('//a[@data-seatid]/@href').get('')
            sector_options = selector.xpath('//div[@class="products"]').getall()
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
                if all([sector_url, seat_id, price]):
                    sectors.append({
                        'price': price,
                        'seat_id': seat_id,
                        'product_name': product_name,
                        'sector_url': 'https://tickets.'
                                      'etihadarena.ae' + sector_url,
                        'count': int(count) if count else 15
                    })
        return sectors

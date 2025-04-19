import json
import logging
import re
from json import JSONDecodeError
from typing import Iterable
from urllib.parse import parse_qs
import scrapy
from scrapy import Request
from scrapy.selector import Selector

from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.spiders.ticketon')

class TicketonBot(scrapy.Spider):

    name = 'ticketon'
    custom_settings = {
        'ITEM_PIPELINES': {
            # 'spiders.pipelines.InitParamsAndCheckActuality': 100,
            # 'spiders.pipelines.CheckTicket': 110,
            # 'spiders.etihadarena.pipelines.BotEtihadArenaParseMode': 125,
            # 'spiders.pipelines.BotBuyMode': 200,
        },
        'DOWNLOADER_MIDDLEWARES': {
            # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            # 'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 102,
            # 'spiders.middlewares.CookiesAndHeadersMiddleware': 103,
            # 'spiders.middlewares.ProxyMiddleware': 105,
            # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
            # 'spiders.middlewares.TlsSessionMiddleware': 115,
            'spiders.middlewares.RandomUserAgentMiddleware': 600,
            # 'spiders.middlewares.TooManyRequestsRetryMiddleware': 630,
            # 'spiders.middlewares.SpiderRetryMiddleware': 640,
        },
        'source': 'ticketon',
        # 'tls_session': True,
    }

    def __init__(self, **kwargs):
        self.token = '70acf28834751512ffd403c8aac6b04274f083fd'
        self.show_id = None
        self.event = None
        self.event_url = 'https://static.ticketon.kz/w/1/placePage?++applePayType=newPage++&parentLocationHref=https://ticketon.kz/astana/event/jose-carreras-v-astane?item_list_name=%25D0%259F%25D0%25BE%25D0%25BF%25D1%2583%25D0%25BB%25D1%258F%25D1%2580%25D0%25BD%25D0%25BE%25D0%25B5%26item_list_id=popular%26index=3++&parentOrigin=https://ticketon.kz++&timestamp=1744664728796&show_id=5544411&item_list_name=%D0%9F%D0%BE%D0%BF%D1%83%D0%BB%D1%8F%D1%80%D0%BD%D0%BE%D0%B5&item_list_id=popular&index=3'

        self.tickets = {}
        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):
        if show_id := parse_qs(self.event_url).get('show_id'):
            self.show_id = show_id[0]
            yield scrapy.Request(
                url=f'https://api.ticketon.kz/show?id={self.show_id}&token={self.token}',
                callback=self.parse_seats
            )

    @staticmethod
    def get_seat_query(seat, seat_data, sector):
        return f'&seats[]={sector}-{seat_data["row"]}-{seat_data["seat"]}-0-1'

    def parse_seats(self, response):
        try:
            data = json.loads(response.text)
        except JSONDecodeError as err:
            logger.error('Ошибка декодирования: %s', err)
        for _id, data in data.get('hall', []).get('levels', {}).items():
            if data.get('seats_free', 0) > 0:
                sector_id = _id
                full_name = data.get('name', '')
                short_name = data.get('svg_text', '')
                prices = data.get('types')
                for seat in data.get('seats', []):
                    if seat['sale']:
                        self.tickets.setdefault(short_name, {
                            'sector_id': sector_id,
                            'full_name': full_name,
                            'seats': {}
                        })
                        self.tickets[short_name]['seats'].update({
                            seat['id']: {
                                'seat': seat['num'],
                                'row': seat['row'],
                                'price': prices[seat['type']].get('sum')
                            }
                        })

        # закупка
        query = []
        for sec, data in self.tickets.items():
            if sec == 'Партер':
                for seat, seat_data in data['seats'].items():
                    if len(query)<4:
                        query.append(self.get_seat_query(seat, seat_data, data["sector_id"]))

        query = ''.join(query)
        yield scrapy.Request(
            url=f'https://api.ticketon.kz/sale_create?token={self.token}&show={self.show_id}{query}&lang=ru&gaClientId=',
            callback=self.get_order
        )

    def get_order(self, response):
        try:
            data = json.loads(response.text)
        except JSONDecodeError as err:
            logger.error('Ошибка декодирования: %s', err)
        security_token = data.get('sale_secury_token')
        sale = data.get('sale')
        yield scrapy.Request(
            url=f'https://api.ticketon.kz/sale_confirm?token={self.token}&sale_security_token={security_token}&sale={sale}&email=porise5601@insfou.com&phone=79995621015&type=paybox',
            callback=self.get_pay_url
        )

    def get_pay_url(self, response):
        print(response.text)
        url = response.xpath('//a[@id="div_link"]/@href').get()
        logger.info('Ссылка на оплату: %s', url)
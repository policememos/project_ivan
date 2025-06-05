import logging
import random
import re

import scrapy
import yaml
from spiders.enum import BotMode
# from spiders.ticket import Ticket

# from datetime import datetime

logger = logging.getLogger('scrapy.spiders.base')


class BotSpider(scrapy.Spider):  # pylint: disable=R0902

    name = 'bot'
    custom_settings = {
        'ITEM_PIPELINES': {
            'spiders.pipelines.InitParamsAndCheckActuality': 100,
            'spiders.pipelines.CheckTicket': 110,
            'spiders.pipelines.BotParseMode': 125,
            'spiders.pipelines.BotBuyMode': 150,
        },
    }

    def __init__(self, **kwargs):
        self.event = None
        self.mode = BotMode.PARSE
        self.conditions = []
        # mytickets = [{'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'L', 'seat': '17', 'seat_id': 10065580, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}, {'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'L', 'seat': '18', 'seat_id': 10065418, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}]
        # tiks = [Ticket(x) for x in mytickets]
        # tiks = []
        # self.max_tickets = 5
        # self.source = 'etihadarena'
        self.tickets = []

        self.retry_tickets = []

        self.cookies = {}
        self.account = {}
        self.switch_accounts = 3
        self.solve_captcha = False
        self.fakes = []

        self.event_id = None
        self.id_event = None
        self.base_url = None
        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def get_fake_data(self):
        if not self.fakes:
            with open('fakes.yml', 'r', encoding='utf-8') as ymlfile:
                self.fakes = yaml.safe_load(ymlfile)
        return random.choice(self.fakes)  # nosec

    @staticmethod
    def create_fake_phone():
        phone = ''.join(random.choice('0123456789') for _ in range(7))  # nosec
        return '+7912' + phone

    def check_sector_name(self, sector):
        sector = re.sub(r'\s+', ' ', str(sector)).strip()
        for cond in self.conditions:
            if cond.check_sector(sector):
                return True
        logger.info('Пропускаем сектор: %s', sector)
        return False

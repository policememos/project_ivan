import logging
import random
import re

import scrapy
import yaml

from datetime import datetime

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
        self.mode = 'parse'
        self.conditions = []
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

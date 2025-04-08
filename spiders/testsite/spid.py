import scrapy
import logging
from spiders.mongo_utils import save_data


logger = logging.getLogger('scrapy.spiders.testsite.spid')

class SpidSpider(scrapy.Spider):

    name = 'spid'

    def __init__(self, **kwargs):
        self.event_url = None
        self.tariffs = {}
        super().__init__(**kwargs)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):
        yield scrapy.Request(
            url= self.event_url,
            callback=self.parse_it
        )

    def parse_it(self, response):
        save_data()
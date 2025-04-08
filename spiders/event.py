import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict
import yaml
from spiders.mongo_utils import get_client
from spiders.ticket import Ticket

logger = logging.getLogger('scrapy.spiders.event')


class Event:  # pylint: disable=R0902

    def __init__(self, event: dict, spider):
        event.update(event.pop('bot_params', {}))
        self.id_event = event.pop('_id')
        self.source = event.pop('source', None)
        self.url = event.pop('url', None)
        self.when = event.pop('when', None)
        self.name = event.pop('name', None)
        max_tickets = event.pop('max_tickets', 4)
        self.max_tickets = getattr(spider, 'max_tickets', None) or max_tickets
        self.min_tickets = event.pop('min_tickets', 1)
        self.conditions = event.pop('conditions', [{}, ]) or [{}, ]
        self.count: Optional[int] = event.pop('count', None)

        for key, value in event.items():
            setattr(self, key, value)


    def __str__(self):
        return f'{self.id_event} - "{self.name}" - {self.url}'

    def __check_exists_count(self) -> bool:
        return any('count' in cond for cond in self.conditions)

import logging
from spiders.event import Event
from spiders.condition import Condition
from spiders.mongo_utils import get_client

logger = logging.getLogger('scrapy.pipelines')

class MongoDBPipeline:
    def process_item(self, item, spider):
        return item


class InitParamsAndCheckActuality:

    def open_spider(self, spider):
        logger.info('pipline InitParams начал %s',  spider.name)
        if event := self.get_event_from_db(spider.name):
            logger.info('event_url: %s', event['url'])
            setattr(spider, 'event', Event(event, spider))
            max_tickets = getattr(spider.event, 'max_tickets', 4)
            conditions = [
                Condition(cond, max_tickets)
                for cond in spider.event.conditions
                if cond.get('count') != 0
            ]
            if spider.event.count != 0 and conditions:
                setattr(spider, 'conditions', conditions)
                return
        else:
            logger.info('Мероприятие %s больше не в работе', spider.id_event)




    @staticmethod
    def check_conditions(conditions, ticket):
        for cond in conditions:
            if cond.check(ticket):
                return True
        return False



    def get_event_from_db(self, spid_name):
        if client := get_client():
            try:
                bots_db = client['bots']
                return bots_db['events'].find_one({
                    'bot_status': 2,
                    'source': spid_name
                })
            except Exception:  # pylint: disable=W0703
                logger.error('Ошибка в get_event_from_db:',
                             exc_info=True)
            finally:
                client.close()
        return None

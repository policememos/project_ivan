from datetime import datetime, timedelta
# from typing import Optional

from crawler.celery import app
from crawler.conf import settings
from spiders.runners import run_bot
from spiders.mongo_utils import (get_bot_events,  dump_event_changes)

# from spiders.account_utils import get_accounts
from spiders.enum import BotMode
import logging
# from spiders.ticket import Ticket


SCHED = settings.SCHEDULE_SETTINGS  # type: ignore
logger = logging.getLogger('spiders.tasks')


@app.task(ignore_result=True, expires=SCHED['bots']['expires'])
def run_all_bots_main(default_delay: int = 60) -> None:
    events: list = get_bot_events()
    logger.info(f'Нашел ивенты {[x["name"] for x in events]}')
    for event in events:
        last_parse = event.get('last_parse')
        parse_delay = timedelta(seconds=event.get('parse_delay', default_delay))
        if not last_parse or last_parse + parse_delay <= datetime.now():
            dump_event_changes(event['_id'], last_parse=datetime.now())
            logger.info(f'Отправляю на запуск ИВЕНТ {event["name"]}')
            run_bot_quick.delay(event['_id'], event['source'])


@app.task(ignore_result=True, expires=SCHED['bots']['expires'])
def run_bot_quick(id_event, source) -> None:
    run_bot(id_event=id_event, source=source)

def run_all_bots_main_test(default_delay: int = 60) -> None:
    events: list = get_bot_events()
    for event in events:
        last_parse = event.get('last_parse')
        parse_delay = timedelta(seconds=event.get('parse_delay', default_delay))
        if not last_parse or last_parse + parse_delay <= datetime.now():
            dump_event_changes(event['_id'], last_parse=datetime.now())
        run_bot(event['_id'], event['source'])


@app.task(ignore_result=True, expires=SCHED['bots']['expires'])
def run_bot_quick_from_spider(id_event: str, source: str, **kwargs) -> None:
    # check_solve_captcha(source, kwargs)
    # mytickets = [{'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'H', 'seat': '18', 'seat_id': 10064833, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}, {'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'H', 'seat': '19', 'seat_id': 10064932, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}, {'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'H', 'seat': '20', 'seat_id': 10064887, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}, {'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'H', 'seat': '21', 'seat_id': 10064816, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}, {'price': '350.00', 'product_id': 'YBA.EVN1.MCC66929', 'product_name': '108 LOWER TIER GOLD', 'row': 'L', 'seat': '15', 'seat_id': 10065370, 'sector': '108 LOWER TIER GOLD', 'sector_url': 'https://tickets.etihadarena.ae/yba_b2c/seats.html?ssId=YBA.EVN284.PRF1.SPS51', 'ssid': 'YBA.EVN284.PRF1.SPS51'}]
    # tiks = [Ticket(x) for x in mytickets]
    run_bot(
        id_event=id_event,
        source=source,
        mode=BotMode.BUY,
        **kwargs,
        # max_tickets=5,
        # tickets = tiks
    )

# @app.task(ignore_result=True, expires=SCHED['old_orders']['expires'])
# def run_delete_old_orders(days=30):
#     remove_old_orders(days)

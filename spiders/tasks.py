from datetime import datetime, timedelta
from typing import Optional

from crawler.celery import app
from crawler.conf import settings
from spiders.runners import run_bot
from spiders.mongo_utils import (get_bot_events, get_wait_events,
                                 get_accounter_sources, dump_event_changes,
                                 remove_old_orders, remove_old_captchas)

from spiders.account_utils import get_accounts

SCHED = settings.SCHEDULE_SETTINGS  # type: ignore

def run_all_bots_main(default_delay: int = 60) -> None:
    events: list = get_bot_events()
    for event in events:
        last_parse = event.get('last_parse')
        parse_delay = timedelta(seconds=event.get('parse_delay', default_delay))
        if not last_parse or last_parse + parse_delay <= datetime.now():
            dump_event_changes(event['_id'], last_parse=datetime.now())
        run_bot_quick(event['_id'], event['source'])




def run_bot_quick(id_event, source) -> None:
    run_bot(id_event=id_event, source=source)
# #
#
#
# def run_bot_quick_from_spider(id_event: str, source: str, **kwargs) -> None:
#     check_solve_captcha(source, kwargs)
#     run_bot(
#         id_event=id_event,
#         source=source,
#         mode=BotMode.BUY,
#         **kwargs,
#     )
#
#
# @app.task(ignore_result=True, expires=SCHED['bots']['expires'])
# def run_bot_long_from_spider(id_event: str, source: str, **kwargs) -> None:
#     check_solve_captcha(source, kwargs)
#     run_bot(
#         id_event=id_event,
#         source=source,
#         mode=BotMode.BUY,
#         **kwargs,
#     )
#
#
# @app.task(ignore_result=True, expires=SCHED['all_accounter']['expires'])
# def run_all_accounter_main(used: Optional[bool] = None) -> None:
#     sources: dict = get_accounter_sources(used=used)
#     for source, interval in sources.items():
#         for acc in get_accounts(source=source, used=used, interval=interval):
#             run_one_accounter.delay(source, acc)
#
#
# @app.task(ignore_result=True, expires=SCHED['all_accounter']['expires'])
# def run_one_accounter(source, account) -> None:
#     run_accounter(source, account)
#
#
# @app.task(ignore_result=True, expires=SCHED['bots']['expires'])
# def run_solver_captcha(source, kwargs) -> None:
#     captcha_version = kwargs.get('captcha_version') or 'v2'
#     sitekey = kwargs.get('sitekey')
#     solver_captcha(source, captcha_version, sitekey)
#
#
# @app.task(ignore_result=True, expires=SCHED['wait']['expires'])
# def run_all_waiters() -> None:
#     events = get_wait_events()
#     for event in events:
#         run_waiter_main.delay(event)
#
#
# @app.task(ignore_result=True, expires=SCHED['wait']['expires'])
# def run_waiter_main(event) -> None:
#     run_waiter(event)
#
#
# @app.task(ignore_result=True, expires=SCHED['old_orders']['expires'])
# def run_delete_old_orders(days=30):
#     remove_old_orders(days)
#
#
# @app.task(ignore_result=True, expires=SCHED['old_captchas']['expires'])
# def run_delete_old_captchas(minutes=5):
#     remove_old_captchas(minutes)

run_all_bots_main()
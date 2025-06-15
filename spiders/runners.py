import logging
from typing import Optional
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

from crawler.conf import settings
from spiders.enum import BotMode


logger = logging.getLogger('scrapy.runners')


# def solver_captcha(spider, captcha_version, sitekey):
#     logger.info('Гадаем капчу для %s', spider)
#     if solver := settings.SPIDERS[spider].get('solver'):
#         solver(captcha_version, sitekey)


def run_bot(id_event: str,
            source: str,
            mode: int = BotMode.PARSE,
            **kwargs) -> None:
    runner = CrawlerRunner(get_project_settings())
    runner.crawl(
        settings.SPIDERS[source]['bot'],  # type: ignore
        id_event=id_event,
        mode=mode,
        **kwargs,
    )
    deffered = runner.join()
    deffered.addBoth(lambda _: reactor.stop())
    reactor.run()


# def run_queuer(source: str,
#                config: dict,
#                mode: int = QueueMode.CREATE,
#                start_sale: Optional[bool] = None,
#                tokens: Optional[list] = None) -> None:
#     runner = CrawlerRunner(get_project_settings())
#     runner.crawl(
#         settings.SPIDERS[source]['queuer'],
#         config=config,
#         mode=mode,
#         start_sale=start_sale,
#         tokens=tokens,
#     )
#     deffered = runner.join()
#     deffered.addBoth(lambda _: reactor.stop())
#     reactor.run()
#
#
# def run_accounter(source: str, account: dict) -> None:
#     runner = CrawlerRunner(get_project_settings())
#     runner.crawl(settings.SPIDERS[source]['accounter'], account=account)
#     deffered = runner.join()
#     deffered.addBoth(lambda _: reactor.stop())
#     reactor.run()
#
#
# def run_waiter(event: dict) -> None:
#     runner = CrawlerRunner(get_project_settings())
#     runner.crawl(
#         settings.SPIDERS[event['source']]['waiter'],  # type: ignore
#         main_event=event,
#         wait_params=event['wait_params'],
#     )
#     deffered = runner.join()
#     deffered.addBoth(lambda _: reactor.stop())
#     reactor.run()

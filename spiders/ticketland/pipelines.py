from spiders.enum import BotMode
from spiders.pipelines import BotParseMode

from spiders.ticketland.helpers import get_free_account


class BotTicketlandParseMode(BotParseMode):

    def init_stand_buy_tasks(self, spider):
        if self.stand_tickets:
            for ticket in self.stand_tickets:
                ticket = self.get_stand_ticket(spider, ticket)
                count_tasks = ticket['count'] // spider.event.max_tickets
                max_tickets = min([ticket['count'], spider.event.max_tickets])
                for _ in range(count_tasks or 1):
                    self.count_orders += 1
                    self.count_tickets += max_tickets
                    self.run_bot_delay(
                        id_event=spider.id_event,
                        source=spider.custom_settings['source'],
                        tickets=[ticket] * max_tickets,
                        max_tickets=max_tickets,
                        solve_captcha=spider.solve_captcha,
                    )


class AccountPipeline:

    @staticmethod
    def open_spider(spider):
        if spider.mode == BotMode.BUY:
            spider.account = get_free_account()

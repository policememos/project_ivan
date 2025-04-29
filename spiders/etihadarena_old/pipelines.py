from spiders.pipelines import BotParseMode
import logging

logger = logging.getLogger('scrapy.spiders.etihadarena.pipelines')

class BotEtihadArenaParseMode(BotParseMode):

    def init_tickets_buy_tasks(self, spider):
        logger.info(f'3. Pipeline BotEtihadArenaParseMode начал init_tickets_buy_tasks, {spider.mode=}')
        if not self.tickets:
            return
        sectors = self.sort_by_sectors(self.tickets)
        for s_tickets in sectors.values():
            for tickets in self.get_parts_tickets(s_tickets, spider):
                logger.info(f'3. Pipeline BotEtihadArenaParseMode отправляю на откупку билеты {tickets=}')
                self.count_orders += 1
                self.count_tickets += len(tickets)
                self.run_bot_delay(
                    id_event=spider.id_event,
                    source=spider.custom_settings['source'],
                    tickets=tickets,
                    max_tickets=spider.event.max_tickets,
                )


    @staticmethod
    def sort_by_sectors(tickets):
        sectors = {}
        for ticket in tickets:
            s_name = ticket['product_id']
            sectors[s_name] = sectors.get(s_name, []) + [ticket]
        return sectors

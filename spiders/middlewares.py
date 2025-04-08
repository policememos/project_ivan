import logging

logger = logging.getLogger('scrapy.middlewares')

class UserAgentMiddleware:
    def __init__(self, crawler):
        self.user_agent = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        if not self.user_agent:
            source = spider.custom_settings['source']
            default_user_agent = ('Mozilla/5.0 (X11; Linux x86_64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/124.0.0.0 Safari/537.36')
            # with open(f'spiders/{source}/config.yml', 'r',
            #           encoding='utf-8') as ymlfile:
            #     cfg = yaml.safe_load(ymlfile)
            # self.user_agent = cfg.get('user_agent') or default_user_agent
            self.user_agent = default_user_agent
            logger.debug(
                'Для всех запросов будет использоваться user-agent: %s',
                self.user_agent)
        logger.debug('Выбранный юзерагент: %s', self.user_agent)
        request.headers.setdefault('User-Agent', self.user_agent)
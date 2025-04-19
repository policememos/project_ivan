import time
import logging
from datetime import datetime
from curl_cffi import requests as tls_requests

import yaml
from scrapy import signals
from scrapy.http import TextResponse, Headers
from scrapy.utils.response import response_status_message
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from fake_useragent import UserAgent

from spiders.mongo_utils import get_proxy_settings

logger = logging.getLogger('scrapy.middlewares')


class ProxyMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):  # pylint: disable=W0613
        self.sources = get_proxy_settings()
        self.proxy_session = None
        with open('config.yml', 'r', encoding='utf-8') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        self.main_proxy = cfg['main_proxy']

    def _source_enabled(self, spider):
        if not spider.custom_settings.get('proxy_rotation', True):
            if not self.proxy_session:
                self.proxy_session = datetime.now().timestamp()
        return spider.custom_settings['source'] in self.sources

    @staticmethod
    def _get_source(spider):
        return spider.custom_settings['source']

    def process_request(self, request, spider):
        if self._source_enabled(spider):
            proxy = self.main_proxy.replace('http://', '')
            source = self._get_source(spider)
            ses_str = f'proxy_session={self.proxy_session}'
            session = ses_str if self.proxy_session else ''
            proxy = f'http://source=bots_{source}:{session}@{proxy}'
            if self.proxy_session:
                setattr(spider, 'proxy_ip', proxy)
            logger.info('Будет использоваться Proxy: %s', proxy)
            request.meta['proxy'] = proxy


class RandomUserAgentMiddleware:

    def __init__(self, crawler):
        fallback = crawler.settings.get('FAKEUSERAGENT_FALLBACK', None)
        self.ua_type = 'random'
        self.user_agent = UserAgent(fallback=fallback)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        def get_ua():
            return getattr(self.user_agent, self.ua_type)

        user_agent = get_ua()
        request.headers.setdefault('User-Agent', user_agent)
        logger.debug('User-Agent: %s %s', str(user_agent), str(request))


class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        dont_retry = request.meta.get('dont_retry', False)
        if response.status not in self.retry_http_codes or dont_retry:
            return response
        if response.status in [429, 500, 502, 503]:
            self.crawler.engine.pause()
            time.sleep(2)
            self.crawler.engine.unpause()
        reason = response_status_message(response.status)
        return self._retry(request, reason, spider) or response


class SpiderRetryMiddleware(RetryMiddleware):

    def process_response(self, request, response, spider):
        if not hasattr(spider, '_retry'):
            spider._retry = self._retry
        return super(SpiderRetryMiddleware, self).process_response(
            request, response, spider)


class UserAgentMiddleware:

    def __init__(self, crawler):  # pylint: disable=W0613
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
            with open(f'spiders/{source}/config.yml', 'r',
                      encoding='utf-8') as ymlfile:
                cfg = yaml.safe_load(ymlfile)
            self.user_agent = cfg.get('user_agent') or default_user_agent
            logger.debug(
                'Для всех запросов будет использоваться user-agent: %s',
                self.user_agent)
        request.headers.setdefault('User-Agent', self.user_agent)


class CookiesAndHeadersMiddleware:

    def __init__(self, crawler):  # pylint: disable=W0613
        self.user_agent = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    @staticmethod
    def get_event_cookies(spider):
        if event := getattr(spider, 'event', None):
            return getattr(event, 'cookies', None)
        return None

    @staticmethod
    def get_event_headers(spider):
        if event := getattr(spider, 'event', None):
            return getattr(event, 'headers', None)
        return None

    @staticmethod
    def prepare_request_cookies(request):
        req_cookies = request.headers.get(b'Cookie') or b''
        return req_cookies.decode('utf-8').split(';')

    def add_cookies_to_headers(self, cookies, request):
        req_cookies = self.prepare_request_cookies(request)
        cookies = [f'{k}={v}' for k, v in cookies.items()]
        request.headers[b'Cookie'] = '; '.join([
            c.strip() for c in req_cookies + cookies if c.strip()
        ])

    def process_request(self, request, spider):
        if headers := self.get_event_headers(spider):
            request.headers.update(headers)
        if cookies := self.get_event_cookies(spider):
            self.add_cookies_to_headers(cookies, request)


class TlsSessionMiddleware:
    """
    Включается параметром tls_session: True в custom_settings.

    Имеет возможность игнорировать запросы (не использовать на них сессию)
    параметром without_tls: True в meta запроса.

    По умолчанию введет одну сессию, если нужно пересоздавать сессию
    на каждый запрос (полезно, что б не смешивались куки)
    можно указать параметр recreate_tls_session: True
    в custom_settings (будет использоваться на всех запросах),
    или в meta отдельного запроса.

    Так же можно передать параметры напрямую в функцию tls,
    такие, как timeout (по умолчанию используется общий таймаут),
    verify и прочее,
    для этого нужно указать словарь tls_params
    в custom_settings (для всех запросов одинаковые параметры)
    или в meta запроса (параметра для одного запроса)
    """

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler)
        crawler.signals.connect(
            middleware.spider_closed,
            signal=signals.spider_closed
        )
        crawler.signals.connect(
            middleware.spider_opened,
            signal=signals.spider_opened
        )
        return middleware

    def __init__(self, crawler):
        self.tls_session = None
        self.timeout = crawler.settings.get('DOWNLOAD_TIMEOUT') or 30

    @staticmethod
    def _enable_tls(request, spider):
        return (spider.custom_settings.get('tls_session')
                and not request.meta.get('without_tls')) #проверяй, есть ли without_tls

    def _is_recreate_tls(self, request, spider):
        return (spider.custom_settings.get('recreate_tls_session')
                or request.meta.get('recreate_tls_session') or
                not self.tls_session or request.meta.get('cookiejar')) #проверяй, есть ли cookiejar

    def _choice_request_method(self, request):  # pylint: disable=R0911
        match request.method:
            case "GET":
                return self.tls_session.get
            case "POST":
                return self.tls_session.post
            case "PUT":
                return self.tls_session.put
            case "DELETE":
                return self.tls_session.delete
            case "OPTIONS":
                return self.tls_session.options
            case "PATCH":
                return self.tls_session.patch
            case "HEAD":
                return self.tls_session.head
            case _:
                raise ValueError(
                    f'Не поддерживаемый метод запроса '
                    f'({request.method}) в tls session'
                )

    @staticmethod
    def _get_data(request):
        if isinstance(request.body, dict):
            return '&'.join(f'{k}={v}' for k, v in request.body.items())

        if isinstance(request.body, bytes):
            return request.body.decode('utf-8')

        return request.body

    def _get_headers(self, request):
        headers = {
            key.decode('utf-8'): ';'.join(v.decode('utf-8') for v in value)
            for key, value in request.headers.items()
        }
        if cookies := self.prepare_cookies(request):
            headers['cookie'] = (headers.get('cookie') or '') + cookies
        return headers

    def process_request(self, request, spider):  # noqa: C901
        if not self._enable_tls(request, spider):
            return None
        logger.debug('для ссылки %s будет использоваться tls', request.url)
        if self._is_recreate_tls(request, spider):
            logger.debug('для ссылки %s будет пересоздана сессия', request.url)
            if self.tls_session:
                self.tls_session.close()
            self.tls_session = tls_requests.Session(impersonate="chrome124")

        if proxy := request.meta.get('proxy'):
            self.tls_session.proxies = {
                'http': proxy,
                'https': proxy,
            }

        r_func = self._choice_request_method(request)
        tls_params = {
            'headers': self._get_headers(request),
            'data': self._get_data(request),
            'timeout': self.timeout
        }
        tls_params.update(spider.custom_settings.get('tls_params') or {}) # чекай есть ли параметры
        tls_params.update(request.meta.get('tls_params') or {})# чекай есть ли параметры
        try:
            response = r_func(  # pylint: disable=E1102
                url=request.url, **tls_params
            )
        except Exception as c_error:  # pylint: disable=W0703
            logger.error('url: %s ERROR: %s', request.url, c_error)
            return None

        response.headers['Content-Encoding'] = 'utf-8'
        headers = Headers(response.headers)
        headers.setlist(
            'Set-Cookie', [f'{k}={v};' for k, v in response.cookies.items()]
        )
        return TextResponse(
            url=response.url,
            status=response.status_code,
            headers=headers,
            body=response.content,
            request=request,
            encoding='utf-8',
        )

    @staticmethod
    def prepare_cookies(http_obj):
        return '; '.join(f'{k}={v}' for k, v in http_obj.cookies.items())

    def spider_opened(self, spider):
        if spider.custom_settings.get('tls_session'):
            self.tls_session = tls_requests.Session(impersonate="chrome124")

    def spider_closed(self, spider):
        if self.tls_session:
            self.tls_session.close()

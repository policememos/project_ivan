import yaml
# Scrapy settings for scrapybot project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# Obey robots.txt rules
with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

ROBOTSTXT_OBEY = False
TELNETCONSOLE_PORT = None
CONCURRENT_REQUESTS = 20
FAKEUSERAGENT_FALLBACK = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                         'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                         'Chrome/74.0.3729.169 Safari/537.36'

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'spiders.middlewares.CookiesAndHeadersMiddleware': 103,
    'spiders.middlewares.ProxyMiddleware': 105,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'spiders.middlewares.TlsSessionMiddleware': 115,
    'spiders.middlewares.RandomUserAgentMiddleware': 600,
    'spiders.middlewares.TooManyRequestsRetryMiddleware': 630,
    'spiders.middlewares.SpiderRetryMiddleware': 640,
}

RETRY_TIMES = cfg['spiders']['retry_times']

LOG_LEVEL = cfg['spiders']['loglevel']
LOG_ENABLED = cfg['spiders']['log_enabled']

DOWNLOAD_DELAY = cfg['spiders']['download_delay']
DOWNLOAD_TIMEOUT = cfg['spiders']['download_timeout']

# REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'

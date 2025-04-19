import logging
import re
from curl_cffi import requests as tls_requests
import yaml
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# import undetected_chromedriver as uc

from spiders.mongo_utils import get_proxy_settings

logger = logging.getLogger('spiders.spider_helpers')

with open('config.yml', 'r', encoding='utf-8') as cfgfile:
    cfg = yaml.safe_load(cfgfile)


# def get_driver(source, load_strategy='normal',
#                only_undetected=False, debugger_address=None):
#     if debugger_address:
#         options = Options()
#         options.add_experimental_option("debuggerAddress", debugger_address)
#         return webdriver.Chrome(options=options)
#     default_agent = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
#                      ' (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
#     if (selenium_server := cfg.get('selenium_server')) and not only_undetected:
#         options = Options()
#     else:
#         options = uc.ChromeOptions()
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-setuid-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument('--disable-browser-side-navigation')
#     options.add_argument('--no-default-browser-check')
#     options.add_argument('--disable-gpu')
#     options.add_argument('--disable-extensions')
#     options.add_argument('--disable-application-cache')
#     options.add_argument('--headless')
#
#     user_agent = cfg.get('chrome_user_agent') or default_agent
#     options.add_argument(f'--user-agent={user_agent}')
#
#     options.page_load_strategy = load_strategy
#
#     enabled = get_proxy_settings()
#     if source in enabled:
#         proxy = cfg['main_proxy']
#         proxy = re.sub('//.*@', '//', proxy)
#         options.add_argument(f'--proxy-server={proxy}')
#         logger.info('подключен прокси для undetected: %s', proxy)
#     if selenium_server and not only_undetected:
#         return webdriver.Remote(
#             options=options, command_executor=f'{selenium_server}/wd/hub'
#         )
#     version = cfg.get('undetected_version', 124)
#     return uc.Chrome(version_main=version, options=options)


def get_tls_session(source, proxy=None):
    session = tls_requests.Session(impersonate="chrome124")
    enabled = get_proxy_settings()
    if source in enabled:
        if not proxy:
            proxy = cfg['main_proxy']
        session.proxies = {
            'http': proxy,
            'https': proxy,
        }
        logger.info('подключен прокси для tls: %s', proxy)
    return session

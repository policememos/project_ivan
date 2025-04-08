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
ROBOTSTXT_OBEY = False

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

LOG_LEVEL = cfg['spiders']['loglevel']
LOG_ENABLED = cfg['spiders']['log_enabled']

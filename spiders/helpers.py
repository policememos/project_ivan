import json
import logging
import requests

from crawler.conf import settings

logger = logging.getLogger('scrapy.helpers')


def is_quick_spider(spider):
    return settings.SPIDERS[spider]['queue'] == 'quick'


def send_sms_message(phone: str, message: str) -> None:
    message_limit = 2000
    if len(message) > message_limit:
        message = f'{message[:(message_limit - 3)]}...'
    data = {
        'chat_id': phone,
        'text': message
    }
    headers = {
        'Content-Type': 'application/json'
    }
    url = f'https://api.telegram.org/bot{settings.BOT_TOKEN}/sendMessage'

    response = requests.post(url, headers=headers, json=data, timeout=30)
    if response.status_code != 200:
        logger.error('Ошибка (%s) %s отправки сообщения в телеграме: %s',
            response.status_code, response.text, data)
    else:
        logger.info('Отправил сообщение для %s, текст: %s',
            phone, message)
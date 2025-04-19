import json
import logging

# import pika

from crawler.conf import settings

logger = logging.getLogger('scrapy.helpers')
logging.getLogger('pika').setLevel(logging.INFO)


def is_quick_spider(spider):
    return settings.SPIDERS[spider]['queue'] == 'quick'


# def send_sms_message(phone: str, message: str, mode: str) -> None:
#     message_limit = settings.RABBITMQ_MESSAGE_LIMIT  # type: ignore
#     if len(message) > message_limit:
#         message = f'{message[:(message_limit-3)]}...'
#     data = json.dumps(
#         {
#             'phone': phone,
#             'message': message,
#             'mode': mode,
#             'high_priority': True,
#         },
#         ensure_ascii=False
#     )
#     connection = pika.BlockingConnection(
#         pika.ConnectionParameters(
#             settings.RABBITMQ_HOST,  # type: ignore
#             settings.RABBITMQ_PORT,  # type: ignore
#             '/',
#             pika.PlainCredentials(
#                 settings.RABBITMQ_USERNAME,  # type: ignore
#                 settings.RABBITMQ_PASSWORD  # type: ignore
#             )
#         )
#     )
#     channel = connection.channel()
#     channel.basic_publish(
#         exchange='',
#         routing_key=settings.RABBITMQ_QUEUES['alerts'],  # type: ignore
#         body=data
#     )
#     connection.close()

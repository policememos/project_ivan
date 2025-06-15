import os
import logging
from logging.handlers import RotatingFileHandler

from crawler.conf import settings
from spiders.helpers import send_sms_message
from spiders.mongo_utils import get_bot_admins


LOG_DIR = "/tmp/Logs"
LOG_FILE = os.path.join(LOG_DIR, "alert_start.log")
# Создаем директорию если не существует
os.makedirs(LOG_DIR, exist_ok=True)


logger = logging.getLogger('project_ivan.tester')
logging.basicConfig(
    level=logging.INFO,
    format="%(name)s - %(asctime)s - %(levelname)s - %(message)s",  # Формат сообщения,
    handlers=[
        RotatingFileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ])

logger.info("Демон запущен и пишет логи в %s", LOG_FILE)

admin = get_bot_admins()
for adm in admin:
    send_sms_message(adm, f'Supervisor STARTED')
    logger.info('отправил приветствие %s', adm)


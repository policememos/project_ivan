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
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(name)s - %(asctime)s - %(levelname)s - %(message)s")

# Обработчик для файла с ротацией
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10*1024*1024,  # 10 MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

# Обработчик для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Добавляем обработчики к логгеру
logger.addHandler(file_handler)
logger.addHandler(console_handler)



logger.info("Демон запущен и пишет логи в файл %s", LOG_FILE)



try:
    admin = get_bot_admins()
    for adm in admin:
        send_sms_message(adm, f'Supervisor STARTED')
        logger.info('отправил приветствие %s', adm)
except Exception:
    logger.error("Ошибка при отправке сообщений: ", exc_info=True)


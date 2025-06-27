import os
import logging
from logging.handlers import RotatingFileHandler

from crawler.conf import settings
from spiders.helpers import test_send_sms_message, print_all_loggers


LOG_DIR = "/tmp/Logs"
LOG_FILE = os.path.join(LOG_DIR, "alert_start.log")
# Создаем директорию если не существует
os.makedirs(LOG_DIR, exist_ok=True)

root_logger = logging.getLogger('project_ivan')
root_logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s: %(levelname)s/%(name)s] %(message)s')

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
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger('project_ivan.tester')
logger.info("Это логгертестер. Демон запущен и пишет логи в файл %s", LOG_FILE)

try:
    test_send_sms_message('adm', f'Supervisor STARTED')
    logger.error("Это логгертестер. err ")
except Exception:
    logger.error("Ошибка при отправке сообщений: ", exc_info=True)

print_all_loggers()

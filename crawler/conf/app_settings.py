from crawler.schedule import cfg, SCHEDULE, _QueueRouter

# REDIS SETTINGS
REDIS_HOST = cfg['redis']['host']
REDIS_PORT = cfg['redis']['port']
REDIS_PASS = cfg['redis']['password']
REDIS_DB = cfg['redis']['celery_db']

# CELERY SETTINGS
REDIS_HOSTPORTDB = f'redis://:{REDIS_PASS}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
broker_url = REDIS_HOSTPORTDB
result_backend = REDIS_HOSTPORTDB
task_routes = (_QueueRouter(), )
task_send_sent_event = True
worker_max_tasks_per_child = 1
worker_concurrency = 1
worker_prefetch_multiplier = 1
timezone = 'Europe/Moscow'

beat_scheduler = 'celery.beat.PersistentScheduler'
beat_schedule = SCHEDULE

worker_hijack_root_logger = False
LOG_LEVEL = cfg['celery']['loglevel']

BOT_TOKEN = cfg['bot_token']

# MONGO BOTS CONNECTION SETTINGS
MONGO_BOTS_URI = cfg['mongo_bots']['uri']
MONGO_BOTS_SSL = cfg['mongo_bots']['ssl']
BOTS_DB = cfg['mongo_bots']['bots_db']
TICKETS_DB = cfg['mongo_bots']['tickets_db']
SOURCES_DB = cfg['mongo_bots']['sources_db']
EVENTS_COLLECTION = 'events'
ORDERS_CL = 'orders'
SCHEDULE_SETTINGS = cfg['celery']['schedule']
MONGO_PARSERS_URI = cfg['mongo_parsers']['uri']
MONGO_PARSERS_SSL = cfg['mongo_parsers']['ssl']
PARSERS_DB = cfg['mongo_parsers']['parsers_db']

# SPIDERS CLASSES
SPIDERS: dict = {}
BOT_SPIDERS = cfg['spiders'].get('bots') or []


import yaml
from celery.schedules import crontab

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

_BOTS_QUICK = 'bots_quick'


class Tasks:
    TASK_RUN_ALL_BOTS = 'spiders.tasks.run_all_bots_main'
    TASK_RUN_BOT_QUICK = 'spiders.tasks.run_bot_quick'
    # TASK_RUN_BOT_LONG = 'spiders.tasks.run_bot_long'
    # TASK_RUN_BOT_QUICK_FROM_SPIDER = 'spiders.tasks.run_bot_quick_from_spider'
    # TASK_RUN_BOT_LONG_FROM_SPIDER = 'spiders.tasks.run_bot_long_from_spider'
    # TASK_RUN_SOLVER_CAPTCHA = 'spiders.tasks.run_solver_captcha'
    # TASK_RUN_DELETE_OLD_ORDERS = 'spiders.tasks.run_delete_old_orders'
    # TASK_RUN_DELETE_OLD_CAPTCHAS = 'spiders.tasks.run_delete_old_captchas'

    BOTS_QUICK_QUEUE = {
        task_name: _BOTS_QUICK
        for task_name in (
            TASK_RUN_BOT_QUICK,
        )
    }

    QUEUES = {
        **BOTS_QUICK_QUEUE,
    }


class _QueueRouter:

    @staticmethod
    def route_for_task(task, args=None, kwargs=None):
        if task in Tasks.QUEUES:
            return Tasks.QUEUES[task]
        return None


schedule_cfg = cfg['celery']['schedule']

SCHEDULE = {
    'run_all_bots_main': {
        'task': Tasks.TASK_RUN_ALL_BOTS,
        'schedule': schedule_cfg['bots']['seconds'],
        'options': {'expires': schedule_cfg['bots']['expires']},
        'args': (schedule_cfg['bots']['default_delay'],)
    },
}

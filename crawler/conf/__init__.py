from importlib import import_module

from crawler.conf import app_settings


class Config:
    def __init__(self, settings_obj):
        for param in dir(settings_obj):
            setattr(self, param, getattr(settings_obj, param))


settings = Config(app_settings)
for spider in settings.BOT_SPIDERS:  # noqa: C901
    settings.SPIDERS[spider] = {}
    modules = {
        'bot': 'BotSpider',
    }
    for module, classname in modules.items():
        try:
            settings.SPIDERS[spider][module] = getattr(
                import_module(f'spiders.{spider}.{module}'), classname
            )
        except (ModuleNotFoundError, AttributeError):
            pass
    try:
        settings.SPIDERS[spider]['solver'] = getattr(
            import_module(f'spiders.{spider}.helpers'),
            'solve_captcha_for_datebase'
        )
    except (ModuleNotFoundError, AttributeError):
        pass

    settings.SPIDERS[spider]['queue'] = 'quick'

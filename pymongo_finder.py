from crawler.conf import settings
from spiders.mongo_utils import get_mongo_bots_client



def get_bot_all_events():
    if client := get_mongo_bots_client():
        try:
            bots_cl = client[settings.BOTS_DB][settings.EVENTS_COLLECTION]
            return list(bots_cl.find({
                # 'bot_status': 2,
                # 'when': {'$gt': datetime.now()},
                # 'url': {'$exists': True, '$nin': [None, '']},
                # 'bot_params': {'$exists': True, '$nin': [None, {}]},
                # 'bot_params.users': {'$exists': True, '$nin': [None, []]},
            }))
        except Exception as err:  # pylint: disable=W0703
            print('Ошибка в get_bot_events:', err)
        finally:
            client.close()
    return []

res = 'Empty :('

ans = get_bot_all_events()



print(res)
print(res)
print(res)
print(res)
print(ans)

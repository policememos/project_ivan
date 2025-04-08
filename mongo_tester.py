import pymongo
import yaml

cfg = yaml.safe_load(open("config.yml"))

client = pymongo.MongoClient(
    cfg["mongo"]["uri"]
)

db = client["test"]["col_test"]
data = list(db.find({}))
print(data)
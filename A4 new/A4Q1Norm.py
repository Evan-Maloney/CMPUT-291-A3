from pymongo import MongoClient
import sys

port = int(sys.argv[1])
client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
db = client['A4dbNorm']


thing = [
    {
        "$lookup": {
            "from": "recordings",
            "localField": "songwriter_id",
            "foreignField": "songwriter_ids",
            "as": "recordings"
        }
    },
    {
        "$match": {
            "recordings.1": {"$exists": True}
        }
    },
    {
        "$project": {
            "_id": 1,
            "songwriter_id": 1,
            "name": 1,
            "num_recordings": {"$size": "$recordings"}
        }
    }
]

data = db.songwriters.aggregate(thing)

for objects in data:
    print(objects)
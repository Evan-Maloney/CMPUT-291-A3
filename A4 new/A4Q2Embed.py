from pymongo import MongoClient
import sys

port = int(sys.argv[1])
client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
db = client['A4dbNorm']

pipeline = [
    {
        "$match": {
            "recording_id": {"$regex": "^70"}
        }
    },
    {
        "$group": {
            "_id": None,
            "avg_rhythmicality": {"$avg": "$rhythmicality"}
        }
    }
]

result = list(db.recordings.aggregate(pipeline))

print(result[0])

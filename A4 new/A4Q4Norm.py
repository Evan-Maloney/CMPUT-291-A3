from pymongo import MongoClient
from datetime import datetime
import sys

port = int(sys.argv[1])
client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
db = client['A4dbNorm']

query = [
    {
        "$match": {
            "issue_date": {"$gte": datetime(1950, 1, 1)}
        }
    },
    {
        "$unwind": "$songwriter_ids"
    },
    {
        "$lookup": {
            "from": "songwriters",
            "localField": "songwriter_ids",
            "foreignField": "songwriter_id",
            "as": "songwriter"
        }
    },
    {
        "$unwind": "$songwriter"
    },
    {
        "$project": {
            "_id": 1,
            "name": "$songwriter.name",
            "r_name": "$name",
            "r_issue_date": "$issue_date"
        }
    },
    {
        "$sort": {"r_issue_date": 1}
    }
]

result = list(db.recordings.aggregate(query))

for x in result:
    print(x)
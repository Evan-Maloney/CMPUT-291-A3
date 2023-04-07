from pymongo import MongoClient
import sys

port = int(sys.argv[1])
client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
db = client['A4dbEmbed']



query = [

    {"$unwind": "$recordings"},

    {"$group": {
        "_id": "$songwriter_id",
        "total_length": {"$sum": "$recordings.length"},
        "songwriter_id": {"$first": "$songwriter_id"}
    }},

    {"$project": {
        "_id": 1,
        "total_length": 1,
        "songwriter_id": 1
    }}
]


result = list(db.SongwritersRecordings.aggregate(query))

for x in result:
    print(x)
from pymongo import MongoClient
import sys

port = int(sys.argv[1])
client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
db = client['A4dbEmbed']


query = [
    {
        "$match": {"recordings.1": {'$exists': True}}
    },
  
    {
        "$project": {
            
           "_id": 1,
           "songwriter_id": 1,
            "name": 1,
            "num_recordings": {"$size": "$recordings"},
        }
    }
]

result = list(db.SongwritersRecordings.aggregate(query))

for x in result:
    print(x)
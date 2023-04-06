from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb+srv://aaronboyd385:Pdnkn0N09sx2Zcbc@cluster0.whhfxux.mongodb.net/test")
db = client['A4dbEmbed']

start_date = datetime(1950, 1, 1)

query = [
    {
        "$match": {"recordings.issue_date": {"$gte": start_date}}
    },
    {
        "$unwind": "$recordings"
    },
    {
        "$match": {"recordings.issue_date": {"$gte": start_date}}
    },
    {
        "$project": {
            
           "_id": 1,
            "name": 1,
            "r_name": "$recordings.name",
            "issue_date": "$recordings.issue_date",
        }
    }
]

result = list(db.SongwritersRecordings.aggregate(query))

for x in result:
    print(x)
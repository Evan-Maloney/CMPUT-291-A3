from pymongo import MongoClient

# port_number = int(sys.argv[1])
port_number = 27017
client = MongoClient("mongodb+srv://aaronboyd385:Pdnkn0N09sx2Zcbc@cluster0.whhfxux.mongodb.net/test", port_number)
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
            "recordings": {"$exists": True}
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
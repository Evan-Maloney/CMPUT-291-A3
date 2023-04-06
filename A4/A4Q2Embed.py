from pymongo import MongoClient

# port_number = int(sys.argv[1])
port_number = 27017
client = MongoClient("mongodb+srv://aaronboyd385:Pdnkn0N09sx2Zcbc@cluster0.whhfxux.mongodb.net/test", port_number)
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

from pymongo import MongoClient

# port_number = int(sys.argv[1])
port_number = 27017
client = MongoClient("mongodb+srv://aaronboyd385:Pdnkn0N09sx2Zcbc@cluster0.whhfxux.mongodb.net/test", port_number)
db = client['A4dbNorm']



thing = [
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
        "$group": {
            "_id": "$songwriter.songwriter_id",
            "total_length": {"$sum": "$length"},
            "songwriter_id": {"$first": "$songwriter.songwriter_id"}
        }
    },
    {
        "$project": {
            "_id": 1,
            "total_length": 1,
            "songwriter_id": 1,
           
        }
    }
]

data = list(db.recordings.aggregate(thing))

for objects in data:
    print(objects)
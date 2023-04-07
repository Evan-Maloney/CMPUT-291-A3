from pymongo import MongoClient
import bson.json_util
import sys

def read_json_file(file_name):
    with open(file_name, 'r') as file:
        return bson.json_util.loads(file.read())
    
def main():
    port = int(sys.argv[1])
    client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
    db = client['A4dbNorm']
    songwriters_collection = db['songwriters']
    recordings_collection = db['recordings']

    data_recordings = read_json_file('recordings.json')
    recordings_collection.insert_many(data_recordings)
    
    data_songwriters = read_json_file('songwriters.json')
    songwriters_collection.insert_many(data_songwriters)

main()

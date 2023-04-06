from pymongo import MongoClient
import bson.json_util
import sys

def read_json_file(file_name):
    with open(file_name, 'r') as file:
        return bson.json_util.loads(file.read())

def main():
    port = sys.argv[1]
    client = MongoClient("mongodb+srv://ejmalone:291PasswordA4@a4.8a9trxd.mongodb.net/test", port)
    db = client['A4dbEmbed']
    songwriters_collection = db['songwriters']
    recordings_collection = db['recordings']

    # create dummy databases to read from
    data_songwriters = read_json_file('songwriters.json')
    songwriters_collection.insert_many(data_songwriters)
    
    data_recordings = read_json_file('recordings.json')
    recordings_collection.insert_many(data_recordings)

    Songwriters_Recordings_Collection = db['SongwritersRecordings']
    # go through every songwriter
    for songwriter in songwriters_collection.find():
        songwriter_id = songwriter['songwriter_id']
        songwriter_recordings = []
        # go through every recording
        for recording in recordings_collection.find({'songwriter_ids': songwriter_id}):
            recording_id = recording['recording_id']
            recording_name = recording['name']
            recording_date = recording['issue_date']
            recording_songwriters = recording['songwriter_ids']

            #put it in the songwriters recordings value
            songwriter_recordings.append({
                '_id': recording['_id'],
                'recording_id': recording_id,
                'songwriter_ids': recording_songwriters,
                'rhythmicality': recording['rhythmicality'],
                'length': recording['length'],
                'name': recording_name,
                'reputation': recording['reputation'],
                'issue_date': recording_date
            })

        # put this updated record into mongo
        Songwriters_Recordings_Collection.insert_one({
            '_id': songwriter['_id'],
            'songwriter_id': songwriter_id,
            'name': songwriter['name'],
            'fans': songwriter['fans'],
            'reputation': songwriter['reputation'],
            'recordings': songwriter_recordings
        })
    
    db['recordings'].drop()
    db['songwriters'].drop()

main()

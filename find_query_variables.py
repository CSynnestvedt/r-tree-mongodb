from datetime import datetime, time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Local connection
uri = "localhost:27019"

# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.geodatabase
collection = db.newyork


def main():
    


    try:
        results = []
        lowX, highX = 596000, 600000
        lowY, highY = 1126600, 1127570
        starttime = datetime.now()
        cursor = collection.find({"position": {"geoIntersects": {"geometry": {"type": "Polygon", "coordinates": [[
            [
                lowX,
                lowY
            ],
            [
                highX,
                lowY
            ],
            [
                highX,
                highY
            ],
            [
                lowX,
                highY
            ],
            [
                lowX,
                lowY
            ]
        ]]}}}})
        for doc in cursor:
            results.append(doc)
        endtime = datetime.now()
        difference = endtime - starttime
        print(difference.microseconds)

    except Exception as e:
        print(e)
    print(len(results))
if __name__ == "__main__":
    main()

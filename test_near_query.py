from datetime import datetime, time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Local connection
uri = "localhost:27019"

# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.geodatabase
collection = db.points

def main():
    startDate = datetime.now()
    results = []
    # maxDist = 2243 -> 9149. maxDist > 2243 -> 15660 (do not know why this happens), 1150 -> 9269, 1140 -> 9184,
    # 1120 -> 9000, 1110 -> 188, 1115 > 8950, 1114 -> 8938, 1113 -> 8931, 1112 -> 188, 30 -> 226, 29 -> 220  (er i hvert fall consistent (ikke cache):))
    # 
    try:
        cursor = collection.find({"position": {"near": {"geometry": {"type": "Point", "minDistance": 0, "maxDistance": 1113, "coordinates": [6367845.0, 256426.0]}}}})
        for doc in cursor:
            results.append(doc)


    except Exception as e:
        print(e)
    endDate = datetime.now()
    difference = endDate - startDate
    print(difference)
    print(len(results))

if __name__ == "__main__":
    main()
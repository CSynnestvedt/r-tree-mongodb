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
    

    timings = []
    count = 0

    try:
        # Paris center: x: 600511.3259323537, y: 1128382.4141358726
        # Newyork center: x: 1010956, y: 198846
        centerX, centerY = 1010956, 198846 
        size = 4200
        lowX, highX = centerX - size, centerX + size
        lowY, highY = centerY - size, centerY + size
        for i in range(5):
            results = []
            
            startDate = datetime.now()
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
            endDate = datetime.now()
            difference = endDate - startDate
            if count == 0:
                count = len(results)
            resultNumber = str(difference.microseconds/1000).replace(".", ",")
            print(resultNumber)

    except Exception as e:
        print(e)
    print(count)

if __name__ == "__main__":
    main()

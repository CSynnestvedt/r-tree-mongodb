from datetime import datetime, time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Local connection
uri = "localhost:27019"

# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.geodb
collection = db.newyork


def main():
    count = 0
    maxCount = 119005

    try:
        centerX, centerY = 997993, 216802
        size = 4650
        lowX, highX = centerX - size, centerX + size
        lowY, highY = centerY - size, centerY + size
        for i in range(5):
            results = []
            
            startDate = datetime.now()
            cursor = collection.find({"position": {"geoWithin": {"geometry": {"type": "Polygon", "coordinates": [[
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
            print(resultNumber, end=" ")
            

    except Exception as e:
        print(e)
    print()
    print(count)
    print(count/maxCount)

if __name__ == "__main__":
    main()

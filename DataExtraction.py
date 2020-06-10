import json
import sys
import requests
try:
    URL = "https://api.covid19india.org/raw_data1.json"
    req = requests.get(url=URL)
    sum = 0
    data = req.json()
    Covid_Data = data["raw_data"]
    sum += (len(data["raw_data"]))

    URL = "https://api.covid19india.org/raw_data2.json"
    req = requests.get(url=URL)
    data = req.json()
    Covid_Data +=data["raw_data"]
    sum += (len(data["raw_data"]))

    URL = "https://api.covid19india.org/raw_data3.json"

    req = requests.get(url=URL)

    data = req.json()
    Covid_Data +=data["raw_data"]
    sum += (len(data["raw_data"]))

    URL = "https://api.covid19india.org/raw_data4.json"

    req = requests.get(url=URL)

    data = req.json()
    Covid_Data += data["raw_data"]
    sum += (len(data["raw_data"]))

    URL = "https://api.covid19india.org/raw_data5.json"

    req = requests.get(url=URL)

    data = req.json()
    Covid_Data += data["raw_data"]
    sum += (len(data["raw_data"]))

    with open("CovidDataset.json", "w") as file:
        json.dump(Covid_Data, file)

    print(sum)

    URL = "https://api.covid19india.org/deaths_recoveries.json"

    req = requests.get(url=URL)

    data = req.json()

    with open("RD_Dataset.json", "w") as file:
        json.dump(data["deaths_recoveries"], file)

    print(len(data["deaths_recoveries"]))
except KeyboardInterrupt:
    sys.exit()





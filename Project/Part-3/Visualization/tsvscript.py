import csv, json
from geojson import Feature, FeatureCollection, Point
features = []
import re

with open('visualization5c.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    data = csvfile.readlines()
    for line in data[2:len(data)-2]:
        line.strip()
        row = line.split("|")
        x = row[0]
        y = row[1]
        speed =int(float(row[2].strip()))
        if speed is None or speed == "":
            continue
     
        try:
            latitude, longitude = map(float, (x, y))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': speed
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data5c.geojson", "w") as f:
    f.write('%s' % collection)
#!/usr/bin/env python3
# simulate routes to all shelters from all block group centroids

import json
import multiprocessing
import os
import pymongo
import requests
import shapely
import time

import route_distances
import otpmanager

import util

BOSTON_BOUNDING_BOX = {
    "left": -71.191155,
    "bottom": 42.227926,
    "right": -70.748802,
    "top": 42.400819999999996
}

CACHED_SHELTERS_JSON_URL = "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/Neighborhood_Emergency_Shelters/FeatureServer/0/query?f=json&where=1=1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100&resultOffset=0&resultRecordCount=1000"

CACHED_SHELTERS_JSON_PATH = "sources/shelters.json"

EVAC_ZONES = ["ZONE A", "ZONE B", "ZONE C"]

MONGO_DB = "local"

MONGO_COLLECTION = "shelter_routes"

OTP_PATH = "%s/otp-1.1.0-shaded.jar" % os.path.expanduser("~")

ORIGIN_OVERRIDES = {
    "250259813002": [-71.01728, 42.36671], # logan airport
    "250259817001": [-71.06843, 42.35438], # boston common
    "250250008032": [-71.11566, 42.35170] # bu agganis arena
}

def printjson(json_):
    """ Print a dictionary in human readable, indented format

    Args:
        json_: A dictioary to be printed
    """

    print(json.dumps(json_, indent = 4))

def arcgis_to_geojson(arcgis_json):
    """ Convert a JSON object returned by the ArcGIS FeatureServer REST API
    into a GeoJson FeatureCollection

    Args:
        arcgis_json: A JSON object of what is returned by the FeatureServer

    Returns:
        A GeoJSON object containing the data found in arcgis_json
    """

    feature_collection = {
        "type": "FeatureCollection",
        "features": []
    }

    for feature_orig in arcgis_json["features"]:
        properties = feature_orig["attributes"]
        lng = float(properties.pop("Longitude"))
        lat = float(properties.pop("Latitude"))

        feature_geojson = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [ lng, lat ]
            },
            "properties": properties
        }

        feature_collection["features"].append(feature_geojson)

    return feature_collection

def find_blockgroups():
    """ Connect to the MongoDB instance running on walnut to find blockgroups

    Returns:
        An array of GeoJSON objects corresponding to tiger_2016.blockgroups
        documents
    """

    blockgroups_collection = pymongo.MongoClient()["tiger_2016"]["blockgroups"]

    # union the evacuation zones
    evac_union = util.union_evac_zones(EVAC_ZONES)

    # find blockgroups whose polygons intersect the evacuation zone
    blockgroups = list(blockgroups_collection.find({
        "geometry.geometries.1": {
            "$geoIntersects": {
                "$geometry": shapely.geometry.mapping(evac_union)
            }
        }
    }))

    for blockgroup in blockgroups:
        blockgroup.pop("_id")

    return blockgroups

def get_geojson():
    """ Load the GeoJSON of emergency shelters, reformatting the raw data if
    necessary

    Returns:
        A GeoJSON of emergency shelters
    """

    # Retrieve a new JSON
    if (not os.path.exists(CACHED_SHELTERS_JSON_PATH)):

        response = requests.get(CACHED_SHELTERS_JSON_URL)

        assert response.status_code == 200

        arcgis_json = response.json()
        geojson = arcgis_to_geojson(arcgis_json)

        with open(CACHED_SHELTERS_JSON_PATH, "w") as f:
            json.dump(geojson, f, indent = 4)

        return geojson

    # Use a cached JSON
    else:
        with open(CACHED_SHELTERS_JSON_PATH, "r") as f:
            return json.load(f)

def get_routes(instructions):
    geojson = get_geojson()
    router = route_distances.OTPDistances(instructions["otp_host"])
    from_coords = instructions["origin"]

    results = {
        "blockgroup": {
            "geoid": instructions["geoid"],
            "origin": instructions["origin"]
        },
        "shelters": []
    }

    for shelter in geojson["features"]:
        to_coords = shelter["geometry"]["coordinates"]
        shelter_results = {
            "objectid": shelter["properties"]["OBJECTID"],
            "routes": {}
        }

        for mode in ["walk", "drive", "transit"]:
            result = (router.route(
                from_coords[0], from_coords[1], to_coords[0], to_coords[1],
                mode = mode
            ))
            shelter_results["routes"][mode] = result

        printjson(shelter_results)
        results["shelters"].append(shelter_results)

    pymongo[MONGO_DB][MONGO_COLLECTION].insert_one(results)

def main(threads = multiprocessing.cpu_count()):
    print("preloading shelters")
    geojson = get_geojson()
    print("subsetting blockgroups")
    blockgroups = find_blockgroups()
    instructions = []

    manager = otpmanager.OTPManager(
        "boston", otp_path = OTP_PATH, **BOSTON_BOUNDING_BOX
    )
    manager.start()
    time.sleep(2)

    print("building instruction set")
    for blockgroup in blockgroups:
        geoid = blockgroup["properties"]["GEOID"]

        if (geoid in ORIGIN_OVERRIDES):
            origin = ORIGIN_OVERRIDES[geoid]
            print("overriding %s origin point: %s" % (geoid, origin))
        else:
            origin = list(shapely.geometry.shape(
                blockgroup["geometry"]["geometries"][1]
            ).centroid.coords)[0]

        instructions.append({
            "geoid": geoid,
            "origin": origin,
            "otp_host": "localhost:%d" % manager.port
        })

    pool = multiprocessing.Pool(threads)
    pool.map(get_routes, instructions)
    pool.close()
    pool.join()

    manager.stop_otp()

if (__name__ == "__main__"):
    main()

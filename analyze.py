#!/usr/bin/env python3

from matplotlib import pyplot
import json
import os
import pandas

SOURCES = "sources/"
SHELTERS_JSON = "%s/shelters.json" % SOURCES
ROUTES_JSON = "%s/shelter_routes.json" % SOURCES

OUTDIR = "analysis/"
UPDATED_ROUTES_TEMPLATE = "%s/routes_%%s_sorted.json" % OUTDIR
STATS_JSON_TEMPLATE = "%s/shelter_stats_%%s_%%s.json" % OUTDIR

ACS5_CSV = "%s/ACS_2015_5YR_BG_25_MASSACHUSETTS.csv" % SOURCES
ACS5_GEOID_PREFIX = "15000US"
ACS5_POP_TOTAL = "B01003e1"

def update_routes():

    with open(SHELTERS_JSON, "r") as f:
        shelter_info = json.load(f)

    def find_shelter(object_id):
        for shelter in shelter_info["features"]:
            if (shelter["properties"]["OBJECTID"] == object_id):
                return shelter

    for mode in ["walk", "drive", "transit"]:
        output = UPDATED_ROUTES_TEMPLATE % mode
        print("Creating %s" % output)

        with open(ROUTES_JSON, "r") as f_in, open(output, "w") as f_out:
            docs = []

            for line in f_in.readlines():
                doc = json.loads(line)
                doc.pop("_id")
                for shelter in doc["shelters"]:
                    shelter["coordinates"] = find_shelter(shelter["objectid"])["geometry"]["coordinates"]
                doc["shelters"] = sorted(
                    doc["shelters"],
                    key = lambda shelter: (
                        shelter["routes"][mode]
                        and shelter["routes"][mode]["duration"]
                        # arbitrarily large number pushes items without calculated
                        # routes to the end of the list
                        or 1e10
                    )
                )
                docs.append(doc)

            json.dump(docs, f_out, indent = 4)

class Renderer(object):
    def __init__(self):
        print("Loading acs5 csv")
        self.acs5 = pandas.read_csv(ACS5_CSV)
        self.acs5.index = self.acs5.pop("GEOID")

    def render(mode, n_closest):
        print("Rendering %s, %d closest shelters" % (mode, n_closest))
        shelter_pops = {}

        with open(UPDATED_ROUTES_TEMPLATE % mode, "r") as f:
            docs = json.load(f)

        for doc in docs:
            for shelter in doc["shelters"]:
                pass

if (__name__ == "__main__"):
    if (not os.path.isdir(OUTDIR)):
        os.mkdir(OUTDIR)

    #update_routes()
    r = Renderer()
    r.render("walk", 3)

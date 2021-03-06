#!/usr/bin/env python3

from matplotlib import cm, collections, colors, lines, patches, pyplot
from cartopy import crs
from cartopy.io import img_tiles
import descartes
import itertools
import json
import matplotlib
import os
import pandas
import pymongo
import shapely

import util

# data sources
SOURCES = "sources/"
SHELTERS_JSON = "%s/shelters.json" % SOURCES
ROUTES_JSON = "%s/shelter_routes.json" % SOURCES
BOSTON_GEOJSON = "%s/boston.geojson" % SOURCES
with open(BOSTON_GEOJSON, "r") as f:
    BOSTON_POLYGON = shapely.geometry.shape(json.load(f))


ACS5_CSV = "%s/acs5_2015_ma_subset.csv" % SOURCES
ACS5_GEOID_PREFIX = "15000US"
ACS5_POP_TOTAL_COL = "B01003e1"

BG_DB = "tiger_2016"
BG_COLLECTION = "blockgroups"

EVAC_DB = "massgis_mapserver"
EVAC_COLLECTION = "CityServices.Evacuation"
EVAC_EXCLUDE = ["ZONE A", "ZONE B", "ZONE C"]

# formatted data
OUTDIR = "analysis/"
UPDATED_ROUTES_TEMPLATE = "%s/routes_%%s_sorted.json" % OUTDIR
STATS_JSON_TEMPLATE = "%s/shelter_stats_%%s_%%s.json" % OUTDIR

# ignore
IGNORE_GEOIDS = [
    "250259901010", # ocean
    "250235001011" # hull
]

# appearance
DPI = 400
TITLE_FONT_SIZE = 8
BOUNDING_BOX = [-71.2, 42.21, -70.9, 42.42]

COLORMAP = "YlOrRd"
#COLORMAP = "Paired"
#COLORMAP = "viridis_r"

BOSTON_BOUNDARIES_COLOR = "#777777"
COLOR_INACCESSIBLE = "#bbbbbb"
POLY_OPACITY = 0.8

SHELTER_COLOR = "#4daf4a"
SHELTER_COLOR_EXCLUDED = "#377eb8"
#SHELTER_COLOR_UNUSED = "#984ea3"
#SHELTER_COLOR_UNUSED = SHELTER_COLOR
SHELTER_COLOR_UNUSED = "#2f6b2d" # darker version of shelter_color
SHELTER_MIN_SIZE = 2
SHELTER_MAX_SIZE = 5

SHELTER_LINK_COLOR = "#294040"
SHELTER_LINK_LINEWIDTH = 0.25
SHELTER_LINK_OPACITY = 0.2

LEGEND_MARKER_SIZE = 6

def update_routes():
    """ Add information to and organize the raw data generated by simulate.py
    """

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

class Analyst(object):

    def __init__(self):
        self.acs5 = pandas.read_csv(ACS5_CSV)
        self.acs5.index = self.acs5.pop("GEOID")

    def analyze(self, mode, n_closest, excluded_zones = EVAC_EXCLUDE):
        """ Analyze data generated by update_routes()

        Args:
            mode: The mode of transport (walk, drive, transit)
            n_closest: The number of closest shelters to consider
            excluded_zones: A list of zones to exclude

        Returns:
            A dictionary, documented below
        """

        data = {
            # Structure:
            #     key: String representation of shelter object ID
            #     value: Population that that shelter serves
            "shelter_pops": {},

            # Array of the JSON data for all shelters encountered during the
            # analysis
            "shelters": [],

            # Contains dictionaries with the following indices:
            # avg_travel: The average travel time, or False if no routes could
            #     be made to this block group
            # geoid: GEOID string
            # geojson: GeoJSON dict
            # population: population int
            "blockgroups": [],

            # Origin-destination pairs of coordinate pairs of straight lines
            # between block groups and shelters
            "bg_to_shelter_lines": [],

            # Reduce the number of spatial calculations
            # Additionally, excluded_shelters can be used for stylistic reasons
            "excluded_shelters": set(),

            # Analysis parameter information
            "mode": mode,
            "n_closest": n_closest,
            "excluded_zones": excluded_zones
        }

        not_excluded_shelters = set()

        exclude_polygon = False
        if (type(excluded_zones) is list):
            exclude_polygon = util.union_evac_zones(excluded_zones)

        with open(UPDATED_ROUTES_TEMPLATE % mode, "r") as f:
            for doc in json.load(f):
                if (not doc["blockgroup"]["geoid"] in IGNORE_GEOIDS):

                    bg_geoid = doc["blockgroup"]["geoid"]

                    if (not BOSTON_POLYGON.contains(
                        shapely.geometry.Point(doc["blockgroup"]["centroid"])
                    )):
                        continue

                    bg_acs5_geoid = ACS5_GEOID_PREFIX + bg_geoid
                    bg_pop = self.acs5[ACS5_POP_TOTAL_COL][bg_acs5_geoid]

                    travel_times = []

                    for shelter in doc["shelters"]:

                        if (not shelter in data["shelters"]):
                            data["shelters"].append(shelter)

                        if (shelter["routes"][mode] is not False):

                            if (exclude_polygon):
                                object_id = shelter["objectid"]

                                if (object_id in data["excluded_shelters"]):
                                    continue
                                elif (object_id in not_excluded_shelters):
                                    pass
                                elif (exclude_polygon.contains(
                                    shapely.geometry.Point(shelter["coordinates"])
                                )):
                                    data["excluded_shelters"].add(object_id)
                                    continue
                                else:
                                    not_excluded_shelters.add(object_id)

                            travel_times.append(
                                shelter["routes"][mode]["duration"]
                            )

                            object_id_str = str(shelter["objectid"])
                            if (object_id_str in data["shelter_pops"]):
                                data["shelter_pops"][object_id_str] += bg_pop
                            else:
                                data["shelter_pops"][object_id_str] = bg_pop

                            data["bg_to_shelter_lines"].append([
                                shelter["coordinates"],
                                doc["blockgroup"]["origin"]
                            ])

                        if (len(travel_times) == n_closest):
                            break

                    if (len(travel_times) > 0):
                        bg_avg_travel = sum(travel_times) / len(travel_times)
                    else:
                        bg_avg_travel = False

                    data["blockgroups"].append({
                        "avg_travel": bg_avg_travel / 60,
                        "geoid": bg_geoid,
                        "population": bg_pop
                    })

        return data

class Renderer(object):

    def __init__(self):
        self.analyst = Analyst()
        self.colormap = cm.get_cmap(COLORMAP)
        self.blockgroup_collection = pymongo.MongoClient()[BG_DB][BG_COLLECTION]
        self.blockgroup_polygon_cache = {}

    def retrieve_blockgroup_polygon(self, geoid):
        """ Retrieve the polygon of a block group from the MongoDB instance or
        the cache

        Args:
            geoid: The GEOID of the block group

        Returns:
            A GeoJSON polygon object
        """

        if (geoid in self.blockgroup_polygon_cache):
            return self.blockgroup_polygon_cache[geoid]
        else:
            blockgroup_polygon = self.blockgroup_collection.find_one({
                "properties.GEOID": geoid
            })["geometry"]["geometries"][1]

            self.blockgroup_polygon_cache[geoid] = blockgroup_polygon
            #print("Cached %s" % geoid)

            return blockgroup_polygon

    def render(self, data, output_file = None, min_colorbar = None,
               max_colorbar = None):
        """ Render data generate by Analyst.analyze

        Args:
            data: A dictionary containing data generated by Analyst.analyze
            output_file: The file to save the visualization to. If None, the
                visualization is displayed instead
            min_colorbar, max_colorbar: The limits to use for the colorbar. If
                None, these are calculated from the data
        """

        #print("Excluded shelters: %s" % data["excluded_shelters"])
        #print("Populations served by shelters: %s" % data["shelter_pops"])

        stamen_terrain = img_tiles.StamenTerrain()
        point_transform = crs.Geodetic()
        poly_transform = crs.PlateCarree()

        figure, axis = pyplot.subplots(
            subplot_kw = {"projection": stamen_terrain.crs}
        )

        axis.set_extent([
            BOUNDING_BOX[0], BOUNDING_BOX[2], BOUNDING_BOX[1], BOUNDING_BOX[3]
        ])
        axis.add_image(stamen_terrain, 13)

        ## city boundaries
        boston_patch = descartes.PolygonPatch(
            shapely.geometry.mapping(BOSTON_POLYGON),
            facecolor = "none",
            edgecolor = BOSTON_BOUNDARIES_COLOR,
            linewidth = 0.5,
            alpha = POLY_OPACITY
        )
        boston_patch.set_transform(poly_transform)
        axis.add_patch(boston_patch)

        ## blockgroup plotting
        bg_travel = [
            blockgroup["avg_travel"]
            for blockgroup in data["blockgroups"]
        ]
        min_bg_travel = min(bg_travel)
        max_bg_travel = max(bg_travel)
        bg_travel_range = max_bg_travel - min_bg_travel

        if (
            (min_colorbar is not None)
            and (max_colorbar is not None)
        ):
            colormap_normalize = colors.Normalize(min_colorbar, max_colorbar)
        else:
            colormap_normalize = colors.Normalize(min_bg_travel, max_bg_travel)

        for blockgroup in data["blockgroups"]:
            bg_geoid = blockgroup["geoid"]

            if blockgroup["avg_travel"]:
                facecolor = self.colormap(
                    colormap_normalize(blockgroup["avg_travel"])
                )
            else:
                # rgb conversion is done for the value transformation
                facecolor = colors.to_rgb(COLOR_INACCESSIBLE)

            #edgecolor_hsv = colors.rgb_to_hsv(facecolor[:3])
            #edgecolor_hsv[2] *= 0.5

            #print("Adding block group %s" % bg_geoid)
            patch = descartes.PolygonPatch(
                self.retrieve_blockgroup_polygon(bg_geoid),
                facecolor = facecolor,
                #edgecolor = colors.hsv_to_rgb(edgecolor_hsv)
                edgecolor = facecolor,
                alpha = POLY_OPACITY
            )
            patch.set_transform(poly_transform)
            axis.add_patch(patch)

        ## shelter plotting
        shelter_pops_values = [
            data["shelter_pops"][objectid] / 3
            for objectid in data["shelter_pops"]
        ]
        min_pop = min(shelter_pops_values)
        max_pop = max(shelter_pops_values)
        pop_range = max_pop - min_pop

        for shelter in data["shelters"]:
            object_id_str = str(shelter["objectid"])

            if (object_id_str in data["shelter_pops"]):
                pop = data["shelter_pops"][object_id_str]
                pop_normalized = (pop - min_pop) / pop_range
                last_shelter = axis.plot(
                    shelter["coordinates"][0],
                    shelter["coordinates"][1],
                    marker = "o",
                    markersize = SHELTER_MIN_SIZE + (
                        pop_normalized * (SHELTER_MAX_SIZE - SHELTER_MIN_SIZE)
                    ),
                    color = SHELTER_COLOR,
                    transform = point_transform
                )

            # excluded by query
            elif (shelter["objectid"] in data["excluded_shelters"]):
                last_shelter_excluded = axis.plot(
                    shelter["coordinates"][0],
                    shelter["coordinates"][1],
                    marker = "o",
                    markersize = SHELTER_MIN_SIZE,
                    color = SHELTER_COLOR_EXCLUDED,
                    transform = point_transform
                )

            # not among the n closest of any block group
            else:
                last_shelter_unused = axis.plot(
                    shelter["coordinates"][0],
                    shelter["coordinates"][1],
                    marker = "o",
                    markersize = SHELTER_MIN_SIZE,
                    color = SHELTER_COLOR_UNUSED,
                    transform = point_transform
                )

        ## lines from shelters to block groups
        line_collection = collections.LineCollection(
            data["bg_to_shelter_lines"],
            colors = SHELTER_LINK_COLOR,
            linewidths = SHELTER_LINK_LINEWIDTH,
            alpha = SHELTER_LINK_OPACITY
        )
        line_collection.set_transform(poly_transform)
        axis.add_collection(line_collection)

        ## final tweaks
        if (data["n_closest"] > 1):
            n_closest_string = "%d closest shelters" % data["n_closest"]
        else:
            n_closest_string = "closest shelter"
        pyplot.title(
            "Relationships between block groups and the %s; mode of transit = "
            "%s" % (
                n_closest_string, data["mode"]
            ),
            fontsize = TITLE_FONT_SIZE
        )

        mappable = pyplot.cm.ScalarMappable(colormap_normalize, COLORMAP)
        mappable.set_array([min_bg_travel, max_bg_travel])
        colorbar = figure.colorbar(mappable)
        colorbar.set_label("Average transit time, in minutes")

        ## legend
        handles = [
            lines.Line2D(
                [], [], linewidth = 0, marker = "o",
                markersize = LEGEND_MARKER_SIZE,
                color = SHELTER_COLOR,
                label = "Shelter"
                #label = "Active shelter"
            ),
            #lines.Line2D(
            #    [], [], linewidth = 0, marker = "o",
            #    markersize = LEGEND_MARKER_SIZE,
            #    color = SHELTER_COLOR_UNUSED,
            #    label = "Out-of-way shelter"
            #),

            patches.Patch(
                color = COLOR_INACCESSIBLE,
                label = "No easy access to shelters"
            )
        ]
        if (len(data["excluded_shelters"]) > 0):
            handles = [lines.Line2D(
                [], [], linewidth = 0, marker = "o",
                markersize = LEGEND_MARKER_SIZE,
                color = SHELTER_COLOR_EXCLUDED,
                label = "Unsafe shelter"
            )] + handles
        pyplot.legend(loc = "lower right", handles = handles)

        ## remove tick marks around the map
        axis.set_xticks([])
        axis.set_yticks([])

        ## finished
        print("Rendering image")
        if (output_file is None):
            pyplot.show()
        else:
            pyplot.savefig(output_file, dpi = DPI)
            print("Saved to %s" % output_file)
        pyplot.close()

def render_all_modes(n_closest_list = [1, 3], excluded_zones = EVAC_EXCLUDE):
    if (type(n_closest_list) == int):
        n_closest_list = [n_closest_list]

    a = Analyst()
    r = Renderer()

    data = {
        "walk": {},
        "drive": {},
        "transit": {}
    }

    for n_closest in n_closest_list:
        for mode in data:
            print("Analyzing %s; %d closest" % (mode, n_closest))
            data[mode][str(n_closest)] = a.analyze(mode, n_closest,
                                                   excluded_zones)

    all_travel = []
    for n_closest in n_closest_list:
        for mode in data:
            all_travel += [
                blockgroup["avg_travel"]
                for blockgroup in data[mode][str(n_closest)]["blockgroups"]
            ]

    for n_closest in n_closest_list:
        for mode in data:
            print("Rendering %s; %d closest" % (mode, n_closest))
            r.render(
                data[mode][str(n_closest)],
                "%s_%d_closest.png" % (mode, n_closest),
                min(all_travel), max(all_travel)
            )

if (__name__ == "__main__"):
    if (not os.path.isdir(OUTDIR)):
        os.mkdir(OUTDIR)

    #Renderer().render(Analyst().analyze("walk", 3))
    render_all_modes()
    #render_all_modes(excluded_zones = None)

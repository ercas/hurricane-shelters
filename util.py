#!/usr/bin/env python3
# miscellaneous functions shared by analyze.py and simulate.py

import pymongo
import shapely.geometry

DB = "massgis_mapserver"
COLLECTION = "CityServices.Evacuation"

def union_evac_zones(zone_list):
    """ Given a list of evacuation zone names, return their union

    Args:
        zone_list: A list of strings containing zone names that the relevant
            polygons have in their properties.ZONE field

    Returns:
        A unioned shapely.geometry.multipolygon.MultiPolygon, or None if no
        evacuation zones could be found
    """

    evac_zones = pymongo.MongoClient()[DB][COLLECTION]
    evac_union = None

    for zone in zone_list:
        # create a very small buffer so that they will overlap barely
        zone_shape = shapely.geometry.shape(
            evac_zones.find_one({"properties.ZONE": zone})["geometry"]["geometries"][1]
        ).buffer(1e-9)

        if (evac_union is None):
            evac_union = zone_shape
        else:
            evac_union = evac_union.union(zone_shape)

    return evac_union

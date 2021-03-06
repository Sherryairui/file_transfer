from pyspark import SparkContext

def readGeoFile(shapefile_new):
    import fiona
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile_new).to_crs(fiona.crs.from_epsg(2263))
    return zones


def processTrips(pid, records):
    import fiona
    import fiona.crs
    import shapely
    import rtree
    import pandas as pd
    import geopandas as gpd
    import csv
    import pyproj
    import shapely.geometry as geom

    if pid==0:
        next(records)

    counts = {}
    import rtree
    reader = csv.reader(records)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    shapefile_start = 'neighborhoods.geojson'
    #shapefile_end = 'boroughs.geojson'   

    neighborhoods = gpd.read_file(shapefile_start).to_crs(fiona.crs.from_epsg(2263))
    #boroughs = gpd.read_file(shapefile_end).to_crs(fiona.crs.from_epsg(2263))

    index_start = rtree.Rtree()
    for idx,geometry in enumerate(neighborhoods.geometry):
        index_start.insert(idx, geometry.bounds)

    # index_end = rtree.Rtree()
    # for idx,geometry in enumerate(boroughs.geometry):
    #     index_end.insert(idx, geometry.bounds)

    for row in reader:
        try:
            p_start = geom.Point(proj(float(row[5]), float(row[6])))
            p_end = geom.Point(proj(float(row[9]), float(row[10])))
        except:
            continue
        
        match_end = None
        for idx in index_start.intersection((p_end.x, p_end.y, p_end.x, p_end.y)):
            shape = neighborhoods.geometry[idx]
            if shape.contains(p_end):
                match_end = neighborhoods['borough'][idx]
                break
        if match_end:
            match_start = None
            for idx in index_start.intersection((p_start.x, p_start.y, p_start.x, p_start.y)):
                shape = neighborhoods.geometry[idx]
                if shape.contains(p_start):
                    match_start = neighborhoods['neighborhood'][idx]
                    break
            if match_start:
                yield((match_start, match_end), 1)
                #counts[match_start] = counts.get(match_start, 0) + 1
                #counts[(match_start, match_end)] = counts.get((match_start, match_end), 0) + 1
    #return counts.items()


if __name__ == "__main__": 
    #shapefile = 'neighborhoods.geojson'
    #neighborhoods = readGeoFile(shapefile)

    #shapefile = 'boroughs.geojson'
    #boroughs = readGeoFile(shapefile)

    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/yellow_tripdata_2011-05.csv')
    #.map(lambda x: (boroughs['boroname'][x[0][1]], (neighborhoods['neighborhood'][x[0][0]], x[1]))) \
    counts = rdd.mapPartitionsWithIndex(processTrips) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0][1], (x[0][0], x[1]))) \
                .reduceByKey(lambda x,y: x+y).collect()

    for i in range(0, len(counts)):
        print(counts[i][0])
        print(sc.parallelize(zip(counts[i][1][::2], counts[i][1][1::2])).top(3, lambda x: x[1]))

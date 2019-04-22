from pyspark import SparkContext

def readTxt(text_file):
    text_file = open(text_file, "r")
    drug_word = text_file.read().split('\n')
    return(drug_word)

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processDrugs(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('hdfs:///tmp/bdm/500cities_tracts.geojson')  
    
    drug_word_1 = readTxt('hdfs:///tmp/bdm/drug_illegal.txt')
    drug_word_2 = readTxt('hdfs:///tmp/bdm/drug_sched2.txt')
    
    drug_words = drug_word_1+drug_word_2
    
    reader = csv.reader(records, delimiter='|')
    
    for rowList in reader:
        
        latitude = rowList[1]
        longitude = rowList[2]
        tweet = rowList[-1].split(' ')
        
        
        for item in tweet:
            if item in drug_words:
                p = geom.Point(proj(float(longitude), float(latitude)))
                zone = findZone(p, index, zones)
                if zone:
                    yield((zones['plctract10'][zone], zones['plctrpop10'][zone]), 1)

if __name__ == "__main__": 

	sc = SparkContext()
	#print('1')
	rdd = sc.textFile('hdfs:///tmp/bdm/tweets.csv')
	counts = rdd.mapPartitionsWithIndex(processDrugs) \
	            .reduceByKey(lambda x,y: x+y) \
	            .map(lambda x: (x[0][0], float(x[1])/x[0][1])).collect()
	print(counts)

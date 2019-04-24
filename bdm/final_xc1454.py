from pyspark import SparkContext

def readTxt(text_file, singleDrug, multiDrugFirst, multiDrugAll):
    import string
    text_file = open(text_file, "r")
    drug_word = text_file.read().split('\n')

    for item in drug_word:
        item = item.lower()
        if len(item.split(' ')) == 1:
            singleDrug.add(item.lower())
        else:
            multiDrugFirst.add(item.split(' ')[0])
            item = [''.join(c for c in s if c not in string.punctuation) for s in item.split(' ')]
            item = [s.lower() for s in item if s]
            item_string = ' '.join(e for e in item)
            if item[0] in multiDrugAll:
                multiDrugAll[item[0]] += [item_string]
            else:
                multiDrugAll[item[0]] = [item_string]
        
        
    return(singleDrug, multiDrugFirst, multiDrugAll)

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

def processTwitter(row):
    import string
    
    latitude = row[1]
    longitude = row[2]
    tweet_raw = row[5].encode('ascii', 'ignore').decode('ascii').split(' ')
    tweet_no_punct = [''.join(c for c in s if c not in string.punctuation) for s in tweet_raw]
    tweet = [s.lower() for s in tweet_no_punct if s]
    tweet_set = set(tweet)
    tweet_string = ' '.join(e for e in tweet_no_punct)
    return(latitude, longitude, tweet_set, tweet_string)

def processDrugs(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    import re
    # import sys
    # reload(sys)
    # sys.setdefaultencoding('utf-8')
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')  
    
    singleDrug = set()
    multiDrugFirst = set()
    multiDrugAll = {}
    
    singleDrug, multiDrugFirst, multiDrugAll = readTxt('drug_illegal.txt', singleDrug, multiDrugFirst, multiDrugAll)
    singleDrug, multiDrugFirst, multiDrugAll = readTxt('drug_sched2.txt', singleDrug, multiDrugFirst, multiDrugAll)
    
    reader = csv.reader(records, delimiter='|')
    
    for row in reader:
        
        latitude, longitude, tweet_set, tweet_string = processTwitter(row)
        
        #yield(tweet_string, 1)

        if singleDrug.intersection(tweet_set):
            #yield(1,1)
            p = geom.Point(proj(float(longitude), float(latitude)))
            zone = findZone(p, index, zones)
            if zone:
                yield((zones['plctract10'][zone], zones['plctrpop10'][zone]), 1)
        elif tweet_set.intersection(multiDrugFirst):
            #yield(1,1)
            for item in list(tweet_set.intersection(multiDrugFirst)):
                for durgString in multiDrugAll[item]:
                    if re.match(durgString, tweet_string):
                        p = geom.Point(proj(float(longitude), float(latitude)))
                        zone = findZone(p, index, zones)
                        if zone:
                            yield((zones['plctract10'][zone], zones['plctrpop10'][zone]), 1)

if __name__ == "__main__": 

	sc = SparkContext()
	rdd = sc.textFile('tweets.csv')
	counts = rdd.mapPartitionsWithIndex(processDrugs) \
	            .reduceByKey(lambda x,y: x+y) \
	            .map(lambda x: (x[0][0], float(x[1])/x[0][1])).collect()
	print(counts)

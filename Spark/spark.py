##### from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from geopy.geocoders import Nominatim
from filtertweet import filterWords
import random
import json
import nltk

#nltk.download('vader_lexicon')


def analyzeSentiment(string):

    sid = SentimentIntensityAnalyzer()
    sentimentMap = sid.polarity_scores(string)
    del sentimentMap['compound']
    sentiment = max(sentimentMap,key = sentimentMap.get)
    wordMap = {'pos':'positive','neg':'negative','neu':'neutral'}
    return wordMap[sentiment]
    #print sorted_values[0],ss[0]

def parseDate(date):
    index = date.find('+')
    date = date[:index]
    split = date.split()
    date = '2018-04-11 '+split[3]
    return date

def findGeo(location):
    geolocator = Nominatim()
    coordinates = geolocator.geocode(location)
    return coordinates

def findCoordinates(place,user_location):
    return randomGeo()
    if place:    
        if place['bounding_box']['coordinates']:
            latitude = place['bounding_box']['coordinates'][0][0][0]
            longitude = place['bounding_box']['coordinates'][0][0][1]
            return {
            "lat":latitude,"lon":longitude
            }
        elif place['country']:
            coordinates = findGeo(place['country'])
            return{
                "lat":coordinates.latitude,
                "lon":coordinates.longitude
            }
           #print location.latitude,location.longitude
    elif user_location:
        user_location = user_location.split(',')[0]
        coordinates = findGeo(user_location)
        if not coordinates:
            return randomGeo()
        return{
            "lat":coordinates.latitude,
            "lon":coordinates.longitude
        }
    else:
        randomGeo()
            
def randomGeo():
    geoLocations = [{"lat":32.806671,
    "lon":-86.791130},
    {"lat":61.370716,
    "lon":-152.404419},
    {"lat":61.370716,
    "lon":-152.404419},
     {"lat":36.116203, "lon":-119.681564},
     {"lat":21.094318, "lon":-157.498337},
    {"lat":42.165726,"lon":-74.948051},
    {"lat":41.680893,"lon":-71.511780},
    {"lat":44.306940,"lon":20.571010},
    {"lat":51.624980,"lon":20.816150},
    {"lat":45.780000,"lon":7.300000},]
    return random.choice(geoLocations);
    
def makeData(tweet):
    jsonObj = json.loads(tweet)
    
    return{
        
        "text":filterWords(jsonObj['text']),
        "location":findCoordinates(jsonObj['place'],jsonObj['user']['location']),
        "sentiment":analyzeSentiment(jsonObj['text']),
        "timestamp":parseDate(jsonObj['created_at'])
    }
    
    

TCP_IP = 'localhost'
TCP_PORT = 8038

ind = 'twitter'
es = Elasticsearch([{'host':'localhost','port':'9200'}])
if(es.indices.exists(index='twitter')):
    pass
else:
    es.indices.create(index='twitter',
             body={"mappings": {
                            "doc": {
                              "properties": {
                                "location":{"type":"geo_point"},
                                "timestamp":{
                                    "type":"date",
                                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS"
                                }
                              }
                        }
                  }
             }
            )
# Pyspark
# create spark configuration
#conf = SparkConf()
#conf.setAppName('TwitterApp')
#conf.setMaster('local[2]')
# create spark context with the above configuration
#sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port specified above
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
#dataStream.pprint()
######### your processing here ###################
#dataStream.pprint()
#words = dataStream.flatMap(lambda x: x.split(' '))
#wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)



rdd = dataStream.map(lambda line: (None,json.dumps(makeData(line)))) # Saves it as an RDD pair to be recognized by the es-hadoop jar

rdd.pprint()
conf = {"es.resource": "twitter/doc", "es.input.json": "true","es.nodes": "localhost:9200"}
rdd.foreachRDD(lambda x: x.saveAsNewAPIHadoopFile(
        path="-",
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=conf))

#################################################

ssc.start()
ssc.awaitTermination()


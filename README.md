# Big-Data

## Spark Streaming 
* The project involved parsing live streams of tweets using the twitter API and continuously feed them into Elasticsearch using Apache Spark Streaming.
* Kibana is used for visualizing the data fed into Elasticsearch for analysis, such as using geo-encoded locations to determine the distribution of tweets across various locations.
* Input is taken as hashtags
* The data is parsed using Natural Language Tool Kit to classify the sentiment of each tweet

### Requirements
* [Apache Spark](https://spark.apache.org/)
* [Elasticsearch](https://www.elastic.co/)
* [Kibana](https://www.elastic.co/products/kibana)
* [NLTK](https://www.nltk.org/)

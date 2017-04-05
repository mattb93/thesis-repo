package edu.vt.dlib.api.dataStructures

class SVTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val path: String, val config: SVConfig = new SVConfig()) extends TweetCollection(collectionID, sc, sqlContext) {	

    // Filter for bad input lines. It isn't really our problem if the input isn't formatted correctly, but it's easy enough to check for here
    //collection = sc.textFile(path).filter(line => line.split(config.separator).length < config.numColumns).map(line => new SVTweet(line, config))
    collection = sc.textFile(path).filter(line => line contains config.separator).map(line => new SVTweet(line, config))
}

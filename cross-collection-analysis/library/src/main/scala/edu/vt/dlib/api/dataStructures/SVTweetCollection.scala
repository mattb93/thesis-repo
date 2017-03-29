package edu.vt.dlib.api.dataStructures

class SVTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val path: String, val config: SVConfig = new SVConfig()) extends TweetCollection(collectionID, sc, sqlContext) {	

    // idcolumn, textcolumn
    collection = sc.textFile(path).map(line => new SVTweet(line, config))
}

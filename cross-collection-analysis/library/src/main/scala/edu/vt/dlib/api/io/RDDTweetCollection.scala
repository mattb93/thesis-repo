package edu.vt.dlib.api.io

class RDDTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val _collection: org.apache.spark.rdd.RDD[(String, Array[String])]) extends TweetCollection(collectionID, sc, sqlContext) {
	var collection = _collection
}

package edu.vt.dlib.api.io

class RDDTweetCollection(sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val _collection: org.apache.spark.rdd.RDD[(String, Array[String])]) extends TweetCollection(sc, sqlContext) {
	var collection = _collection
}

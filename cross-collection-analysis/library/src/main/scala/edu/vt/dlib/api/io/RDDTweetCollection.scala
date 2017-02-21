package edu.vt.dlib.api.io

class RDDTweetCollection(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext, val _collection: org.apache.spark.rdd.RDD[(String, Array[String])]) extends TweetCollection {
	collection = _collection
}
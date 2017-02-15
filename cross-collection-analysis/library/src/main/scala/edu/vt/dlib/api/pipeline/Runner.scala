package edu.vt.dlib.api.pipeline

//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.io.TweetCollection

//class Runner(val collectionNumbers : Array[Int], val r: Runnable) {
class Runner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(collectionNumbers: Array[String], r: Runnable) {
		for( collectionNumber <- collectionNumbers) {
			
			var collection = new TweetCollection(collectionNumber, sc, sqlContext)
			r.run(collection)
		}
	}
}

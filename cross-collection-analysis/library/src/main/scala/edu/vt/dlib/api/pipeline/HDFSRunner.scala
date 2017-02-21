package edu.vt.dlib.api.pipeline

//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.io._

//class Runner(val collectionIDs : Array[Int], val r: Runnable) {
class HDFSRunner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(r: Runnable, collectionIDs: Array[String]) {
		for(collectionID <- collectionIDs) {
			
			var collection = new HDFSTweetCollection(sc, sqlContext, collectionID)
			r.run(collection)
		}
	}
}

package edu.vt.dlib.api.pipeline

//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.dataStructures.HDFSTweetCollection

class HDFSRunner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(r: Runnable, collectionIDs: Array[String]) {
		for(collectionID <- collectionIDs) {
			
			var collection = new HDFSTweetCollection(collectionID, sc, sqlContext, collectionID)
			r.run(collection)
		}
	}
}

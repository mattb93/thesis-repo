package edu.vt.dlib.api.pipeline
/*
//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.dataStructures.SVTweetCollection
import edu.vt.dlib.api.dataStructures.SVConfig

class SVRunner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(r: Runnable, paths: Array[String], config: SVConfig) {
		for(path <- paths) {
			
			var collection = new SVTweetCollection(path.split("/").last.split('.')(0), sc, sqlContext, path, config)
			r.run(collection)
		}
	}
}
*/

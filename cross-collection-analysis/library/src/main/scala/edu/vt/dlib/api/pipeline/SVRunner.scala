package edu.vt.dlib.api.pipeline

//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.io._

class SVRunner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(r: Runnable, paths: Array[String], separator: String = ",", textColumn: Int = 1, idColumn: Int = 0) {
		for(path <- paths) {
			
			var collection = new SVTweetCollection(path.split("/").last.split(",").first, sc, sqlContext, path, separator, textColumn, idColumn)
			r.run(collection)
		}
	}
}

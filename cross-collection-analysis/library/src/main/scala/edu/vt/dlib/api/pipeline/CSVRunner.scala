package edu.vt.dlib.api.pipeline

//import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.io._

class CSVRunner(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
	def run(r: Runnable, paths: Array[String], textColumn: Int = 1, idColumn: Int = 0) {
		for(path <- paths) {
			
			var collection = new CSVTweetCollection(path, sc, sqlContext, path, textColumn, idColumn)
			r.run(collection)
		}
	}
}

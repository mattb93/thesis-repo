package edu.vt.dlib.api.io

class CSVTweetCollection(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext, val path: String, val textColumn: Int = 1, val idColumn: Int = 0) extends TweetCollection {
	

	val records = sc.textFile("file://" + path)
    collection = records.map(line=> line.split(", ")).map(elem => (elem(idColumn), elem(textColumn).split(" "))

}
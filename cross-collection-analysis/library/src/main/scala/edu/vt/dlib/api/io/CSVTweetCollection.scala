package edu.vt.dlib.api.io

class CSVTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val path: String, val textColumn: Int = 1, val idColumn: Int = 0) extends TweetCollection(collectionID, sc, sqlContext) {	

	val records = sc.textFile("file://" + path)
    var collection = records.map(line=> line.split(", ")).map(elem => (elem(idColumn), elem(textColumn).split(" ")))

}

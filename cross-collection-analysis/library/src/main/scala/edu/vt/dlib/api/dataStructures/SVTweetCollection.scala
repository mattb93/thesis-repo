package edu.vt.dlib.api.dataStructures

class SVTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, val path: String, val separator: String = ",",  val textColumn: Int = 1, val idColumn: Int = 0) extends TweetCollection(collectionID, sc, sqlContext) {	

    // idcolumn, textcolumn
    var collection = sc.textFile(path).map(line => line.split(separator)).map(elem => (elem(idColumn), elem(textColumn).split(" ")))
}

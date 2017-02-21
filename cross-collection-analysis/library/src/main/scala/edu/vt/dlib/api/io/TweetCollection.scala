package edu.vt.dlib.api.io

/*
 * Provides convenience methods to read tweet data from and write tweet data to the DLRL cluster.
 * Reads from avro files and provides methods to map data to more useful formats.
 * 
 */
//class TweetCollection(var collectionId: String, val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
class TweetCollection(var path: String, val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
    import org.apache.avro.mapred.AvroInputFormat
    import org.apache.avro.mapred.AvroWrapper
    import org.apache.avro.generic.GenericRecord
    import org.apache.spark.rdd.RDD
    import org.apache.spark.ml.feature.StopWordsRemover
    import org.apache.hadoop.io.NullWritable
    import scala.collection.mutable.WrappedArray
    
    import sqlContext.implicits._

    var collection

    def getCollection() : RDD[(String, Array[String])] = {
        return collection
    }

    def getPlainText(): RDD[String] = {
        return collection.map(entry => entry._2.mkString(" "))
    }

    def getPlainTextID() : RDD[(String, String)] = {
        return collection.map(entry => (entry._1, entry._2.mkString(" ")))
    }

    def getTextArrays(): RDD[Array[String]] = {
        return collection.map(entry => entry._2)
    }

    def getTextArraysID(): RDD[(String, Array[String])] = {
        return collection
    }

    def removeStopWords() : TweetCollection = {
        println("Removing Stop Words")

        val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")
        collection = remover.transform(collection.toDF("id", "raw")).select("id", "filtered").map(row => (row(0).toString, (row(1).asInstanceOf[WrappedArray[String]]).toArray))
        return this
    }

    def removeRTs() : TweetCollection = {
        println("Removing 'RT' instances")

        collection = collection.map(entry => (entry._1, entry._2.filter(! _.contains("RT"))))

        return this
    }

    def removeMentions() : TweetCollection = {
        println("Removing mentions")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! """\"*@.*""".r.pattern.matcher(x).matches)))
        return this
    }

    def removeHashtags() : TweetCollection = {
        println("Removing hashtags")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! """#.*""".r.pattern.matcher(x).matches)))
        return this
    }

    def removeURLs() : TweetCollection = {
        println("Removing URLs")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! """.*http.*""".r.pattern.matcher(x).matches)))
        return this
    }

    def removePunctuation() : TweetCollection = {
        println("Removing punctiation")
        collection = collection.map(entry => (entry._1, entry._2.map(x => x.replaceAll("[^A-Za-z0-9@#]", "")))).filter(entry => entry._2.length > 0)
        return this
    }

    def toLowerCase() : TweetCollection = {
        println("Converting to lowercase")
        collection = collection.map(entry => (entry._1, entry._2.map(x => x.toLowerCase())))
        return this
    }

    def removeRegexMatches(regex: scala.util.matching.Regex) : TweetCollection = {
        println("Removing regex")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! regex.pattern.matcher(x).matches)))
        return this
    }

    def removeRegexNonmatches(regex: scala.util.matching.Regex) : TweetCollection = {
        println("Removing regex")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => regex.pattern.matcher(x).matches)))
        return this
    }
}

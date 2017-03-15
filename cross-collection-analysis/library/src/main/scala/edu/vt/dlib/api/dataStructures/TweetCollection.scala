package edu.vt.dlib.api.dataStructures

import java.io.Serializable

/*
 * Provides convenience methods to read tweet data from and write tweet data to the DLRL cluster.
 * Reads from avro files and provides methods to map data to more useful formats.
 * 
 */
abstract class TweetCollection(val collectionID: String, @transient val sc: org.apache.spark.SparkContext, @transient val sqlContext: org.apache.spark.sql.SQLContext) extends Serializable {
    
    import org.apache.spark.rdd.RDD
    import org.apache.spark.ml.feature.StopWordsRemover
    import scala.collection.mutable.WrappedArray
    
    import sqlContext.implicits._

    // http://alvinalexander.com/scala/how-to-control-scala-method-scope-object-private-package
    var collection: RDD[Tweet];

    def getCollection() : RDD[Tweet] = {
        return collection
    }

    def getPlainText(): RDD[String] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => tweet.tokens.mkString(" "))
    }

    def getPlainTextID() : RDD[(String, String)] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => (tweet.id, tweet.tokens.mkString(" ")))
    }

    def getTokenArrays(): RDD[Array[String]] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => tweet.tokens)
    }

    def getTokenArraysID(): RDD[(String, Array[String])] = {
        return collection.filter(tweet => ! tweet.tokrns.isEmpty).map(tweet => (tweet.id, tweet.tokens))
    }

    def removeStopWords() : TweetCollection = {
        println("Removing Stop Words NOT CURRENTLY IMPLEMENTED")

        //val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")
        //val rawTextDF = collection.map(tweet => tweet.text).toDF("id", "raw")
        //collection = remover.transform(collection.map(rawTextDF).select("id", "filtered").map(row => (row(0).toString, (row(1).asInstanceOf[WrappedArray[String]]).toArray))
        return this
    }

    def removeRTs() : TweetCollection = {
        println("Removing 'RT' instances")

        collection = collection.foreach(tweet => tweet.tokens.filter(! _ == "RT"))

        return this
    }

    def removeMentions() : TweetCollection = {
        println("Removing mentions")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => ! """\"*@.*""".r.pattern.matcher(x).matches))
        return this
    }

    def removeHashtags() : TweetCollection = {
        println("Removing hashtags")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => ! """#.*""".r.pattern.matcher(x).matches))
        return this
    }

    def removeURLs() : TweetCollection = {
        println("Removing URLs")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => ! """.*http.*""".r.pattern.matcher(x).matches))
        return this
    }

    def removePunctuation() : TweetCollection = {
        println("Removing punctiation")
        collection = collection.foreach(tweet => tweet.tokens.map(x => x.replaceAll("[^A-Za-z0-9@#]", ""))).filter(tweet => tweet.tokens.length > 0)
        return this
    }

    def toLowerCase() : TweetCollection = {
        println("Converting to lowercase")
        collection = collection.foreach(tweet => tweet.tokens.map(x => x.toLowerCase()))
        return this
    }

    def removeRegexMatches(regex: scala.util.matching.Regex) : TweetCollection = {
        println("Removing regex")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => ! regex.pattern.matcher(x).matches))
        return this
    }

    def removeRegexNonmatches(regex: scala.util.matching.Regex) : TweetCollection = {
        println("Removing regex")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => regex.pattern.matcher(x).matches))
        return this
    }

    def removeTokens(tokensToRemove: Array[String]) : TweetCollection = {
        println("Removing tokens: [" + tokensToRemove.mkString(", ") + "]")
        collection = collection.foreach(tweet => tweet.tokens.filter(x => ! tokensToRemove.contains(x)))
        return this
    }
}

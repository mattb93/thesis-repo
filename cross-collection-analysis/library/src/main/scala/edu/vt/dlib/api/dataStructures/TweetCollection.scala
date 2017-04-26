package edu.vt.dlib.api.dataStructures

import java.io.Serializable
import java.io.{File, FileWriter, BufferedWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.reflect.ClassTag

/*
 * Provides convenience methods to read tweet data from and write tweet data to the DLRL cluster.
 * 
 */
class TweetCollection[T <: Tweet: ClassTag](val collectionID: String, @transient val sc: SparkContext, @transient val sqlContext: SQLContext, var collection: RDD[T]) extends Serializable {
    
    import scala.collection.mutable.WrappedArray
    import sqlContext.implicits._
    import java.lang.IllegalStateException

    case class SerializableFunctionWrapper[SubT](val f: SubT => SubT)
    case class SerializableConditionWrapper[SubT](val f: SubT => Boolean)

    /*
     * Return the collection as an RDD of Tweet objects
     */
    def getCollection() : RDD[T] = {
        return collection
    }

    /*
     * Return the collection as an RDD of Strings representing the tweet text
     */
    def getPlainText(): RDD[String] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => tweet.tokens.mkString(" "))
    }

    /*
     * Return the collection as an RDD of (String, String) tuples containing (tweet id, tweet text)
     */
    def getPlainTextID() : RDD[(String, String)] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => (tweet.id, tweet.tokens.mkString(" ")))
    }

    /*
     * Return the collection as Array[String]. Each array contains the tokens contained in the tweet
     */
    def getTokenArrays(): RDD[Array[String]] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => tweet.tokens)
    }

    /*
     * Return the collection as an RDD of (String, Array[String]) tuples containing (tweet id, array of tokens)
     */
    def getTokenArraysID(): RDD[(String, Array[String])] = {
        return collection.filter(tweet => ! tweet.tokens.isEmpty).map(tweet => (tweet.id, tweet.tokens))
    }

    //.........................................................................
    //============================//
    // FILTER FUNCTIONS           //
    // Remove unnecessary content //
    //============================//

    def filter(function: T => Boolean): TweetCollection[T] = {
        val mapFunctionWrapper = SerializableConditionWrapper[T](function)
         
        collection = collection.filter(mapFunctionWrapper.f)

        return this
    }
    /*
     * Remove retweets from the collection entirely
     */
    def filterRetweets(): TweetCollection[T] = {
        println("Removing Retweets")

        collection = collection.filter(tweet => ! tweet.isRetweet)

        return this
    }


    def filterByID(id: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.id == id) == keepIf)

        return this
    }

    def filterByMention(mention: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.mentions.contains(mention)) == keepIf)

        return this
    }

    def filterByHashtag(hashtag: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.hashtags.contains(hashtag)) == keepIf)

        return this
    }

    def filterByUrl(url: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.urls.contains(url)) == keepIf)

        return this
    }

    def filterByTokens(filter: Array[String], requireAll: Boolean = false, keepIf: Boolean = true): TweetCollection[T] = {
        if(requireAll) {
            collection = collection.filter(tweet => tweet.tokens.union(filter) == filter)
        }
        else {
            collection = collection.filter(tweet => ! tweet.tokens.union(filter).isEmpty)
        }

        return this
    }

    def filterByPayloadKeyValue(key: String, value: Any, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.payload.apply(key) == value) == keepIf)

        return this
    }

    //.........................................................................
    //================================//
    // UTILITY FUNCTIONS              //
    // Other various useful functions //
    //================================//
    /*
     * Return a random sample of the tweets contained in this collection as an RDD of Tweets
     */
    def randomSample(withReplacement: Boolean, fraction: Double) : RDD[T] = {

        return collection.sample(withReplacement, fraction)
    }

    /*
     * Filter invalid tweets out of the collection
     */
    def sanitize() = {
        // No empty tweets
        collection.filter(tweet => tweet.tokens.length > 0)
    }

    
    def applyFunction(function: T => T) = {
        val mapFunctionWrapper = SerializableFunctionWrapper[T](function)
         
        collection = collection.map(mapFunctionWrapper.f)
    }

    def union(otherCollection: TweetCollection[T], filterDuplicates: Boolean = true) = {
        collection = collection.union(otherCollection.getCollection())
        if(filterDuplicates){
            collection = collection.distinct()
        }
    }

    def writeToLocalFile(path: String) = {
        // Write the results back to local disk using standard java io
        val file = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(file))

        for(tweet <- collection.collect()) {
            bufferedWriterCounts.write(tweet.toTSV())
        }

        bufferedWriterCounts.close()
    
    }
}

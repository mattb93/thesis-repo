package edu.vt.dlib.api.dataStructures

import java.io.Serializable
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/*
 * Provides convenience methods to read tweet data from and write tweet data to the DLRL cluster.
 * 
 */
class TweetCollection[T <: Tweet: ClassTag](val collectionID: String, @transient val sc: org.apache.spark.SparkContext, @transient val sqlContext: org.apache.spark.sql.SQLContext, var collection: RDD[T]) extends Serializable {
    
    import scala.collection.mutable.WrappedArray
    import sqlContext.implicits._
    import java.lang.IllegalStateException
    

    // http://alvinalexander.com/scala/how-to-control-scala-method-scope-object-private-package
    //var collection: RDD[T] = null;

    var rtTokensCleaned: Boolean = false;

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
/*
    //.........................................................................
    //==============================//
    // CLEANING FUNCTIONS           //
    // Clean the collection contents//
    //==============================//

    /*
     * Clean the stop words from the tweets using Spark's StopWordsRemover
     */
    def cleanStopWords() : TweetCollection[T] = {
        println("Removing Stop Words")

        //val remover = new StopWordsRemover()
        //val stopWords = remover.getStopWords
        //collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(!stopWords.contains(_))))
        return this
    }

    /*
     * clean the RT tokens that come at the beginning of Retweets
     */
    def cleanRTMarkers() : TweetCollection[T] = {
        println("Removing 'RT' instances")
        
        rtTokensCleaned = true
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(_ != "RT")))

        return this
    }

    /*
     * Remove mentions from the tweets
     */
    def cleanMentions() : TweetCollection[T] = {
        println("Removing mentions")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => ! """@[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)))
        return this
    }

    /*
     * Remove hashtags from the tweets
     */
    def cleanHashtags() : TweetCollection[T] = {
        println("Removing hashtags")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => ! """#[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)))
        return this
    }

    /*
     * Remove URLs from the tweets
     */
    def cleanURLs() : TweetCollection[T] = {
        println("Removing URLs")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => ! """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)))
        return this
    }

    /*
     * Remove punctuation from the tweets
     */
    def cleanPunctuation() : TweetCollection[T] = {
        println("Removing punctiation")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.map(x => x.replaceAll("[^A-Za-z0-9@#]", ""))))
        return this
    }

    /*
     * Remove all tokens which match the specified regular expression
     */
    def cleanRegexMatches(regex: scala.util.matching.Regex) : TweetCollection[T] = {
        println("Removing regex")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => ! regex.pattern.matcher(x).matches)))
        return this
    }

    /*
     * Remove all tokens which do not match the specified regular expression
     */
    def cleanRegexNonmatches(regex: scala.util.matching.Regex) : TweetCollection[T] = {
        println("Removing regex")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => regex.pattern.matcher(x).matches)))
        return this
    }

    /*
     * Remove all instances of the specified tokens from the collection
     */
    def cleanTokens(tokensToRemove: Array[String]) : TweetCollection[T] = {
        println("Removing tokens: [" + tokensToRemove.mkString(", ") + "]")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.filter(x => ! tokensToRemove.contains(x))))
        return this
    }
*/
    //.........................................................................
    //============================//
    // FILTER FUNCTIONS           //
    // Remove unnecessary content //
    //============================//

    /*
     * Remove retweets from the collection entirely
     */
    def filterRetweets(): TweetCollection[T] = {
        println("Removing Retweets")
        
        if(rtTokensCleaned) {
            throw new IllegalStateException("Can't remove retweets if RT Markers have already been cleaned")
        }
        collection = collection.filter(tweet => tweet.tokens.contains("RT"))

        return this
    }

    def filterByArchiveSource(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.archivesource == filter) == keepIf)

        return this
    }

    def filterByToUserId(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.to_user_id == filter) == keepIf)

        return this
    }

    def filterByFromUser(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.from_user == filter) == keepIf)

        return this
    }

    def filterByID(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.id == filter) == keepIf)

        return this
    }

    def filterByFromUserId(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.from_user_id == filter) == keepIf)

        return this
    }

    def filterByIsoLanguageCode(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.iso_language_code == filter) == keepIf)

        return this
    }

    def filterBySource(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.source == filter) == keepIf)

        return this
    }

    def filterByProfileImageURL(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.profile_image_url == filter) == keepIf)

        return this
    }

    def filterByGeoType(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.geo_type == filter) == keepIf)

        return this
    }

    def filterByGeoCoordinates0(filter: Double, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan){
            collection = collection.filter(tweet => tweet.geo_coordinates_0 > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.geo_coordinates_0 < filter)
        }

        return this
    }

    def filterByGeoCoordinates1(filter: Double, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan) {
            collection = collection.filter(tweet => tweet.geo_coordinates_1 > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.geo_coordinates_1 < filter)
        }

        return this
    }

    def filterByCreatedAt(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.created_at == filter) == keepIf)

        return this
    }

    def filterByTime(filter: Int, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan) {
            collection = collection.filter(tweet => tweet.time > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.time < filter)
        }

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
     * Turn all text lowercase
     *
    def toLowerCase() : TweetCollection[T] = {
        println("Converting to lowercase")
        collection = collection.map(tweet => tweet.setTokens(tweet.tokens.map(x => x.toLowerCase())))
        return this
    }
    */

    /*
     * Filter invalid tweets out of the collection
     */
    def sanitize(): TweetCollection[T] = {
        // No empty tweets
        collection.filter(tweet => tweet.tokens.length > 0)
        return this
    }

    case class SerializableFunctionWrapper[SubT](val f: SubT => SubT)

    def applyFunction(function: T => T): TweetCollection[T] = {
        val mapFunctionWrapper = SerializableFunctionWrapper[T](function)
         
        collection = collection.map(mapFunctionWrapper.f)

        return this
    }
}

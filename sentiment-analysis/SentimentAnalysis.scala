// 1. Gather self-labeled tweets (emojis)
// 2. Farm those tweets for commons words and/or frequent patterns
// 3. Expand collection of seed words by running frequent pattern mining on tweets that contain those frequent patterns

import edu.vt.dlib.api.io._

import org.apache.spark.rdd.RDD

// Configutation parameters
val initialNumSeedWords = 5 // initial number of words to be extracted from self-labeled tweets
val targetNumSeedWords = 10	// number of seed words to pull from all tweets
val numIterations = 10		// number of times to repeat searching for new seed words
val orientationSeed = ":(" // regular expression use to start search

val collectionNumber = "41"
val collectionID = "NewtownShooting"

// Get the collection and clean it
val collection: TweetCollection = new HDFSTweetCollection(collectionID, sc, sqlContext, collectionNumber)
val cleanCollection: TweetCollection = collection.removeStopWords().removeRTs().removeMentions().removeURLs().removePunctuation().toLowerCase()

// Find the initial set of self-labeled sentiment oriented tweets
var orientedTweets: RDD[String] = collection.getTextArrays().filter(tokens => tokens.contains(orientationSeed)).map(tokens => tokens.mkString(" "))

// Get the top initialNumSeedWords most frequent words. This will be oriented in whichever direction is desired by orientationSeed
var orientedTokens: Array[(String, Int)] = orientedTweets.flatMap(text => text.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(initialNumSeedWords)

// Run the process over a few times to get a solid representation
var i = 0
for(i <- 0 to numIterations) {
	// Filter for tweets that contain at least one of the already defined seed words
	orientedTweets = collection.getTextArrays().filter(tokens => ! tokens.union(orientedTokens).isEmpty).map(tokens => tokens.mkString(" "))

	// Search through the tweets for the top targetNumSeedWords most frequent words
	orientedTokens = orientedTweets.flatMap(text => text.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(targetNumSeedWords)
}

orientedTokens.foreach(println)
orientedTweets.take(10).foreach(println)

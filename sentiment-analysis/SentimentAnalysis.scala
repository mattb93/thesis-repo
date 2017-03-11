// 1. Gather self-labeled tweets (emojis)
// 2. Farm those tweets for commons words and/or frequent patterns
// 3. Expand collection of seed words by running frequent pattern mining on tweets that contain those frequent patterns

import edu.vt.dlib.api._

// Configutation parameters
val initialNumSeedWords = 5 // initial number of words to be extracted from self-labeled tweets
val targetNumSeedWords = 10	// number of seed words to pull from all tweets
val numIterations = 10		// number of times to repeat searching for new seed words
val orientationSeed = """:\(""".r // regular epression use to start search

val collectionNumber = 41
val colelctionID = "NewtownShooting"

// Get the collection and clean it
val collection: TweetCollection = new HDFSTweetCollection(collectionID, sc, sqlContext, collectionNumber)
val cleanCollection: TweetCollection = collection.removeStopWords().removeRTs().removeMentions().removeURLs().removePunctuation().toLowerCase()

// Find the initial set of self-labeled sentiment oriented tweets
val featureExtractor = new featureExtractor(cleanCollection)
val orientedTweets: RDD[String] = cleanCollection.extractRegexMatches(orientationSeed).map(entry => entry._1)

// Get the top initialNumSeedWords most frequent words. This will be oriented in whichever direction is desired by orientationSeed
val orientedTokens: Array[String] = orientedTweets.flatMap(text => text.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false)
                    .take(initialNumSeedWords)
                    .collect();

// Run the process over a few times to get a solid representation
for( var i <- 0 to numIterations) {
	// Filter for tweets that contain at least one of the already defined seed words
	orientedTweets = collection.getTextArrays().filter(tokens => ! tokens.union(orientedTokens).isEmpty()).map(tokens => tokens.mkString(" "))

	// Search through the tweets for the top targetNumSeedWords most frequent words
	orientedTokens = orientedTweets.flatMap(text => text.split(" "))
                .map(word => (word, 1))
                .reduceByKey(_ + _)
                .sortBy(_._2, false)
                .take(targetNumSeedWords)
                .collect();
}

orientedTokens.foreach(println)
orientedTweets.take(10).foreach(println)
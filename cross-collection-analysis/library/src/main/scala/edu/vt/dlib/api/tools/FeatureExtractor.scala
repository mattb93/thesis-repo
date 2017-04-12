package edu.vt.dlib.api.tools

import java.io.Serializable

class FeatureExtractor() extends Serializable{

    import org.apache.spark.rdd.RDD
    import edu.vt.dlib.api.dataStructures.TweetCollection
    import edu.vt.dlib.api.dataStructures.Tweet
    import java.io._

	// Create a dataframe to hold the collected features

	def extractMentions(collection: TweetCollection[_ <: Tweet]) : RDD[(String, String)] = {
        return collection.getCollection().map(tweet => (tweet.id, tweet.mentions.mkString(" ")))
	}

	def extractHashtags(collection: TweetCollection[_ <: Tweet]) : RDD[(String, String)] = {
        return collection.getCollection().map(tweet => (tweet.id, tweet.hashtags.mkString(" ")))
	}

	def extractURLs(collection: TweetCollection[_ <: Tweet]) : RDD[(String, String)] = {
        return collection.getCollection().map(tweet => (tweet.id, tweet.urls.mkString(" ")))
	}

	def extractRegexMatches(collection: TweetCollection[_ <: Tweet], regex: String) : RDD[(String, String)] = {
        return collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => regex.r.pattern.matcher(token).matches).mkString(" ")))
	}

	def extractToken(collection: TweetCollection[_ <: Tweet], tokenToMatch: String, preserveText: Boolean = true) : RDD[(String, String)] = {
        if(!preserveText) {
             return collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => token == tokenToMatch).mkString(" ")))
        }
	
		return collection.getCollection().filter(tweet => tweet.tokens.contains(tokenToMatch)).map(tweet => (tweet.id, tweet.tokens.mkString(" ")))
	}

    def writeFeaturesToLocalFile(path: String, features: RDD[(String, String)]) = {
        val resultFile = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        for(record <- features.collect()) {
            bufferedWriter.write(record._1 + "\t" + record._2 + "\n")
        }
        
        bufferedWriter.close()
    }
}

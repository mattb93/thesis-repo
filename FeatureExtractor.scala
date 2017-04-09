package edu.vt.dlib.api.tools

import java.io.Serializable

class FeatureExtractor() extends Serializable{

    import org.apache.spark.rdd.RDD
    import edu.vt.dlib.api.dataStructures.TweetCollection
    import java.io._

	// Create a dataframe to hold the collected features

	def extractMentions(collection: TweetCollection) : RDD[(String, String)] = {
        val filtered = collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => """@[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)))
        return filtered.filter(tweet => !tweet._2.isEmpty).map(tweet => (tweet._1, tweet._2.mkString(" ")))
	}

	def extractHashtags(collection: TweetCollection) : RDD[(String, String)] = {
        val filtered = collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => """#[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)))
        return filtered.filter(tweet => !tweet._2.isEmpty).map(tweet => (tweet._1, tweet._2.mkString(" ")))
	}

	def extractURLs(collection: TweetCollection) : RDD[(String, String)] = {
        val filtered = collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)))
        return filtered.filter(tweet => !tweet._2.isEmpty).map(tweet => (tweet._1, tweet._2.mkString(" ")))
	}

	def extractRegexMatches(collection: TweetCollection, regex: String) : RDD[(String, String)] = {
        val filtered = collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => regex.r.pattern.matcher(token).matches)))
        return filtered.filter(tweet => !tweet._2.isEmpty).map(tweet => (tweet._1, tweet._2.mkString(" ")))
	}

	def extractToken(collection: TweetCollection, tokenToMatch: String, preserveText: Boolean = true) : RDD[(String, String)] = {
        if(!preserveText) {
            val filtered = collection.getCollection().map(tweet => (tweet.id, tweet.tokens.filter(token => token == tokenToMatch)))
            return filtered.filter(tweet => !tweet._2.isEmpty).map(tweet => (tweet._1, tweet._2.mkString(" ")))
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

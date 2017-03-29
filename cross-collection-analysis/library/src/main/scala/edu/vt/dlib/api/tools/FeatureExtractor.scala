package edu.vt.dlib.api.tools

import java.io.Serializable

class FeatureExtractor() extends Serializable{

    import org.apache.spark.rdd.RDD
    import edu.vt.dlib.api.dataStructures.TweetCollection
    import java.io._

	// Create a dataframe to hold the collected features

	def extractMentions(collection: TweetCollection) : RDD[(String, String)] = {
		return collection.getPlainTextID().filter(tweet => """\"*@.*""".r.pattern.matcher(tweet._2).matches)
	}

	def extractHashtags(collection: TweetCollection) : RDD[(String, String)] = {
		return collection.getPlainTextID().filter(tweet => """#.*""".r.pattern.matcher(tweet._2).matches)
	}

	def extractURLs(collection: TweetCollection) : RDD[(String, String)] = {
		return collection.getPlainTextID().filter(tweet => """.*http.*""".r.pattern.matcher(tweet._2).matches)
	}

	def extractRegexMatches(collection: TweetCollection, regex: scala.util.matching.Regex) : RDD[(String, String)] = {
		return collection.getPlainTextID().filter(tweet => regex.pattern.matcher(tweet._2).matches)
	}

	def extractToken(collection: TweetCollection, token: String) : RDD[(String, String)] = {

		return collection.getPlainTextID().filter(tweet => tweet._2.split(" ").contains(token))
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

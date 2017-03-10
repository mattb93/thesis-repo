package edu.vt.dlib.api.tools

import edu.vt.dlib.api.io.TweetCollection

class FeatureExtractor(collection: TweetCollection) {

    import org.apache.spark.rdd.RDD

	// Create a dataframe to hold the collected features
	val features = collection.getPlainTextID()

	def extractMentions() : RDD[(String, String)] = {
		return features.filter(entry => """\"*@.*""".r.pattern.matcher(entry._2).matches)
	}

	def extractHashtags() : RDD[(String, String)] = {
		return features.filter(entry => """#.*""".r.pattern.matcher(entry._2).matches)
	}

	def extractURLs() : RDD[(String, String)] = {
		return features.filter(entry => """.*http.*""".r.pattern.matcher(entry._2).matches)
	}

	def extractRegexMatches(regex: scala.util.matching.Regex) : RDD[(String, String)] = {
		return features.filter(entry => regex.pattern.matcher(entry._2).matches)
	}
}

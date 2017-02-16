package edu.vt.dlib.api.tools

import edu.vt.dlib.api.io.TweetCollection

class FeatureExtractor(collection: TweetCollection) {

    import org.apache.spark.rdd.RDD

	// Create a dataframe to hold the collected features
	val features = collection.getTextArraysID()

	def extractMentions() : RDD[(String, Array[String])] = {
		return features.map(entry => (entry._1, entry._2.filter(x => """\"*@.*""".r.pattern.matcher(x).matches)))
	}

	def extractHashtags() : RDD[(String, Array[String])] = {
		return features.map(entry => (entry._1, entry._2.filter(x => """#.*""".r.pattern.matcher(x).matches)))
	}

	def extractURLs() : RDD[(String, Array[String])] = {
		return features.map(entry => (entry._1, entry._2.filter(x => """.*http.*""".r.pattern.matcher(x).matches)))
	}

	def extractRegexMatches(regex: scala.util.matching.Regex) : RDD[(String, Array[String])] = {
		return features.map(entry => (entry._1, entry._2.filter(x => regex.pattern.matcher(x).matches)))
	}
}

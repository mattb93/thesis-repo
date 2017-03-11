package edu.vt.dlib.api.tools

import edu.vt.dlib.api.io.TweetCollection

class FeatureExtractor(collection: TweetCollection) {

    import org.apache.spark.rdd.RDD
    
    import java.io._

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

    def extractToken(token: String) : RDD[(String, String)] = {
        return features.map(entry => (entry._1, entry._2.split(" "))).filter(entry => entry._2.contains(token)).map(entry => (entry._1, entry._2.mkString(" ")))
    }

    def writeToLocalFile(path: String, features: RDD[(String, String)]) = {
        val resultFile = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        for(record <- features.collect()) {
            bufferedWriter.write(record._1 + "\t" + record._2 + "\n")
        }
        
        bufferedWriter.close()
    }
}

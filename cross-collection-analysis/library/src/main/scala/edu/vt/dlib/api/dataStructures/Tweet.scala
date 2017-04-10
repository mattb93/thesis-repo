package edu.vt.dlib.api.dataStructures
import org.apache.spark.ml.feature.StopWordsRemover

abstract class Tweet(val text: String, var id: String = "") extends Serializable {

	//import scala.collection.mutable.Map
	
	/*
	 * Fields which all tweets can provide or generate
	 */
	var tokens:		Array[String] = text.split(" ")
	var hashtags:	Array[String] = tokens.filter(token => """#[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	var mentions:	Array[String] = tokens.filter(token => """@[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	var urls:		Array[String] = tokens.filter(token => """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)

	// Add a payload to hold other arbitrary data
	var payload:	Map[String, String] = scala.collection.immutable.Map[String, String]()


    def addToPayload(key: String, value: String): Tweet = {
        payload += key -> value
        return this
    }

    def cleanStopWords(): Tweet = {
    	// Shortcut to get Spark default stop words
    	val remover = new StopWordsRemover()
    	val stopWords = remover.getStopWords

    	tokens = tokens.filter(!stopWords.contains(_))

    	return this
    }

    def cleanRTMarker(): Tweet = {
    	tokens = tokens.filter(_ != "RT")
    	return this
    }

    def cleanMentions(): Tweet = {
    	tokens = tokens.filter(x => ! """@[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
    	return this
    }

    def cleanHashtags(): Tweet = {
    	tokens = tokens.filter(x => ! """#[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
    	return this
    }

    def cleanURLs(): Tweet = {
		tokens = tokens.filter(x => ! """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
		return this
    }

    def cleanPunctuation(): Tweet = {
		tokens = tokens.map(x => x.replaceAll("[^A-Za-z0-9@#]", ""))
		return this
    }

    def cleanRegexMatches(regex: scala.util.matching.Regex): Tweet = {
		tokens = tokens.filter(x => ! regex.pattern.matcher(x).matches)
		return this
    }

    def cleanRegexNonmatches(regex: scala.util.matching.Regex): Tweet = {
		tokens = tokens.filter(x => regex.pattern.matcher(x).matches)
		return this
    }

    def cleanTokens(tokensToRemove: Array[String]): Tweet = {
        tokens = tokens.filter(x => ! tokensToRemove.contains(x))
        return this
    }

    def toLowerCase(): Tweet = {
    	tokens = tokens.map(x => x.toLowerCase())
    	return this
    }

	override def toString(): String =   {
		return id + "\t" + tokens.mkString(" ")
	}

	def toStringVerbose(): String = {
		var result = "Tweet content:" +
			"\n\tarchivesource: " + archivesource +
			"\n\ttext: " + text +
			"\n\tto_user_id: " + to_user_id +
			"\n\tfrom_user: " + from_user +
			"\n\tid: " + id +
			"\n\tfrom_user_id: " + from_user_id +
			"\n\tiso_language_code: " + iso_language_code +
			"\n\tsource: " + source +
			"\n\tprofile_image_url: " + profile_image_url +
			"\n\tgeo_type: " + geo_type +
			"\n\tgeo_coordinates_0: " + geo_coordinates_0 +
			"\n\tgeo_coordinates_1: " + geo_coordinates_1 +
			"\n\tcreated_at: " + created_at +
			"\n\ttime: " + time +
			"\n\ttokens: " + tokens.mkString(" ") +
			"\n\tpayload:"

		for( (key, value) <- payload) {
			result = result + "\n\t\t(" + key + ", " + value + ")"
		}

		return result
	}

	def canEqual(a: Any) = a.isInstanceOf[Tweet]

  	override def equals(that: Any): Boolean =
    	that match {
      		case that: Tweet => that.canEqual(this) && this.id == that.id
      		case _ => false
   }
}


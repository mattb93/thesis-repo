package edu.vt.dlib.api.dataStructures
import org.apache.spark.ml.feature.StopWordsRemover

abstract class Tweet(val text: String, var id: String = "") extends Serializable {
	
	/*
	 * Fields which all tweets can provide or generate
	 */
	var tokens:		Array[String] = text.split(" ")
	var hashtags:	Array[String] = tokens.filter(token => """#[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	var mentions:	Array[String] = tokens.filter(token => """@[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	var urls:		Array[String] = tokens.filter(token => """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)

	var isRetweet: 	Boolean = tokens(0) == "RT" || tokens(0) == "\"RT"
	// Add a payload to hold other arbitrary data
	var payload:	Map[String, String] = scala.collection.immutable.Map[String, String]()


    def addToPayload(key: String, value: String) = {
        payload += key -> value
    }

    def cleanStopWords() {
    	// Shortcut to get Spark default stop words
    	val remover = new StopWordsRemover()
    	val stopWords = remover.getStopWords

    	tokens = tokens.filter(!stopWords.contains(_))
    }

    def cleanRTMarker() = {
    	tokens = tokens.filter(_ != "RT")
    }

    def cleanMentions() = {
    	tokens = tokens.filter(x => ! """@[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
    }

    def cleanHashtags() = {
    	tokens = tokens.filter(x => ! """#[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
    }

    def cleanURLs() = {
		tokens = tokens.filter(x => ! """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(x).matches)
    }

    def cleanPunctuation() = {
		tokens = tokens.map(x => x.replaceAll("[^A-Za-z0-9@#]", "")).filter(x => x.length > 0)
    }

    def cleanRegexMatches(regex: scala.util.matching.Regex) = {
		tokens = tokens.filter(x => ! regex.pattern.matcher(x).matches)
    }

    def cleanRegexNonmatches(regex: scala.util.matching.Regex) = {
		tokens = tokens.filter(x => regex.pattern.matcher(x).matches)
    }

    def cleanTokens(tokensToRemove: Array[String]) = {
        tokens = tokens.filter(x => ! tokensToRemove.contains(x))
    }

    def toLowerCase() = {
    	tokens = tokens.map(x => x.toLowerCase())
    }

	override def toString(): String =   {
		return id + "\t" + tokens.mkString(" ")
	}

	def toStringVerbose(): String = {
		var result = "Tweet content:" +
			"\n\toriginal text: " + text +
			"\n\tid: " + id +
			"\n\ttokens: [" + tokens.mkString(", ") + "]" +
            "\n\thashtags: " + hashtags.mkString(", ") + 
            "\n\tmentions: " + mentions.mkString(", ") +
            "\n\turls: " + urls.mkString(", ")
        
        if(payload.isEmpty == false) {

            result = result + "\n\tpayload contents:"

	    	for( (key, value) <- payload) {
			    result = result + "\n\t\t(" + key + ", " + value + ")"
		    }
        }

		return result
	}

	def canEqual(a: Any) = a.isInstanceOf[_ <: Tweet]

  	override def equals(that: Any): Boolean =
    	that match {
      		case that: Tweet => that.canEqual(this) && this.id == that.id
      		case _ => false
   }
}


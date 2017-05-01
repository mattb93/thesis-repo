package edu.vt.dlib.api.dataStructures
import org.apache.spark.ml.feature.StopWordsRemover

class SimpleTweet(id: String, text: String) extends Tweet(id, text){
	
	/*
	 * Fields which all tweets can provide or generate
	 */
	tokens = text.split(" ")
	hashtags = tokens.filter(token => """#[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	mentions = tokens.filter(token => """@[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)
	urls = tokens.filter(token => """http://t\.co/[a-zA-Z0-9]+""".r.pattern.matcher(token).matches)

	isRetweet = tokens(0) == "RT" || tokens(0) == "\"RT"
	// Add a payload to hold other arbitrary data
	payload = scala.collection.immutable.Map[String, Any]()


    def addToPayload(key: String, value: Any) = {
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
        tokens = tokens.filter(_ != "rt")
    }

    def cleanMentions() = {
    	tokens = tokens.filter(x => ! x.startsWith("@"))
    }

    def cleanHashtags() = {
    	tokens = tokens.filter(x => ! x.startsWith("#"))
    }

    def cleanURLs() = {
		tokens = tokens.filter(x => ! """http://t\.co/.*""".r.pattern.matcher(x).matches)
        tokens = tokens.filter(x => ! """https://t\.co/.*""".r.pattern.matcher(x).matches)
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
}


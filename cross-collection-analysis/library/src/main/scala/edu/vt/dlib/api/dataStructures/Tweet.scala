package edu.vt.dlib.api.dataStructures

abstract class Tweet(val text: String, val id: String) extends Serializable {
	var tokens:		Array[String] = null
	var hashtags:	Array[String] = null
	var mentions:	Array[String] = null
	var urls:		Array[String] = null
	var isRetweet: 	Boolean = false
	var payload:	Map[String, Any] = null

	def cleanRTMarker()
	def cleanMentions()
	def cleanHashtags()
	def cleanURLs()
	def cleanPunctuation()
	def cleanRegexMatches(regex: scala.util.matching.Regex)
	def cleanRegexNonmatches(regex: scala.util.matching.Regex)
	def cleanTokens(tokens: Array[String])
	def toLowerCase()
    def addToPayload(key: String, value: Any)

	def toStringVerbose(): String

	def toTSV(): String = {
		var result = id + "\t" + 
			tokens.mkString(" ") + "\t" + 
			text + "\t" + 
			hashtags.mkString(" ") + "\t" + 
			mentions.mkString(" ") + "\t" + 
			urls.mkString(" ")

		for( (key, value) <- payload) {
		    result = result + "\t" + value
	    }

	    return result
	}

	override def toString(): String =   {
		return id + "\t" + tokens.mkString(" ")
	}

	def canEqual(a: Any) = a.isInstanceOf[Tweet]

  	override def equals(that: Any): Boolean =
    	that match {
      		case that: Tweet => that.canEqual(this) && this.id == that.id
      		case _ => false
   }
}

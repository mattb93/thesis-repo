abstract class Tweet() extends Serializable {
	var tokens:		Array[String]
	var hashtags:	Array[String]
	var mentions:	Array[String]
	var urls:		Array[String]
	var isRetweet: 	Boolean
	var payload:	Map[String, Any]

	def cleanRTMarker()
	def cleanMentions()
	def cleanHashtags()
	def cleanURLs()
	def cleanPunctuation()
	def cleanRegexMatches(scala.util.matching.Regex)
	def cleanRegexNonmatches(scala.util.matching.Regex)
	def cleanTokens(Array[String])
	def toLowerCase()

	def toStringVerbose(): String

	override def toString(): String =   {
		return id + "\t" + tokens.mkString(" ")
	}

	def canEqual(a: Any) = a.isInstanceOf[_ <: Tweet]

  	override def equals(that: Any): Boolean =
    	that match {
      		case that: Tweet => that.canEqual(this) && this.id == that.id
      		case _ => false
   }
}
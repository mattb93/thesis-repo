package edu.vt.dlib.api.dataStructures

abstract class Tweet() extends Serializable {

	//import scala.collection.mutable.Map
	
	/*
	 * Define fields for all of the data in an avro file
	 */
	var archivesource: 		String	= ""
	var text: 				String	= ""
	var to_user_id: 		String	= ""
	var from_user: 			String	= ""
	var id: 				String	= ""
	var from_user_id:		String	= ""
	var iso_language_code: 	String	= ""
	var source: 			String	= ""
	var profile_image_url: 	String	= ""
	var geo_type: 			String	= ""
	var geo_coordinates_0: 	Double	= -1
	var geo_coordinates_1: 	Double	= -1
	var created_at: 		String	= ""
	var time:				Int		= -1

	/*
	 * Field for tokenized tweet text. The text field (defined above) will
	 * the original text. Modified text will be held here.
	 *
	 * Also define a (key, value) payload to hold other arbitrary fields generated later.
	 */
	var tokens:		Array[String] = new Array[String](0)
	var payload:	Map[String, String] = scala.collection.immutable.Map[String, String]()

    def setTokens(newTokens: Array[String]): Tweet = {
        tokens = newTokens
        return this
    }

    def addToPayload(key: String, value: String): Tweet = {
        payload += key -> value
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
}


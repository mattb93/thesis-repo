package edu.vt.dlib.api.dataStructures

abstract class Tweet() extends Serializable {

	//import scala.collection.mutable.Map
	
	/*
	 * Define fields for all of the data in an avro file
	 */
	var archiveSource: 		String	= null
	var text: 				String	= null
	var to_user_id: 		String	= null
	var from_user: 			String	= null
	var id: 				String	= null
	var from_user_id:		String	= null
	var iso_language_code: 	String	= null
	var source: 			String	= null
	var profile_image_url: 	String	= null
	var geo_type: 			String	= null
	var geo_coordinates_0: 	Double	= null
	var geo_coordinates_1: 	Double	= null
	var created_at: 		String	= null
	var time:				Int		= null

	/*
	 * Field for tokenized tweet text. The text field (defined above) will
	 * the original text. Modified text will be held here.
	 *
	 * Also define a (key, value) payload to hold other arbitrary fields generated later.
	 */
	var tokens:		Array[String] = null
	val payload:	Map[String, String] = Map()

	override def toString(): String =   {
		return id + "\t" + tokens.mkString(" ")
	}

	def toStringVerbose(): String = {
		val result = "Tweet content:" +
			"\n\tarchiveSource: " + archiveSource +
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
			result = result + "\n\t\t(" + key + ", " + value)
		}

		return result
	}
}


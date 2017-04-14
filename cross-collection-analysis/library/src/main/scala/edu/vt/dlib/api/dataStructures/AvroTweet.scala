package edu.vt.dlib.api.dataStructures

import org.apache.avro.generic.GenericRecord

class AvroTweet(dataRecord: GenericRecord) extends SimpleTweet(dataRecord.get("id").toString, dataRecord.get("text").toString) {
	
	var archivesource = dataRecord.get("archivesource").toString
	var to_user_id = dataRecord.get("to_user_id").toString
	var from_user = dataRecord.get("from_user").toString
	var from_user_id = dataRecord.get("from_user_id").toString
	var iso_language_code = dataRecord.get("iso_language_code").toString
	var source = dataRecord.get("source").toString
	var profile_image_url = dataRecord.get("profile_image_url").toString
	var geo_type = dataRecord.get("geo_type").toString
	var geo_coordinates_0 = dataRecord.get("geo_coordinates_0").toString.toDouble
	var geo_coordinates_1 = dataRecord.get("geo_coordinates_1").toString.toDouble
	var created_at = dataRecord.get("created_at").toString
	var time = dataRecord.get("time").toString.toInt

	override def toStringVerbose(): String = {
		var result = super.toStringVerbose()

		result = result + "\n\tAvro content:" +
			"\n\t\tarchivesource: " + archivesource +
			"\n\t\tto_user_id: " + to_user_id +
			"\n\t\tfrom_user: " + from_user +
			"\n\t\tfrom_user_id: " + from_user_id +
			"\n\t\tiso_language_code: " + iso_language_code +
			"\n\t\tsource: " + source +
			"\n\t\tprofile_image_url: " + profile_image_url +
			"\n\t\tgeo_type: " + geo_type +
			"\n\t\tgeo_coordinates_0: " + geo_coordinates_0 +
			"\n\t\tgeo_coordinates_1: " + geo_coordinates_1 +
			"\n\t\tcreated_at: " + created_at +
			"\n\t\ttime: " + time

			return result
	}
}

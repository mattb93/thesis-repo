package edu.vt.dlib.api.dataStructures

import org.apache.avro.generic.GenericRecord

class AvroTweet(dataRecord: GenericRecord) extends Tweet {
	archiveSource = dataRecord.get("archiveSource").toString
	text = dataRecord.get("text").toString
	to_user_id = dataRecord.get("to_user_id").toString
	from_user = dataRecord.get("from_user").toString
	id = dataRecord.get("id").toString
	from_user_id = dataRecord.get("from_user_id").toString
	iso_language_code = dataRecord.get("iso_language_code").toString
	source = dataRecord.get("source").toString
	profile_image_url = dataRecord.get("profile_image_url").toString
	geo_type = dataRecord.get("geo_type").toString
	geo_coordinates_0 = dataRecord.get("geo_coordinates_0").toString.toDouble
	geo_coordinates_1 = dataRecord.get("geo_coordinates_1").toString.toDouble
	created_at = dataRecord.get("created_at").toString
	time = dataRecord.get("time").toString.toInt

	tokens = text.split(" ")
}

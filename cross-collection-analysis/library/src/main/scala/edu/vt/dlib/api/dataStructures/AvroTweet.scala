package edu.vt.dlib.api.dataStructures

import org.apache.avro.generic.GenericRecord

class AvroTweet(dataRecord: GenericRecord) extends Tweet(dataRecord.get("text").toString, dataRecord.get("id").toString) {
	
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

}

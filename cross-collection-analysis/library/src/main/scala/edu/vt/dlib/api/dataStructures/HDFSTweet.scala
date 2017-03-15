package edu.vt.dlib.api.dataStructures

import org.apache.avro.generic.GenericRecord

class HDFSTweet(dataRecord: GenericRecord) extends Tweet {
	var archiveSource: 		String	= dataRecord.get("archiveSource")
	var text: 				String	= dataRecord.get("text")
	var to_user_id: 		String	= dataRecord.get("to_user_id")
	var from_user: 			String	= dataRecord.get("from_user")
	var id: 				String	= dataRecord.get("id")
	var from_user_id:		String	= dataRecord.get("from_user_id")
	var iso_language_code: 	String	= dataRecord.get("iso_language_code")
	var source: 			String	= dataRecord.get("source")
	var profile_image_url: 	String	= dataRecord.get("profile_image_url")
	var geo_type: 			String	= dataRecord.get("geo_type")
	var geo_coordinates_0: 	Double	= dataRecord.get("geo_coordinates_0")
	var geo_coordinates_1: 	Double	= dataRecord.get("geo_coordinates_1")
	var created_at: 		String	= dataRecord.get("created_at")
	var time:				Int		= dataRecord.get("time")

	var tokens = text.split(" ")
}
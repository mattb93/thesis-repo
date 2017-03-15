package edu.vt.dlib.api.dataStructures

class SVTweet(line: String, config: SVConfig = new SVCongig()) extends Tweet {

	var columns = line.split(config.separator)

	var archiveSource: 		String	= columns(config.archiveSource)
	var text: 				String	= columns(config.text)
	var to_user_id: 		String	= columns(config.to_user_id)
	var from_user: 			String	= columns(config.from_user)
	var id: 				String	= columns(config.id)
	var from_user_id:		String	= columns(config.from_user_id)
	var iso_language_code: 	String	= columns(config.iso_language_code)
	var source: 			String	= columns(config.source)
	var profile_image_url: 	String	= columns(config.profile_image_url)
	var geo_type: 			String	= columns(config.geo_type)
	var geo_coordinates_0: 	Double	= columns(config.geo_coordinates_0)
	var geo_coordinates_1: 	Double	= columns(config.geo_coordinates_1)
	var created_at: 		String	= columns(config.created_at)
	var time:				Int		= columns(config.time)

	var tokens = text.split(" ")

	for( (name, column) <- config.otherColumns) {
		payload += (name, columns.column)
	}
}
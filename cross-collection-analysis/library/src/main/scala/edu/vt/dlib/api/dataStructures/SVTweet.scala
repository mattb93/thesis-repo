package edu.vt.dlib.api.dataStructures

class SVTweet(line: String, config: SVConfig = new SVConfig()) extends Tweet {

	var columns = line.split(config.separator)

	archiveSource       = columns(config.archiveSource)
	text                = columns(config.text)
	to_user_id          = columns(config.to_user_id)
	from_user           = columns(config.from_user)
	id                  = columns(config.id)
	from_user_id        = columns(config.from_user_id)
	iso_language_code   = columns(config.iso_language_code)
	source              = columns(config.source)
	profile_image_url   = columns(config.profile_image_url)
	geo_type            = columns(config.geo_type)
	geo_coordinates_0   = columns(config.geo_coordinates_0).toDouble
	geo_coordinates_1   = columns(config.geo_coordinates_1).toDouble
	created_at          = columns(config.created_at)
	time                = columns(config.time).toInt

	tokens = text.split(" ")

	for( (name, column) <- config.otherColumns) {
		payload += name -> columns(column)
	}
}

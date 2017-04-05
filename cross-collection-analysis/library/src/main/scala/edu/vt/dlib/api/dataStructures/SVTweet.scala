package edu.vt.dlib.api.dataStructures

class SVTweet(line: String, config: SVConfig = new SVConfig()) extends Tweet {

    import java.lang.IllegalStateException

	var columns = line.split(config.separator)

    if(config.archivesource >= 0) {
        archivesource = columns(config.archivesource)
    }
    
    if(config.text >= 0) {
        text = columns(config.text)
    }
    else {
        throw new IllegalStateException("Must specify a text column other than -1")
    }
    
    if(config.to_user_id >= 0) {
        to_user_id = columns(config.to_user_id)
    }

    if(config.from_user >= 0) {
        from_user = columns(config.from_user)
    }

    if(config.id >= 0) {
        id = columns(config.id)
    }

    if(config.from_user_id >= 0) {
        from_user_id = columns(config.from_user_id)
    }

    if(config.iso_language_code >= 0) {
        iso_language_code = columns(config.iso_language_code)
    }

    if(config.source >= 0) {
        source = columns(config.source)
    }

    if(config.profile_image_url >= 0) {
        profile_image_url = columns(config.profile_image_url)
    }

    if(config.geo_type >= 0) {
        geo_type = columns(config.geo_type)
    }

    if(config.geo_coordinates_0 >= 0) {
        geo_coordinates_0 = columns(config.geo_coordinates_0).toDouble
    }

    if(config.geo_coordinates_1 >= 0) {
        geo_coordinates_1 = columns(config.geo_coordinates_1).toDouble
    }

    if(config.created_at >= 0) {
        created_at = columns(config.created_at)
    }

    if(config.time >= 0) {
        time = columns(config.time).toInt
    }

	tokens = text.split(" ")

	for( (name, column) <- config.otherColumns) {
		payload += name -> columns(column)
	}

}

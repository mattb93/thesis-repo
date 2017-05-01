package edu.vt.dlib.api.dataStructures

class SVTweet(line: String, config: SVConfig = new SVConfig()) extends SimpleTweet(line.split(config.separator)(config.id), line.split(config.separator)(config.text)) {

	var columns: Array[String] = line.split(config.separator)

	for( (name, column) <- config.otherColumns) {
		payload += name -> columns(column)
	}

}

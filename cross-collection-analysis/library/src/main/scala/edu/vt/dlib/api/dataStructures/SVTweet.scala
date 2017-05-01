package edu.vt.dlib.api.dataStructures

class SVTweet(line: String, config: SVConfig = new SVConfig()) extends SimpleTweet(line.split(config.separator)(config.text), line.split(config.separator)(config.id)) {

	var columns: Array[String] = line.split(config.separator)

	for( (name, column) <- config.otherColumns) {
		payload += name -> columns(column)
	}

}

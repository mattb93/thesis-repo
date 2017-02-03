package edu.vt.dlib.api

import edu.vt.dlip.api.collectionReader

class scriptRunner(val collectionNumbers = Array[String]) {
	
	var collectionWriter = new collectionWriter()

	for( collectionNumber <- collectionNumbers) {
		
		var collection = new collectionReader(collectionNumber)

		:load script.scala
	}
}
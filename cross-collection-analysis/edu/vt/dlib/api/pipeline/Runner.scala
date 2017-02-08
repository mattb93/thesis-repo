package edu.vt.dlib.api.pipeline

import edu.vt.dlib.api.pipeline.runnable
import edu.vt.dlib.api.io.tweetCollection

class Runner(val collectionNumbers : Array[String], val r: Runnable) {
	
	val collection: tweetCollection

	def run() {
		for( collectionNumber <- collectionNumbers) {
			
			collection = new tweetCollection(collectionNumber)
			r.run(collection)
		}
	}
}

package edu.vt.dlib.api.pipeline

trait Runnable {
	def run(collection: edu.vt.dlib.api.dataStructures.TweetCollection[edu.vt.dlib.api.dataStructures.Tweet])
}

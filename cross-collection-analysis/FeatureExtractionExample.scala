import edu.vt.dlib.api.pipeline.Runnable


/*
 * Proof of concept for feature extraction tool. Uses the API's feature extraction tool
 * to extract some information from the tweet collections
 */
class FeatureExtractorExample() extends Runnable {
	import edu.vt.dlib.api.tools.FeatureExtractor
	import edu.vt.dlib.api.io.DataWriter

	def run(collection: TweetCollection) {
		println("Processiong collection number " + collection.collectionId)

		collection.removeStopWords().removeRTs().toLowerCase()

		val featureExtractor = new FeatureExtractor(collection)

		val mentions = featureExtractor.extractMentions()
		val hashtags = featureExtractor.extractHashtags()
		val urls = featureExtractor.extractURLs()
		val positive = featureExtractor.extractRegexMatches(""":)""".r)

		val dataWriter = new DataWriter()

		dataWriter.writeToFile(mentions, "testResuls/" + collection.collectionId + "_mentions")
		dataWriter.writeToFile(mentions, "testResuls/" + collection.collectionId + "_hashtags")
		dataWriter.writeToFile(mentions, "testResuls/" + collection.collectionId + "_urls")
		dataWriter.writeToFile(mentions, "testResuls/" + collection.collectionId + "_positives")
	}
}


// Define collections
//val collections = Array("41", "45", "128", "145", "157", "443")
val collections = Array("41")

// Create a new runner with the collection numbers and a word counter to run
val runner = new Runner(sc, sqlContext)

runner.run(collections, new FeatureExtractorExample())
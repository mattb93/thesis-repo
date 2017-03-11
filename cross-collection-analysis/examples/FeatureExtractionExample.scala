import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.HDFSRunner

/*
 * Proof of concept for feature extraction tool. Uses the API's feature extraction tool
 * to extract some information from the tweet collections
 */
class FeatureExtractorExample() extends Runnable {
	import edu.vt.dlib.api.tools.FeatureExtractor
    import edu.vt.dlib.api.io.TweetCollection
	import edu.vt.dlib.api.io.DataWriter

	def run(collection: TweetCollection) {
		println("Processiong collection: " + collection.collectionID)

		collection.removeStopWords().removeRTs().toLowerCase()

		val featureExtractor = new FeatureExtractor(collection)

        val mentions = featureExtractor.extractMentions()
		val hashtags = featureExtractor.extractHashtags()
		val urls = featureExtractor.extractURLs()
		val negative = featureExtractor.extractToken(":(")

		val dataWriter = new DataWriter()

		featureExtractor.writeToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_mentions", mentions)
		featureExtractor.writeToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_hashtags", hashtags)
		featureExtractor.writeToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_urls", urls)
		featureExtractor.writeToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_positives", negative)
	}
}


// Define collections
//val collections = Array("41", "45", "128", "145", "157", "443")
val collections = Array("41")

// Create a new runner with the collection numbers and a word counter to run
val runner = new HDFSRunner(sc, sqlContext)

runner.run(new FeatureExtractorExample(), collections)

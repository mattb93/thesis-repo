import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.AvroRunner

/*
 * Proof of concept for feature extraction tool. Uses the API's feature extraction tool
 * to extract some information from the tweet collections
 */
class FeatureExtractorExample() extends Runnable {
	import edu.vt.dlib.api.tools.FeatureExtractor
    import edu.vt.dlib.api.dataStructures.TweetCollection

	def run(collection: TweetCollection) {
		println("Processiong collection: " + collection.collectionID)

		collection.cleanStopWords().cleanRTMarkers().toLowerCase()

		val featureExtractor = new FeatureExtractor()

        val mentions = featureExtractor.extractMentions(collection)
		val hashtags = featureExtractor.extractHashtags(collection)
		val urls = featureExtractor.extractURLs(collection)
		val negative = featureExtractor.extractToken(collection, ":(", false)


		featureExtractor.writeFeaturesToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_mentions", mentions)
		featureExtractor.writeFeaturesToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_hashtags", hashtags)
		featureExtractor.writeFeaturesToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_urls", urls)
		featureExtractor.writeFeaturesToLocalFile("results/FeatureExtractionExample/" + collection.collectionID + "_negatives", negative)
	}
}


// Define collections
//val collections = Array("41", "45", "128", "145", "157", "443")
val collections = Array("41")

// Create a new runner with the collection numbers and a word counter to run
val runner = new AvroRunner(sc, sqlContext)

runner.run(new FeatureExtractorExample(), collections)

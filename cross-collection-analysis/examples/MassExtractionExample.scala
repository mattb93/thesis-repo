import edu.vt.dlib.api.dataStructures.TweetCollection
import edu.vt.dlib.api.tools.WordCounter
import edu.vt.dlib.api.tools.FeatureExtractor
import edu.vt.dlib.api.tools.LDAWrapper
import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.SVRunner
import edu.vt.dlib.api.dataStructures.SVConfig

class MassExtractionExample() extends Runnable {
    	

	def run(collection: TweetCollection) = {

		collection.cleanStopWords().cleanRTMarkers().toLowerCase().cleanPunctuation()

		val featureExtractor = new FeatureExtractor()

		val mentions = featureExtractor.extractMentions(collection)
		featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_mentions", mentions)

		val hashtags = featureExtractor.extractHashtags(collection)
		featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_mentions", mentions)

		val urls = featureExtractor.extractURLs(collection)
		featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_mentions", mentions)


		val counter = new WordCounter()
        val counts = counter.count(collection)
        counter.writeCountsToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_counts", counts)

        val ldaWrapper = new LDAWrapper()
        val topics = ldaWrapper.analyze(collection)
        ldaWrapper.writeTopicsToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_topics", topics)
	}
}


//val fileNames = Array("HurricaneMatthew/Dataset_z_887_200026_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_888_164612_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_889_172793_tweets.csv",
//			"HurricaneMatthew/Dataset_z_890_151648_tweets.csv")

val fileNames = Array("trails/AT_0224.txt", "trails/CDT_0224.txt", "trails/PCT_0224.txt")

val runner = new SVRunner(sc, sqlContext)

val config = new SVConfig()
config.setTextIDOnly()
config.separator = "\t"

runner.run(new MassExtractionExample(), fileNames, config)

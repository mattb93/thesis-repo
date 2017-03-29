import edu.vt.dlib.api.io.TweetCollection
import edu.vt.dlib.api.io.DataWriter
import edu.vt.dlib.api.tools.WordCounter
import edu.vt.dlib.api.tools.FeatureExtractor
import edu.vt.dlib.api.tools.LDAWrapper
import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.SVRunner


class MassExtractionExample() extends Runnable {
    	

	def run(collection: TweetCollection) = {

		collection.removeRTs().toLowerCase()

		val featureExtractor = new FeatureExtractor(collection)

		//val mentions = featureExtractor.extractMentions().map(tweet => (tweet._1, tweet._2.split(" ")))
		//val hashtags = featureExtractor.extractHashtags().map(tweet => (tweet._1, tweet._2.split(" ")))
		//val urls = featureExtractor.extractURLs().map(tweet => (tweet._1, tweet._2.split(" ")))
		//val positive = featureExtractor.extractRegexMatches(""":\)""".r)

        val dataWriter = new DataWriter()
		//dataWriter.writeToFile(mentions, "results/MassExtractionExample/" + collection.collectionID + "_mentions")
		//dataWriter.writeToFile(hashtags, "results/MassExtractionExample/" + collection.collectionID + "_hashtags")
		//dataWriter.writeToFile(urls, "results/MassExtractionExample/" + collection.collectionID + "_urls")
		//dataWriter.writeToFile(positive, "results/MassExtractionExample/" + collection.collectionID + "_positives")


		val counter = new WordCounter()
        val counts = counter.count(collection.removeStopWords().removeRTs().toLowerCase())
        counter.writeToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_counts", counts)

        val ldaWrapper = new LDAWrapper()
        val topics = ldaWrapper.analyze(collection)
        ldaWrapper.writeToLocalFile("results/MassExtractionExample/" + collection.collectionID + "_topics", topics)
	}
}


//val fileNames = Array("HurricaneMatthew/Dataset_z_887_200026_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_888_164612_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_889_172793_tweets.csv",
//			"HurricaneMatthew/Dataset_z_890_151648_tweets.csv")

val fileNames = Array("trails/AT_0224.txt", "trails/CDT_0224.txt", "trails/PCT_0224.txt")

val runner = new SVRunner(sc, sqlContext)
runner.run(new MassExtractionExample(), fileNames, "\t", 1, 0)

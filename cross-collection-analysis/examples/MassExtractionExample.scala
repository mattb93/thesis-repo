import edu.vt.dlib.api.io.TweetCollection

import edu.vt.dlib.api.tools.WordCounter
import edu.vt.dlib.api.tools.FeatureExtractor
import edu.vt.dlib.api.tools.LDAWrapper

class MassExtractionExample() extends Runnable {
	
	val dataWriter = new DataWriter()

	def run(collection: TweetCollection) {

		collection.removeStopWords().removeRTs().removePunctuation().toLowerCase()

		val featureExtractor = new FeatureExtractor(collection)

		val mentions = featureExtractor.extractMentions()
		val hashtags = featureExtractor.extractHashtags()
		val urls = featureExtractor.extractURLs()
		val positive = featureExtractor.extractRegexMatches(""":\)""".r)

		dataWriter.writeToFile(mentions, "results/MassExtractionExample/" + collection.collectionId + "_mentions")
		dataWriter.writeToFile(hashtags, "results/MassExtractionExample/" + collection.collectionId + "_hashtags")
		dataWriter.writeToFile(urls, "results/MassExtractionExample/" + collection.collectionId + "_urls")
		dataWriter.writeToFile(positive, "results/MassExtractionExample/" + collection.collectionId + "_positives")

		collection.removeMentions()

		val counter = new WordCounter()
        val counts = counter.count(collection.removeStopWords().removeRTs().toLowerCase()).collect()

        // Write the results back to local disk using standard java io
        val resultFile = new File("results/MassExtractionExample/" + collection.collectionID)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
	}
}


val fileNames = Array("HurricaneMatthew/Dataset_z_887_200026_tweets.csv", 
			"HurricaneMatthew/Dataset_z_888_164612_tweets.csv", 
			"HurricaneMatthew/Dataset_z_889_172793_tweets.csv"
			"HurricaneMatthew/Dataset_z_890_151648_tweets.csv")

val runner = new SVRunner(sc, sqlContext)
runner.run(new MassExtractionExample(), fileNames, ", ", 1, 4)
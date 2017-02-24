import edu.vt.dlib.api.io.TweetCollection
import edu.vt.dlib.api.io.DataWriter
import edu.vt.dlib.api.tools.WordCounter
import edu.vt.dlib.api.tools.FeatureExtractor
import edu.vt.dlib.api.tools.LDAWrapper
import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.SVRunner


class MassExtractionExample() extends Runnable {
    
    import java.io._	
	val dataWriter = new DataWriter()

	def run(collection: TweetCollection) = {

		collection.removeStopWords().removeRTs().removePunctuation().toLowerCase()
/*
		val featureExtractor = new FeatureExtractor(collection)

		val mentions = featureExtractor.extractMentions()
		val hashtags = featureExtractor.extractHashtags()
		val urls = featureExtractor.extractURLs()
		//val positive = featureExtractor.extractRegexMatches(""":\)""".r)

		dataWriter.writeToFile(mentions, "results/MassExtractionExample/" + collection.collectionID + "_mentions")
		dataWriter.writeToFile(hashtags, "results/MassExtractionExample/" + collection.collectionID + "_hashtags")
		dataWriter.writeToFile(urls, "results/MassExtractionExample/" + collection.collectionID + "_urls")
		//dataWriter.writeToFile(positive, "results/MassExtractionExample/" + collection.collectionID + "_positives")

		collection.removeMentions()

		val counter = new WordCounter()
        val counts = counter.count(collection.removeStopWords().removeRTs().toLowerCase()).collect()

        // Write the results back to local disk using standard java io
        val countFile = new File("results/MassExtractionExample/" + collection.collectionID + "_counts")
        val bufferedWriterCounts = new BufferedWriter(new FileWriter(countFile))
        println("Writing counts to local file")
        for(count <- counts) {
            bufferedWriterCounts.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriterCounts.close()
*/
        val ldaWrapper = new LDAWrapper()
        val topics = ldaWrapper.analyze(collection)

        val topicFile = new File("results/MassExtractionExample/" + collection.collectionID + "_topics")
        val bufferedWriterTopics = new BufferedWriter(new FileWriter(topicFile))
        println("Writing topics to local file")
        var topicNum = 1;
        topics.foreach{ case(terms, weights) =>
            bufferedWriterTopics.write("Topic #" + topicNum + "\n")
            topicNum = topicNum + 1
            terms.zip(weights).foreach{ case(term, weight) =>
                bufferedWriterTopics.write(term + " : " + weight + "\n")
            }
            bufferedWriterTopics.write("\n")
        }

        bufferedWriterTopics.close()
	}
}


//val fileNames = Array("HurricaneMatthew/Dataset_z_887_200026_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_888_164612_tweets.csv", 
//			"HurricaneMatthew/Dataset_z_889_172793_tweets.csv",
//			"HurricaneMatthew/Dataset_z_890_151648_tweets.csv")

val fileNames = Array("trails/AT_0220.tsv")

val runner = new SVRunner(sc, sqlContext)
runner.run(new MassExtractionExample(), fileNames, "\t", 1, 4)

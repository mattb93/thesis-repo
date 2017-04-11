import edu.vt.dlib.api.dataStructures._
import edu.vt.dlib.api.tools.WordCounter
import edu.vt.dlib.api.tools.FeatureExtractor
import edu.vt.dlib.api.tools.LDAWrapper

/*
val fileNames = Array("HurricaneMatthew/Dataset_z_887_200026_tweets.csv", 
			"HurricaneMatthew/Dataset_z_888_164612_tweets.csv", 
			"HurricaneMatthew/Dataset_z_889_172793_tweets.csv",
			"HurricaneMatthew/Dataset_z_890_151648_tweets.csv")
*/

val fileNames = Array("trails/AT_0224.txt", "trails/CDT_0224.txt", "trails/PCT_0224.txt")

def cleaning(tweet: AvroTweet): AvroTweet = {
    tweet.cleanPunctuation()
    tweet.cleanStopWords()
    tweet.cleanRTMarker()
    tweet.toLowerCase()

    return tweet
}

val config = new SVConfig()
config.separator = "\t"

val factory = new TweetCollectionFactory(sc, sqlContext)
val wordCounter = new WordCounter()
val ldaWrapper = new LDAWrapper()
val featureExtractor = new FeatureExtractor()

for( fileName <- fileNames) {
	var collection = factory.createFromSVFile("batch_" + fileName, fileName, config)
	collection.applyFunction(cleaning)

	val mentions = featureExtractor.extractMentions(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName + "_mentions", mentions)

	val hashtags = featureExtractor.extractHashtags(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName + "_mentions", hashtags)

	val urls = featureExtractor.extractURLs(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName + "_mentions", urls)

    val counts = counter.count(collection)
    counter.writeCountsToLocalFile("results/MassExtractionExample/" + fileName + "_counts", counts)

    collection.applyFunction(tweet => tweet.cleanURLs())
    val topics = ldaWrapper.analyze(collection)
    ldaWrapper.writeTopicsToLocalFile("results/MassExtractionExample/" + fileName + "_topics", topics)
}

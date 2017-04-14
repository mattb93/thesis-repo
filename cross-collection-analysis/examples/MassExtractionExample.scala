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

//val fileNames = Array("trails/AT_0224.txt", "trails/CDT_0224.txt", "trails/PCT_0224.txt")
val fileNames = Array("trails/AT0412.txt")

def cleaningPhase1(tweet: SVTweet): SVTweet = {
    tweet.cleanPunctuation()
    tweet.cleanRTMarker()
    tweet.toLowerCase()
    tweet.cleanStopWords()

    return tweet
}

def cleaningPhase2(tweet: SVTweet): SVTweet = {
    tweet.cleanURLs()

    return tweet
}

val config = new SVConfig()
config.id = 1
config.text = 12
config.numColumns = 21
config.separator = "\t"

val factory = new TweetCollectionFactory(sc, sqlContext)
val wordCounter = new WordCounter()
val ldaWrapper = new LDAWrapper()
ldaWrapper.numTopics = 7
ldaWrapper.termsToIgnore = Array("appalachian", "trail", "at", "atc")
val featureExtractor = new FeatureExtractor()

for( fileName <- fileNames) {
	var collection = factory.createFromSVFile("batch_" + fileName, fileName, config)
	collection.applyFunction(cleaningPhase1)

	val mentions = featureExtractor.extractMentions(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName.split("/").last + "_mentions", mentions)

	val hashtags = featureExtractor.extractHashtags(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName.split("/").last + "_mentions", hashtags)

	val urls = featureExtractor.extractURLs(collection)
	featureExtractor.writeFeaturesToLocalFile("results/MassExtractionExample/" + fileName.split("/").last + "_mentions", urls)

    val counts = wordCounter.count(collection)
    wordCounter.writeCountsToLocalFile("results/MassExtractionExample/" + fileName.split("/").last + "_counts", counts)

    collection.applyFunction(cleaningPhase2)
    val topics = ldaWrapper.analyze(collection)

    collection.getCollection.take(10).foreach(tweet => println(tweet.toStringVerbose()))
    ldaWrapper.writeTopicsToLocalFile("results/MassExtractionExample/" + fileName.split("/").last + "_topics", topics)
}

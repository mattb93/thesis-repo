import edu.vt.dlib.api.dataStructures._
import edu.vt.dlib.api.tools.WordCounter


// Define collections to be pulled from hbase.
val collectionNumbers = Array(41, 45, 128, 145, 157, 443)
//val collections = Array("41")

def cleaning(tweet: AvroTweet): AvroTweet = {
    tweet.cleanPunctuation()
    tweet.toLowerCase()
    tweet.cleanStopWords()
    tweet.cleanRTMarker()
    tweet.cleanURLs()

    return tweet
}

def filter(tweet: AvroTweet): Boolean = {
    return tweet.isRetweet
}

var wordCounter = new WordCounter()
val factory = new TweetCollectionFactory(sc, sqlContext)

for( collectionNumber <- collectionNumbers)  {
    val collection: TweetCollection[AvroTweet] = factory.createFromAvro("batch_" + collectionNumber, collectionNumber)

    collection.applyFunction(cleaning)
    collection.filter(filter)

    var counts = wordCounter.count(collection)
    wordCounter.writeCountsToLocalFile("results/WordCounterExample/" + collectionNumber + "_counts.txt", counts)
}

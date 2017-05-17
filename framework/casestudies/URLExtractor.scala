import org.apache.spark.rdd.RDD
import java.io._

import edu.vt.dlib.api.dataStructures._
import edu.vt.dlib.api.tools.FeatureExtractor


// Define collections to be pulled from hbase.
val collectionNumbers = Array(55)

def cleaning(tweet: AvroTweet): AvroTweet = {
    tweet.cleanSimpleMarker()
    tweet.cleanPunctuation()
    tweet.toLowerCase()
    tweet.cleanStopWords()
    tweet.cleanRTMarker()

    return tweet
}

def writeFeaturesToLocalFile(path: String, features: RDD[(String, String, String)]) = {
    val resultFile = new File(path)
    val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

    for(record <- features.collect()) {
        bufferedWriter.write(record._1 + "\t" + record._2 + "\t" + record._3 + "\n")
    }
    
    bufferedWriter.close()
}

val factory = new TweetCollectionFactory(sc, sqlContext)

for( collectionNumber <- collectionNumbers)  {
    val collection: TweetCollection[AvroTweet] = factory.createFromAvro("batch_" + collectionNumber, collectionNumber)

    collection.applyFunction(cleaning)

    var urls = collection.getCollection().map(tweet => (tweet.text, tweet.created_at, tweet.urls.mkString(" ")))

    var output_file = "/home/liuqing/2017s_tweet_url/results/URLCrawler/" + collectionNumber + "_urls"

    writeFeaturesToLocalFile(output_file, urls)    
}

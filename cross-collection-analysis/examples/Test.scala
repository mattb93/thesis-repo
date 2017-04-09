import edu.vt.dlib.api.dataStructures.SVConfig
import edu.vt.dlib.api.dataStructures.TweetCollection
import edu.vt.dlib.api.dataStructures.TweetCollectionFactory
//import edu.vt.dlib.api.dataStructures.SVTweetCollection
import edu.vt.dlib.api.dataStructures.Tweet
import edu.vt.dlib.api.dataStructures.SVTweet

//import edu.vt.dlib.api.pipeline.Runnable
//import edu.vt.dlib.api.pipeline.SVRunner
//import edu.vt.dlib.api.pipeline.AvroRunner

import org.apache.spark.rdd.RDD


/*
class Test() extends Runnable {
    import java.io._
    import edu.vt.dlib.api.dataStructures.TweetCollection
    import edu.vt.dlib.api.tools.WordCounter

    /*
     * Run method required by the runnable trait. Must take a TweetCollection as a parameter.
     */
    def run(collection: TweetCollection) = {
        println("Processing collection " + collection.collectionID)

        collection.applyFunction(cleaning).getCollection().take(20).foreach(println)
    }

    def cleaning(tweet: Tweet): Tweet = {
        return tweet.cleanRTMarker().cleanHashtags()
    }
}


// Import Runner so we can instantiate one below


// Define collections to be pulled from hbase.
//val collections = Array("41", "45", "128", "145", "157", "443")
val collections = Array("trails/AT_0224.txt")

// Create a new runner to run the analysis on the batch of collections.
// Pass it the Spark Context and SQL Context provided by the spark shell.
val runner = new SVRunner(sc, sqlContext)

val config = new SVConfig()
config.setTextIDOnly()
config.separator = "\t"

// Run the analysis by calling the run method and passing it the runnable we created above.
runner.run(new Test(), collections, config)
*/


class Test() extends Serializable{

    def cleaning(tweet: SVTweet): SVTweet = {
        tweet.cleanRTMarker()
        tweet.cleanHashtags()
        return tweet
    }

    def process(collection: TweetCollection[SVTweet]) = {

        collection.applyFunction(cleaning).getCollection().take(20).foreach(println)
    }
}

var path = "trails/AT_0224.txt"

val config = new SVConfig()
config.setTextIDOnly()
config.separator = "\t"

var factory = new TweetCollectionFactory(sc, sqlContext)

var collection = factory.createFromSVFile(path.split("/").last.split('.')(0), path, config)

var test = new Test()
test.process(collection)

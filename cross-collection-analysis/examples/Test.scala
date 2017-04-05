import edu.vt.dlib.api.pipeline.Runnable

/*
 * Word count example code. Extends runnable, which means it can be passed into the batch runner.
 */
class Test() extends Runnable {
    import java.io._
    import edu.vt.dlib.api.dataStructures.TweetCollection
    import edu.vt.dlib.api.tools.WordCounter

    /*
     * Run method required by the runnable trait. Must take a TweetCollection as a parameter.
     */
    def run(collection: TweetCollection) {
        println("Processing collection " + collection.collectionID)

        val col = collection.getCollection()

        val col2 = col.map(tweet => tweet.setTokens(tweet.tokens.filter(_.length < 4)))

        col2.take(10).foreach(tweet => println(tweet.tokens.mkString(" ")))
    }
}

// Import Runner so we can instantiate one below
import edu.vt.dlib.api.pipeline.AvroRunner

// Define collections to be pulled from hbase.
//val collections = Array("41", "45", "128", "145", "157", "443")
val collections = Array("41")

// Create a new runner to run the analysis on the batch of collections.
// Pass it the Spark Context and SQL Context provided by the spark shell.
val runner = new AvroRunner(sc, sqlContext)

// Run the analysis by calling the run method and passing it the runnable we created above.
runner.run(new Test(), collections)

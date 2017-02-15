import edu.vt.dlib.api.pipeline.Runnable

/*
 * Word count example code. Extends runnable, which means it can be passed into the batch runner.
 */
class WordCounter() extends Runnable {
    import java.io._
    import edu.vt.dlib.api.io.TweetCollection

    /*
     * Run method required by the runnable trait. Must take a TweetCollection as a parameter.
     */
    def run(collection: TweetCollection) {
        println("Processing collection number " + collection.collectionId)

        // The methods chained here are provided by the dlib api. We take the collection and run it through
        // some of the cleaning methods, then finally get the resulting collection in plain text form.
        val counts = collection.removeStopWords().removeRTs().toLowerCase().getPlainText()
        
        // Run the word count analysis
        counts.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false)
                    .collect();

        // Write the results back to local disk using standard java io
        val resultFile = new File("counts/z_" + collection.collectionId)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
    }
}

// Import Runner so we can instantiate one below
import edu.vt.dlib.api.pipeline.Runner

// Define collections to be pulled from hbase.
val collections = Array("41", "45", "128", "145", "157", "443")

// Create a new runner to run the analysis on the batch of collections.
// Pass it the Spark Context and SQL Context provided by the spark shell.
val runner = new Runner(sc, sqlContext)

// Run the analysis by calling the run method and passing it the runnable we created above.
runner.run(collections, new WordCounter())

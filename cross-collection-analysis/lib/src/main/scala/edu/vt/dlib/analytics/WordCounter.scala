package edu.vt.dlib.analytics

import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.io.TweetCollection


class WordCounter() extends Runnable {
    import java.io._

    def run(collection: TweetCollection) {
        println("Processing collection number " + collection.collectionNumber)

        val counts = collection.asPlainText()           // method provided by dlib api
                    .flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false)
                    .collect();

        val resultFile = new File("counts/z_" + collection.collectionNumber)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
    }
}


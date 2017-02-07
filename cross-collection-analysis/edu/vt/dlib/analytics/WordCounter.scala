package edu.vt.dlib.analytics

import edu.vt.dlib.api.pipeline.runnable
import edu.vt.dlib.api.io.tweetCollection


class WordCounter() extends runnable {
    import java.io._
    import sqlContext.implicits._

    def run(collection: tweetCollection) {
        println("Processiong collection number " + collection.collectionNumber)

        val counts = collection.asPlainText()           // method provided by dlib api
                    .flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false)
                    .collect();

        val resultFile = new File("counts/z_" + collection)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
    }
}


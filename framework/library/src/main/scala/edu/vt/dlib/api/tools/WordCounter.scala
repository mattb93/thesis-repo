package edu.vt.dlib.api.tools

import edu.vt.dlib.api.dataStructures.Tweet
import edu.vt.dlib.api.dataStructures.TweetCollection

class WordCounter() extends Serializable {

    import org.apache.spark.rdd.RDD
    
    import java.io._

	def count(collection: TweetCollection[_ <: Tweet]) : RDD[(String, Int)] = {

        return collection.getPlainText()
        		.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false);
	}

	def writeCountsToLocalFile(path: String, counts: RDD[(String, Int)]) = {
		// Write the results back to local disk using standard java io
        val countFile = new File(path)
        val bufferedWriterCounts = new BufferedWriter(new FileWriter(countFile))
        println("Writing counts to local file")
        for(count <- counts.collect()) {
            bufferedWriterCounts.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriterCounts.close()
	}
}

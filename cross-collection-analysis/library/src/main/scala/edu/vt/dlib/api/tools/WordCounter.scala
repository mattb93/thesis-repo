package edu.vt.dlib.api.tools

import edu.vt.dlib.api.io.TweetCollection

class WordCounter() {

    import org.apache.spark.rdd.RDD

	def count(collection: TweetCollection) : RDD[(String, Int)] = {

        return collection.getPlainText()
        		.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .sortBy(_._2, false);
	}
}

class TweetCleaner() {
    import java.io._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.ml.feature.StopWordsRemover
    import sqlContext.implicits._
    import scala.collection.mutable.WrappedArray

    def removeStopWords(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Removing Stop Words")

        val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")

        val wordsAsDF = collection.zipWithIndex().toDF("raw", "id")
        
        return remover.transform(wordsAsDF).select("filtered").map(r => (r(0).asInstanceOf[WrappedArray[String]]).toArray) 
    }

    def removeRTs(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Removing 'RT' instances")
        return collection.map(arr => arr.filter(! _.contains("RT")))
    }

    def removeMentions(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Removing mentions")
        return collection.map(arr => arr.filter(x => ! """\"*@.*""".r.pattern.matcher(x).matches))
    }

    def removeURLs(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Removing URLs")
        return collection.map(arr => arr.filter(x => ! """.*http.*""".r.pattern.matcher(x).matches))
    }

    def writeTweetsToFile(collection: RDD[Array[String]], fileName: String) {

        println("Writing results to file")
        val resultFile = new File(fileName)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        val localCollection = collection.collect()

        for(arr <- localCollection) {
            for(str <- arr) {
                bufferedWriter.write(str + " ")
            }
            bufferedWriter.write("\n")
        }
        
        bufferedWriter.close()
    }
     
}

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the second smallest at 13,476

val tweetCleaner = new TweetCleaner();

// Read text file
val textFile = sc.textFile("hdfs:///user/mattb93/processedCollections/z_157-textOnly-raw")

// Sets up a tupe of (Long, Array[String]) representing an id and the tweet's text
// ex: (42, [This, is, a, @twitter, tweet)
var wordsArrays = textFile.map(line => line.split(" "))

// Remove stop words from arrays
wordsArrays = tweetCleaner.removeStopWords(wordsArrays)

// Remove RT, links, and mentions
wordsArrays = tweetCleaner.removeRTs(wordsArrays)
wordsArrays = tweetCleaner.removeMentions(wordsArrays)
wordsArrays = tweetCleaner.removeURLs(wordsArrays)

// Print to a text file
tweetCleaner.writeTweetsToFile(wordsArrays, "z_157-noStopWords")

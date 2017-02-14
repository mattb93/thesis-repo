// This is a test
/*
import edu.vt.dlib.api.io.TweetCollection

class CollectionCleaner(var collection: TweetCollection) {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.ml.feature.StopWordsRemover
    import sqlContext.implicits._
    import scala.collection.mutable.WrappedArray

    def removeStopWords() : CollectionCleaner = {
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

    def removePunctuation(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Removing punctiation")
        return collection.map(arr => arr.map(x => x.replaceAll("[^A-Za-z0-9@#]", ""))).map(arr => arr.filter(_.length > 0))
    }

    def toLowerCase(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Converting to lowercase")
        return collection.map(arr => arr.map(x => x.toLowerCase()))
    }
     
}

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroInputFormat
import org.apache.avro.mapred.AvroWrapper
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.io.NullWritable

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the second smallest at 13,476

val tweetCleaner = new TweetCleaner();
//val collectionsToProcess = Array("41", "45", "128", "145", "157", "443")
val collectionsToProcess = Array("157")
for(collectionNumber <- collectionsToProcess) {
    println("Processing z_" + collectionNumber);
    // Read text file
    
    // Sets up an RDD of Array[String] representing tweet's text
    // ex: [This, is, a, @twitter, tweet]
    //var wordsArrays = textFile.map(line => line.split(" "))
    
    // Read text file
    val interface = new hbaseReadWrite()
    val wordsArrays = interface.readCollectionNumber(collectionNumber)

    // Remove stop words from arrays
    wordsArrays = tweetCleaner.removeStopWords(wordsArrays)

    // Remove RT, links, and mentions
    wordsArrays = tweetCleaner.removeRTs(wordsArrays)
    wordsArrays = tweetCleaner.removeMentions(wordsArrays)
    wordsArrays = tweetCleaner.removeURLs(wordsArrays)

    // Remove punctuation
    wordsArrays = tweetCleaner.removePunctuation(wordsArrays)

    // Switch to lowercase
    wordsArrays = tweetCleaner.toLowerCase(wordsArrays)

    // Print to a text file
    tweetCleaner.writeTweetsToHDFS(wordsArrays, "/user/mattb93/hdfsWriteTest", "z_" + collectionNumber + "-textOnly-noStopWords-noRT-noMentions-noURLs")
}
*/

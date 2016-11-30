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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.avro.mapred.AvroInputFormat
import org.apache.avro.mapred.AvroWrapper
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the second smallest at 13,476

val tweetCleaner = new TweetCleaner();

//val collectionsToProcess = Array("41", "45", "121", "122", "128", "145", "157", "443")
val collectionsToProcess = Array("157")
for(collectionNumber <- collectionsToProcess) {
    println("Processing z_" + collectionNumber);
    // Read text file
    
    // Sets up an RDD of Array[String] representing tweet's text
    // ex: [This, is, a, @twitter, tweet]
    //var wordsArrays = textFile.map(line => line.split(" "))
    
    // Read text file
    val path = "/collections/z_" + collectionNumber + "/part-m-00000.avro"
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)

    // Create an RDD of Array[String] representing each tweet's text
    // ex: [This, is, a, @Twitter, #tweet]
    var wordsArrays = avroRDD.map(l => new String(l._1.datum.get("text").toString())).map(line => line.split(" "))

    // Remove stop words from arrays
    wordsArrays = tweetCleaner.removeStopWords(wordsArrays)

    // Remove RT, links, and mentions
    wordsArrays = tweetCleaner.removeRTs(wordsArrays)
    wordsArrays = tweetCleaner.removeMentions(wordsArrays)
    wordsArrays = tweetCleaner.removeURLs(wordsArrays)

    // Print to a text file
    tweetCleaner.writeTweetsToFile(wordsArrays, "data/z_" + collectionNumber + "/z_" + collectionNumber + "-textOnly-noStopWords-noRT-noMentions-noURLs")
}

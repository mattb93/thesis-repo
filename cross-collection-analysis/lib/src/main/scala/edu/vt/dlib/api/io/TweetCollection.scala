package edu.vt.dlib.api.io

/*
 * Provides convenience methods to read tweet data from and write tweet data to the DLRL cluster.
 * Reads from avro files and provides methods to map data to more useful formats.
 *
 * Example usage:
 * val hdfsFile = new hdfsTweetsFileWrapper(42)
 * val tweetsText = hdfsFile.asPlainText()
 * //do some processing
 * 
 */
class TweetCollection(var collectionId: String, val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext) {
    import org.apache.avro.mapred.AvroInputFormat
    import org.apache.avro.mapred.AvroWrapper
    import org.apache.avro.generic.GenericRecord
    import org.apache.hadoop.io.NullWritable
    import org.apache.spark.rdd.RDD
    import scala.collection.mutable.WrappedArray

    import sqlContext.implicits._

    object Format extends Enumeration {
        type Format = Value
        val TEXT, ARRAYS, AVRO = Value
    }


    private val path = "/collections/tweets/z_" + collectionId + "/part-m-00000.avro"
    private val collection = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
    private val currentFormat = AVRO

    /*
     * Returns the raw avro data for more advanced processing.
     */
    //def asAvroRDD() : RDD[AvroWrapper[GenericRecord], NullWriteable, AvroInputFormat[GenericRecord]] = {
    //    return collection
    //}

    /*
     * Returns an RDD containing the text of the tweets.
     * ex: ["This is one tweet", "This is another #tweet @twitter"]
     */
    def asPlainText() : RDD[String] = {
        if(currentFormat == AVRO) {
            collection = collection.map(l => new String(l._1.datum.get("text").toString()))
        }
        else if(currentFormat == ARRAYS) {
            collection = collection.map(L => )
        }
        return collection
    }

    /*
     * Returns an RDD containing the text of the tweets broken up into array form.
     * ex: [["This", "is", "one", "tweet"], ["This", "is", "another", "#tweet", "@Twitter"]]
     */
    def asStringArrays() : RDD[Array[String]]  = {
        return collection.map(l => new String(l._1.datum.get("text").toString())).map(line => line.split(" "))
    }
}

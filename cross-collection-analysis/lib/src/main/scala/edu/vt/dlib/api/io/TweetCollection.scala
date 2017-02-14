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
    import org.apache.spark.rdd.RDD
    import org.apache.spark.ml.feature.StopWordsRemover
    import org.apache.hadoop.io.NullWritable
    import scala.collection.mutable.WrappedArray
    import sqlContext.implicits._


    private val path = "/collections/tweets/z_" + collectionId + "/part-m-00000.avro"
    private val collection = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
    collection = collection.map(lambda => (new String(collectionId + "-" + lambda._1.datum.get("id").toString()), new String(lambda._1.datum.get("text").toString).split(" ")))


    def getCollection() : RDD[(String, Array[String])] = {
        return collection
    }

    def asPlainText(): RDD[String] = {
        return collection.map(entry => entry._2.mkString(" "))
    }

    def asArrays(): RDD[Array[String]] = {
        return collection.map(entry => entry._2)

    def removeStopWords() : TweetCollection = {
        println("Removing Stop Words")

        val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")
        collection = remover.transform(collection.toDF("id", "raw")).select("filtered").map(r => (r(0).asInstanceOf[WrappedArray[String]]).toArray)
        return this
    }

    def removeRTs() : TweetCollection = {
        println("Removing 'RT' instances")

        collection = collection.map(entry => (entry._1, entry_2.filter(! _.contains("RT"))))

        return this
    }

    def removeMentions() : TweetCollection = {
        println("Removing mentions")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! """\"*@.*""".r.pattern.matcher(x).matches))
        return this
    }

    def removeURLs() : TweetCollection = {
        println("Removing URLs")
        collection = collection.map(entry => (entry._1, entry._2.filter(x => ! """.*http.*""".r.pattern.matcher(x).matches)))
        return this
    }

    def removePunctuation() : TweetCollection = {
        println("Removing punctiation")
        collection = collection.map(entry => (entry._1, entry._2.map(x => x.replaceAll("[^A-Za-z0-9@#]", "")))).filter(entry => entry_2.length > 0)
        return this
    }

    def toLowerCase(collection: RDD[Array[String]]) : RDD[Array[String]] = {
        println("Converting to lowercase")
        collection = collection.map(arr => arr.map(x => x.toLowerCase()))
        return this
    }
}

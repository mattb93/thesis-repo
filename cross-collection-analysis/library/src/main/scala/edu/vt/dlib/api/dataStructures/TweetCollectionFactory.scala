// This may be a bad idea...

package edu.vt.dlib.api.dataStructures

class TweetCollectionFactory(@transient sc: org.apache.spark.SparkContext, @transient sqlContext: org.apache.spark.sql.SQLContext) extends Serializable {

    import org.apache.avro.mapred.AvroInputFormat
    import org.apache.avro.mapred.AvroWrapper
    import org.apache.avro.generic.GenericRecord
    import org.apache.hadoop.io.NullWritable

    def createFromAvro(collectionID: String, collectionNumber: Int): TweetCollection[AvroTweet] = {
        val path = "/collections/tweets/z_" + collectionNumber + "/part-m-00000.avro"
        val records = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)

        val collection = records.map(lambda => new AvroTweet(lambda._1.datum))

        return new TweetCollection[AvroTweet](collectionID, sc, sqlContext, collection)
    }

    def createFromSVFile(collectionID: String, path: String, config: SVConfig = new SVConfig()): TweetCollection[SVTweet] = {
        val collection = sc.textFile(path).filter(line => line contains config.separator).map(line => new SVTweet(line, config))

        return new TweetCollection[SVTweet](collectionID, sc, sqlContext, collection)
    }
}

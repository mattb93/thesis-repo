// This may be a bad idea...

package edu.vt.dlib.api.dataStructures

class TweetCollectionFactory(@transient sc: org.apache.spark.SparkContext, @transient sqlContext: org.apache.spark.sql.SQLContext) extends Serializable {

    import org.apache.avro.mapred.AvroInputFormat
    import org.apache.avro.mapred.AvroWrapper
    import org.apache.avro.generic.GenericRecord
    
    import org.apache.hadoop.io.NullWritable
    import org.apache.hadoop.hbase.client

    import org.apache.spark.rdd.RDD

    def createFromArchive(collectionID: String, collectionNumber: Int): TweetCollection[AvroTweet] = {
        val path = "/collections/tweets/z_" + collectionNumber + "/part-m-00000.avro"
        val records = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)

        val collection = records.map(lambda => new AvroTweet(lambda._1.datum))

        return new TweetCollection[AvroTweet](collectionID, sc, sqlContext, collection)
    }

    def createFromAvroFile(collectionID: String, path: String): TweetCollection[AvroTweet] = {
        
        val records = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)

        val collection = records.map(lambda => new AvroTweet(lambda._1.datum))

        return new TweetCollection[AvroTweet](collectionID, sc, sqlContext, collection)
    }

    def createFromSVFile(collectionID: String, path: String, config: SVConfig = new SVConfig()): TweetCollection[SVTweet] = {
        val collection = sc.textFile(path).filter(line => line.split(config.separator).length == config.numColumns).map(line => new SVTweet(line, config))

        return new TweetCollection[SVTweet](collectionID, sc, sqlContext, collection)
    }

    def createFromStringsRDD(collectionID: String, collection: RDD[(String, String)]): TweetCollection[SimpleTweet] = {
        val tweetCollection = collection.map(pair => new SimpleTweet(pair._1, pair._2))

        return new TweetCollection[SimpleTweet](collectionID, sc, sqlContext, tweetCollection)
    }

    def createFromSeqs(collectionID: String, idList: Seq[String], textList: Seq[String]): TweetCollection[SimpleTweet] = {
        val combined = idList.zip(textList)
        val collection = sc.parallelize(combined.map(pair => new SimpleTweet(pair._1, pair._2)))

        return new TweetCollection[SimpleTweet](collectionID, sc, sqlContext, collection)
    }

    def createFromHBase(collectionID: String, table: HTable, config: HBaseConfig): TweetCollection[HBaseTweet] = {
        val scanner = table.getScanner(new Scan()).iterator()

        val localCollection = new Array[SimpleTweet](scanner.length)

        val index = 0
        for(result <- scanner) {
            localCollection(index) = new HBaseTweet(result)
        }

        return new TweetCollection(collectionID, sc, sqlContext, sc.parallelize(localCollection))
    }
}

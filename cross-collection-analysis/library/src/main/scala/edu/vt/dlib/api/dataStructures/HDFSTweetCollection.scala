package edu.vt.dlib.api.dataStructures

class HDFSTweetCollection(collectionID: String, sc: org.apache.spark.SparkContext, sqlContext: org.apache.spark.sql.SQLContext, var collectionNumber: String) extends TweetCollection(collectionID, sc, sqlContext) {
    
    import org.apache.avro.mapred.AvroInputFormat
    import org.apache.avro.mapred.AvroWrapper
    import org.apache.avro.generic.GenericRecord
    import org.apache.hadoop.io.NullWritable
	
	val path = "/collections/tweets/z_" + collectionNumber + "/part-m-00000.avro"
    val records = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
    //var collection = records.map(lambda => (new String(lambda._1.datum.get("id").toString), new String(lambda._1.datum.get("text").toString).split(" ")))
    collection = records.map(lambda => new HDFSTweet(lambda._1.datum))

}

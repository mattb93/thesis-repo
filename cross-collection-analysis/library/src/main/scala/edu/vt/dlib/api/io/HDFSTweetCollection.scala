package edu.vt.dlib.api.io

class HDFSTweetCollection(val sc: org.apache.spark.SparkContext, val sqlContext: org.apache.spark.sql.SQLContext, var collectionId: String) extends TweetCollection {
	
	val path = "/collections/tweets/z_" + collectionId + "/part-m-00000.avro"
    val records = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
    collection = records.map(lambda => (new String(lambda._1.datum.get("id").toString), new String(lambda._1.datum.get("text").toString).split(" ")))

}
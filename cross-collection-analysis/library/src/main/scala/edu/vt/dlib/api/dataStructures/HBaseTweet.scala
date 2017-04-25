package edu.vt.dlib.api.dataStructures

import org.apache.hadoop.hbase.client.Result


class HBaseTweet(val id: String, val result: Result, val config: HBaseConfig) 
	extends SimpleTweet(Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes(config.textColumnFamily), Bytes.toBytes(config.textColumnName)))) = {


	for( (name, identifier) <- config.otherColumns) {
		payload += name -> Bytes.toString(result.getValue(Bytes.toBytes(identifier.split(":")(0)), Bytes.toBytes(identifier.split(":")(1))))
	}

}

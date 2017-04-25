package edu.vt.dlib.api.dataStructures

import org.apache.hadoop.hbase.client.HTable

// THIS CLASS IS PROBABLY NOT NECESSARY LUL xD

class HBaseTweet(val table: HTable, val config: HBaseConfig) 
	extends SimpleTweet(config.ID, Bytes.toString(table.get(new Get(Bytes.toBytes(config.ID)))).getValue(Bytes.toBytes(config.textColumnFamily), Bytes.toBytes(config.textColumnName))) = {

    def getValueAt(columnFamily: String, column: String, rowKey: String): String = {
        // Make a new get object to handle getting the row from the table
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Get.html
        val get = new Get(Bytes.toBytes(rowKey))

        // get info from the table, returns a Result object
        // https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/Result.html
        val result = table.get(get)

        // get the value out of the desired column and return
        val finalResult = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))
        return finalResult
    }

	for( (column, value) <- config.otherColumns) {
		payload += name -> getValueAt(column._1, column._2, config.ID)
	}

}

package edu.vt.dlib.api.dataStructures

class HBaseConfig() extends Serializable {

	var tableName:			String = ""

	var textColumnFamily:	String = ""
	var textColumnName:		String = ""

	// (name, columnFamily:column)
	var otherColumns:		Array[(String, String)] = new Array[(String, String)](0)
}

package edu.vt.dlib.api.dataStructures

class HBaseConfig() extends Serializable {

	//import scala.collection.mutable.Map

	var tableName:			String = ""

	// 
	var ID:					String = ""

	var textColumnFamily:	String = ""
	var textColumnName:		String = ""

	var otherColumns:		Array[((String, String), String)] = new Array[((String, String), String)](0)
}

package edu.vt.dlib.api.dataStructures

class SVConfig() extends Serializable {

	//import scala.collection.mutable.Map

	var separator:			String = ", "
    var numColumns:         Int = 2

	var text: 				Int	= 1
	var id: 				Int	= 0
	var otherColumns:		Array[(String, Int)] = Array[(String, Int)](0)

    def setEmpty() = {
        numColumns = 0
	    text = -1
	    id = -1
    }

    def setDefault() = {
        numColumns = 2
	    text = 1
	    id = 0
    }
}

class processor(val collectionNumbers : Array[String]) {
	
	var collectionWriter = new collectionWriter()

	for( collectionNumber <- collectionNumbers) {
		
		var collection = new collectionReader(collectionNumber)

		// write your code here

        collectionWriter.write(collection)
	}
}

var collectionNumbers = Array("21")
val scriptRunner = new scriptRunner(collectionNumbers)

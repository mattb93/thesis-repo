class WordCounter() {
    import java.io._   
    import sqlContext.implicits._
    
    def countWords(collection: String) {
        println("Processing z_" + collection);

        val textFile = sc.textFile("hdfs:///user/mattb93/processedCollections/z_" + collection + "-textOnly-noStopWords-noRT-noMentions-noURLs")

        val counts = textFile.flatMap(line => line.split(" "))
                            .map(word => (word, 1))
                            .reduceByKey(_ + _)
                            .sortBy(_._2, false)
                            .collect();

        val resultFile = new File("counts/z_" + collection + ".tsv")
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
    }   
    

    def batchCount(collections: Array[String]) {
        var collection = ""
    
        for( collection <- collections ) {
            println("Counting z_" + collection)
            countWords(collection)
        }
    }
}

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the second smallest
// at 13,476
val wordCounter = new WordCounter();
wordCounter.countWords("443");
//wordCounter.batchCount(Array("41", "45", "122", "128", "145", "158", "444"));

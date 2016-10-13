class WordCounter() {
    import java.io._

    def countWords(collection: String) {
        println("Processing z_" + collection);
        val textFile = sc.textFile("hdfs:///collections/tweets/z_" + collection)
        val counts = textFile.flatMap(line => line.split(" "))
                            .map(word => (word, 1))
                            .reduceByKey(_ + _)
                            .sortBy(_._2, false)
                            .collect();
        //counts.saveAsTextFile("z_" + collection)

        val file = new File("counts/z_" + collection)
        val bufferedWriter = new BufferedWriter(new FileWriter(file))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write("(" + count._1 + ", " + count._2 + ")\n")
        }
        bufferedWriter.close()
    }   
    

    def batchCount(collections: Array[String]) {
        var collection = ""
    
        for( collection <- collections ) {
            println("z_" + collection)
            countWords(collection)
        }
    }
}

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the secon smallest
// at 13,476
val wordCounter = new WordCounter();
//wordCounter.countWords("157");
//wordCounter.batchCount(Array("157", "145"))
wordCounter.batchCount(Array("41", "45", "121", "122", "128", "145", "157", "443"));

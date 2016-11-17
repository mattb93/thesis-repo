class WordCounter() {
    import java.io._
    import org.apache.spark.ml.feature.StopWordsRemover
    
    import sqlContext.implicits._
 
    def batchRemoveStopWords(collections: Array[String]) {
        val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")

        var collection = ""

        for( collection <- collections) {
            //val textFile = sc.textFile("hdfs:///collections/tweets/z_" + collection)
            val textFile = sc.textFile("hdfs:///user/mattb93/processedCollections/z_157-textOnly-raw")
            val words = textFile.map(line => (line.split(" "), "")).toDF("raw", "filtered")
            //val words = textFile.map(line => (line, "")).toDF("raw", "filtered")
            
            remover.transform(words).show(200)
            
            /*
            val processedEntries = remover.transform(words).collect()

            val resultFile = new File("noStopWords-z_" + collection + ".txt")
            val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
            for(entry <- processedEntries) {
                bufferedWriter.write(entry.toString() + "\n")
            }
            bufferedWriter.close()
            */
        }
    }
    
    def countWords(collection: String, localFile: Boolean) {
        println("Processing z_" + collection);

        //val textFile : DataFrame = null

        //if(localFile) {
        //    textFile = sc.textFile("z_" + collection)
        //}
        //else {
        val    textFile = sc.textFile("hdfs:///collections/tweets/z_" + collection)
        //}

        val counts = textFile.flatMap(line => line.split(" "))
                            .map(word => (word, 1))
                            .reduceByKey(_ + _)
                            .sortBy(_._2, false)
                            .collect();
        //counts.saveAsTextFile("z_" + collection)

        val resultFile = new File("counts/z_" + collection + ".tsv")
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
        for(count <- counts) {
            //println(count)
            bufferedWriter.write(count._1 + "\t" + count._2 + "\n")
        }
        bufferedWriter.close()
    }   
    

    def batchCount(collections: Array[String], localFile: Boolean) {
        var collection = ""
    
        for( collection <- collections ) {
            println("z_" + collection)
            countWords(collection, localFile)
        }
    }
}

// Collection 157 is the New Mexico Middle School Shooting, 
// which is the smallest one at 11,757. 121 is the secon smallest
// at 13,476
val wordCounter = new WordCounter();
//wordCounter.countWords("157");
wordCounter.batchRemoveStopWords(Array("157"));
//wordCounter.batchCount(Array("41", "45", "121", "122", "128", "145", "157", "443"), false);

package edu.vt.dlib.api.io

/*
 * Interface to write collections back to a local file, hdfs, or hbase 
 *
 */
class CollectionWriter() {

    
	/*
	 * Write the tweets to a file on the local filesystem (as opposed to on HDFS).
	 * Takes in an RDD[Array[String]] and maps it back to plain text. Uses java io.
	 */
	def writeToFile(collection: RDD[Array[String]], path: String) {

        println("Writing results to local file '" + path + "'")

        val resultFile = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        val localCollection = collection.collect()

        for(arr <- localCollection) {
            for(str <- arr) {
                bufferedWriter.write(str + " ")
            }
            bufferedWriter.write("\n")
        }
        
        bufferedWriter.close()
    }

    def writeToFile(collection: RDD[String], path: String) {
        println("Writing results to local file '" + path + "'")

        val resultFile = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        val localCollection = collection.collect()

        for(str <- localCollection) {
            bufferedWriter.write(str + "\n")
        }
        
        bufferedWriter.close()
    }

    /*
     * Write the tweets back to a file in hdfs. Takes in an RDD[Array[String]] and
     * maps it back to plain text. Recommend specifying an absolute path rather 
     * than a local one.
     */
    def writeToHDFS(collection: RDD[Array[String]], path: String) {

        println("Writing results to HDFS at '" + path + "'")

        collection.map(L => L.mkString(" ")).saveAsTextFile(path)
        
    }

    def writeToHDFS(collection: RDD[String], pathL String) {
        println("Writing results to HDFS at '" + path + "'")

        collection.saveAsTextFile(path)
    }
}

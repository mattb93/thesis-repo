class LDAWrapper() {
  import scala.collection.mutable
  import org.apache.spark.mllib.clustering.{DistributedLDAModel,LDA}
  import org.apache.spark.mllib.linalg.{Vector, Vectors}
  import org.apache.spark.rdd.RDD
  import java.io._

  import edu.vt.dlib.api.io.SVTweetCollection
  import edu.vt.dlib.api.tools.WordCounter

  def analyze(file: String, termsToIgnore: Array[String], numTopics: Int) = {
    
    val rawCollection = new SVTweetCollection("AT", sc, sqlContext, file, "\t")
    val collection = rawCollection.removeStopWords().removeRTs().removeMentions().removeHashtags().removeURLs().removePunctuation().toLowerCase()
    
    val termCounts = new WordCounter().count(collection)

    // Collect all the individual words
    val vocabArray: Array[String] = termCounts.map(_._1).collect()
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Tokenize the collection. You can ignore certain terms that you know will
    // appear too often to be meaningful
    val tokens = collection.getTextArrays().map(_.filter( t => !termsToIgnore.contains(t)))

    // Create a collection of term count vectors
    val documents = tokens.zipWithIndex.map { case (tokens, id) => 
      val counts = new mutable.HashMap[Int, Double]()
      tokens.foreach { term =>
        if(vocab.contains(term)){
          val index = vocab(term)
          counts(index) = counts.getOrElse(index, 0.0) + 1.0
        }
      }
      (id, Vectors.sparse(vocab.size, counts.toSeq))
    }

    // LDA Params
    val maxIterations = 100;
    
    // Run LDA
    val lda = new LDA().setOptimizer("em").setK(numTopics).setMaxIterations(maxIterations)
    val ldaModel = lda.run(documents)

    // Dump topics to a file
    println("Writing to file")
    val resultFile = new File("data/trails/topics/" + file.split("/").last + "-topics")
    val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))
    var topicIndices = ldaModel.describeTopics(numTopics)
    var topicNum = 1;

    topicIndices.foreach{ case(terms, weights) =>
      bufferedWriter.write("Topic #" + topicNum + "\n")
      println("Topic #" + topicNum)
      topicNum = topicNum + 1
      terms.zip(weights).foreach { case (term, weight) =>
        bufferedWriter.write(vocabArray(term) + " : " + weight + "\n")
        println(vocabArray(term) + " : " + weight)
      }
      bufferedWriter.write("\n")
    }

    bufferedWriter.close()
  }
}

val ldaRunner = new LDAWrapper()
ldaRunner.analyze("/user/mattb93/trails/AT_0220.tsv", Array(), 5)


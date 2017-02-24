package edu.vt.dlib.api.tools

class LDAWrapper() {
	import edu.vt.dlib.api.io.TweetCollection
	import edu.vt.dlib.api.tools.WordCounter

	import scala.collection.mutable
	import org.apache.spark.mllib.clustering.{DistributedLDAModel,LDA}
	import org.apache.spark.mllib.linalg.{Vector, Vectors}
	import org.apache.spark.rdd.RDD

	// LDA parameters. Can be set before running.
	var maxIterations: Int = 100
	var numTopics: Int = 5
	var ldaOptimizer: String = "em"
	var termsToIgnore: Array[String] = Array()

	// Returns RDD[(TopicNumber, Array[(Term, Weight)])]
	def analyze(collection: TweetCollection) : Array[(Array[String], Array[Double])] = {
		
		// Get term counts
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

	    
	    // Run LDA
	    val lda = new LDA().setOptimizer(ldaOptimizer).setK(numTopics).setMaxIterations(maxIterations)
	    val ldaModel = lda.run(documents)

	    var topicIndices = ldaModel.describeTopics(numTopics)

	    val result: Array[(Array[String], Array[Double])] = topicIndices.map { case (termIndices, weights) => (termIndices.map(index => vocabArray(index)), weights)}

	    return result
	}
}
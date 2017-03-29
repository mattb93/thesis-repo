// 1. Gather self-labeled tweets (emojis)
// 2. Farm those tweets for commons words and/or frequent patterns
// 3. Expand collection of seed words by running frequent pattern mining on tweets that contain those frequent patterns

import edu.vt.dlib.api.io.{TweetCollection, HDFSTweetCollection}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector

class Word2VecSentimentAnalysis() extends Serializable {
    
    import java.io._

    // Configutation parameters
    val tokensToDiscard = Array("connecticut", "shooting", "school", "sandy", "hook")
    val collectionNumber = "41"
    val collectionID = "NewtownShooting"
    val orientationSeed = ":("

    val word2VecModelLength = 70
    val tweetLength = 70
    var word2VecModel : Word2VecModel = null

    val numClasses = 2
    var classifierModel : LogisticRegressionModel = null

    def run() = {
        // Get the collection and clean it
        val collection: TweetCollection = new HDFSTweetCollection(collectionID, sc, sqlContext, collectionNumber)
        val cleanCollection: TweetCollection = collection.removeStopWords().removeRTs().removeMentions().removeURLs().toLowerCase().removeTokens(tokensToDiscard)

        // Format corpus as RDD of Seq for Word2Vec. This is only tweets which contained the oriented emoji we're looking for.
        val orientedTweets: RDD[(Double, Array[String])] = cleanCollection.getTextArrays().filter(entry => entry.contains(orientationSeed)).map(entry => (0, entry))

        // Fit a word2Vec model with the oriented data
        val word2Vec = new Word2Vec()
        word2Vec.setVectorSize(word2VecModelLength)
        word2VecModel = word2Vec.fit(orientedTweets.map(entry => entry._2.toSeq))

        // Need an RDD of LabeledPoints to pass to LogisticRegressionWithLBFGS
        // Get the features
        val training = orientedTweets.map(tweet => createLabeledPoint(tweet)).cache()
        val data = cleanCollection.getTextArrays().map(tweet => (tweet.mkString(" "), createFeatureVector(tweet))).cache()

        classifierModel = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(training)

        val predictions = data.map { case (text, features) =>
            val prediction = classifierModel.predict(features)
            (text, prediction)
        }

        predictions.take(100).foreach(println)

        printToFile("results/test.txt", predictions)
    }

    def createFeatureVector(tweet: Array[String]): org.apache.spark.mllib.linalg.Vector = {
        val doubles: Array[Double] = tweet.map(word => getAveragedWordVector(word))
        return Vectors.dense(PadFeatureArray(doubles))
    }

    def createLabeledPoint(tweet: (Double , Array[String])): LabeledPoint = {
        val features: Array[Double] = tweet._2.map(word => getAveragedWordVector(word))
        val featuresVector = Vectors.dense(PadFeatureArray(features))

        val labeledPoint = new LabeledPoint(tweet._1, featuresVector)

        return labeledPoint
    }

    def getAveragedWordVector(word: String): Double = {
        var sum = 0.0
        try {
          val transformedVector = word2VecModel.transform(word)
          transformedVector.toArray.map(x => sum = sum + x)
        } catch {
          case _ => sum = 0.0
        }

        return sum/word2VecModelLength
    }

    def PadFeatureArray(features: Array[Double]): Array[Double] = {
        val paddedFeatures = new Array[Double](tweetLength)
        for (i <- 1 to features.length) {
            paddedFeatures(i - 1) = features(i -1)
        }
        return paddedFeatures
    }

    def printToFile(path: String, data: RDD[(String, Double)]) = {
        val resultFile = new File(path)
        val bufferedWriter = new BufferedWriter(new FileWriter(resultFile))

        for(record <- data.collect()) {
            bufferedWriter.write(record._1 + "\t" + record._2 + "\n")
        }
        
        bufferedWriter.close()
    }
}


val w2va = new Word2VecSentimentAnalysis()
w2va.run()

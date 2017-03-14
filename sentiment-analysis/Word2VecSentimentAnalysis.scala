// 1. Gather self-labeled tweets (emojis)
// 2. Farm those tweets for commons words and/or frequent patterns
// 3. Expand collection of seed words by running frequent pattern mining on tweets that contain those frequent patterns

import edu.vt.dlib.api.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.regression.LabeledPoint


import java.io._

class Word2VecSentimentAnalysis() {
    

    // Configutation parameters
    val tokensToDiscard = Array("connecticut", "shooting", "school", "sandy", "hook")
    val collectionNumber = "41"
    val collectionID = "NewtownShooting"
    val oritntationSeed = ":("

    val word2VecModelLength = 70
    val word2VecModel : Word2VecModel = null

    val numClasses = 2
    val classifierModel : LogisticRegressionModel = null

    def run() = {
        // Get the collection and clean it
        val collection: TweetCollection = new HDFSTweetCollection(collectionID, sc, sqlContext, collectionNumber)
        val cleanCollection: TweetCollection = collection.removeStopWords().removeRTs().removeMentions().removeURLs().toLowerCase().removeTokens(tokensToDiscard)

        // Format corpus as RDD of Seq for Word2Vec. This is only tweets which contained the oriented emoji we're looking for.
        val orientedTweets: RDD[(String, Array[String])] = cleanCollection.getTextArraysID().filter(entry => entry._2.contains(orientationSeed))

        // Fit a word2Vec model with the oriented data
        val word2Vec = new Word2Vec()
        word2Vec.setModelLength(word2VecModelLength)
        word2VecModel = word2Vec.fit(orientedTweets.map(entry => entry._2.toSeq))

        // Need an RDD of LabeledPoints to pass to LogisticRegressionWithLBFGS
        // Get the features
        val training = orientedTweets.map(tweet => createLabeledPoint(tweet)).cache()
        val data = cleanCollection.map(tweet => createLabeledPoint(tweet)).cache()

        classifierModel = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(training)

        val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
            val prediction = classifierModel.predict(features)
            (prediction, label)
        }

        predictionAndLabels.take(100).foreach(println)
    }

    def createLabeledPoint(tweet: (String, Array[String])): LabeledPoint = {
        val features = tweet._2.map(word => getAveragedWordVector(word))
        features = Vectors.dense(PadFeatureArray(features))

        val labeledPoint = new LabeledPoint(tweet._1.toDouble, features)

        return labeledPoint
    }

    def getAveragedWordVector(word: String) = {
        var sum = 0.0
        try {
          val transformedVector = word2VecModel.transform(word)
          transformedVector.toArray.map(x => sum = sum + x)
        } catch {
          case _ => sum = 0.0
        }

        return sum/_word2VecModelLength
    }

    def PadFeatureArray(features: Array[Double]): Array[Double] = {
        val paddedFeatures = new Array[Double](tweetLength)
        for (i <- 1 to features.length) {
            paddedFeatures(i - 1) = features(i -1)
        }
        return paddedFeatures
    }
}




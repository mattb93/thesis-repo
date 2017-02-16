# Cross Collection analysis library

This library is intended to make interacting with the DLRL Cluster faster and easier. It is broken up into two parts: A wrapper to run scala scripts on any number of collections, and a set of tools to make interacting with data easier. +This readme is very much a work in progress.


## Using the Runnable/Runner wrappers

Refer to this boilerplate code, explained below.

```scala
import edu.vt.dlib.api.pipeline.Runnable
import edu.vt.dlib.api.pipeline.Runner

/*
 * Boilerplate Analysis class. Must extend Runnable to be able to be passed to the runner later.
 */
class Analysis() extends Runnable {
    import java.io._
    import edu.vt.dlib.api.io.TweetCollection

    /*
     * Run method required by the runnable trait. Must take a TweetCollection as a parameter.
     * TweetCollections will be set up by the framework and passed to the run method.
     */
    def run(collection: TweetCollection) {

        // Insert your own analysis code here...

    }
}

// Define collections to be pulled from hbase.
val collections = Array("41", "45", "128", "145", "157", "443")

// Create a new runner to run the analysis on the batch of collections.
// Pass it the Spark Context (sc) and SQL Context provided by the spark shell.
val runner = new Runner(sc, sqlContext)

// Run the analysis by calling the run method and passing it the runnable we created above.
runner.run(collections, new Analysis())
```

This boilerplate shows an "Analysis" class. This is the class that you will instantiate to run your code. It can be named anything you like. The class must extend `runnable`, which is a scala trait that requires that it have a method named `run` which takes a `TweetCollection` as a parameter. The run method is what the framework will call when it sets up new `TweetCollection`s to process. The `TweetCollection` wrapper is explained below.



## The TweetCollection Wrapper

The `TweetCollection` class is what handles reading data from the cluster to be used in your analysis. This class takes a collection Id, spark context, and sql context as a parameter and uses them to pull an avro file out of HDFS, decode it, and turn it into an RDD to be processed here. It also provides a handful of convenience methods, including ways to clean the tweets and several ways to transform it into a format that may be more useful to you. By default, `getCollection()` will (currently) return an `RDD[(String, Array[String])]`.  The first string represents the tweet's individual ID, and the array of strings contains the tweet's text broken up into tokens. This will be changed later to include more of the metadata provided by the avro file (tweet author, time of posting, etc). There are four other methods to get an RDD out of the collection:

1. `getPlainText()`
  * Returns an `RDD[String]` containing the text of the tweets
2. `getPlainTextID()`
  * Returns an `RDD[(String, String)]` containing the ID's and the text of the tweets
3. `getTextArrays()`
  * Returns an `RDD[Array[String]]` containing the tokenized tweet text in array form
4. `getTextArraysID()`
  * Returns an `RDD[(String, Array[String])]` containing the tweets' ID's and the tokenized text in array form (currently the same as `getCollection()`)

The class also provides the following cleaning methods

1. `removeStopWords()`
  * Removes the default set of stop words as defined in Spark's `org.apache.spark.ml.feature.StopWordsRemover`
2. `removeRTs()`
  * Removes the "RT" token that appears at the beginning of every tweet
3. `removeMentions()`
  * Removes mentions from tweets. Mentions have the format `@mention`
4. `removeHashtags()`
  * Removes hashtags from the tweets. Hashtags have the format `#hashtag`
5. `removeURLs()`
  * Removes urls from the tweets.
6. `removePunctuation()`
  * Does not remove @ or # symbols to preserve mentions and hashtags
7. `toLowerCase()`
  * Makes all letters lowercase
8. `removeRegexMatches(regex: scala.util.matching.Regex)`
  * Removes all tokens matching specified regular expression
9. `removeRegexNonmatches(regex: scala.util.matching.Regex)`
  * Removes all tokens that do not match specified regular expression

All of the cleaning methods return the Tweet Collection, so they can be chained together. For example, if you wanted to remove stop words and RTs, change everything to lowercase, and then get the plain text, you could make a call like this in one line:
```scala
collection.removeStopWords().removeRTs().toLowerCase().getPlainText()
```
Note that the cleaning methods modify the internal collection, so whenever you call one of the get methods all of the cleaning done beforehand will be applied.

## Tools

The library also provides a set of tools to help with your analyses. Currently implemented tools include:

1. A word counter to count the frequency of tokens in the collection
2. A Feature extractor to extract information from the tweets

Planned tools include:

1. A wrapper for the Stanford Named Entity Recognizer
2. Sentiment and topic analysis tools
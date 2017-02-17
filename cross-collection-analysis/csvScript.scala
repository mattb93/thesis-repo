import edu.vt.dlib.api.io.TweetCollection
import edu.vt.dlib.api.tools.WordCounter

val collection = new TweetCollection("data/Dataset_z_887_200026_tweets.csv", sc, sqlContext).removeStopWords().removePunctuation().removeRTs().removeURLs().toLowerCase()

val counter = new WordCounter()

counter.count(collection)

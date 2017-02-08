import edu.vt.dlib.api.pipeline.Runner
import edu.vt.dlib.analytics.WordCounter


//========================================//
// Run word count on a set of collections //
//========================================//

// Define collections
val collections = Array(41, 45, 128, 145, 157, 443)

// Create a new runner with the collection numbers and a word counter to run
val runner = new Runner(sc, sqlContext)

// Run the analysis
runner.run(collections, new WordCounter())

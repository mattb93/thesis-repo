import edu.vt.dlib._

//========================================//
// Run word count on a set of collections //
//========================================//

// Define collections
val collections = Array("41", "45", "128", "145", "157", "443")

// Create a new runner with the collection numbers and a word counter to run
val runner = new Runner(collections, new WordCounter())

// Run the analysis
runner.run()
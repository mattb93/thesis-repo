package edu.vt.dlib.api.dataStructures

class SVConfig() {

	//import scala.collection.mutable.Map

	var separator			String = ", "

	var archiveSource: 		Int	= 0
	var text: 				Int	= 1
	var to_user_id: 		Int	= 2
	var from_user: 			Int	= 3
	var id: 				Int	= 4
	var from_user_id:		Int	= 5
	var iso_language_code: 	Int	= 6
	var source: 			Int	= 7
	var profile_image_url: 	Int	= 8
	var geo_type: 			Int	= 9
	var geo_coordinates_0: 	Int	= 10
	var geo_coordinates_1: 	Int	= 11
	var created_at: 		Int	= 12
	var time:				Int	= 13
	var otherColumns:		Map[String, Int] = Map()
}
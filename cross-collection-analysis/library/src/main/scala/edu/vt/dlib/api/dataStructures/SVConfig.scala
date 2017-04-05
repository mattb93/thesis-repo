package edu.vt.dlib.api.dataStructures

class SVConfig() extends Serializable {

	//import scala.collection.mutable.Map

	var separator:			String = ", "
    var numColumns:         Int = 14

	var archivesource: 		Int	= 0
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

    def setEmpty() = {
        numColumns = 0

 	    archivesource = -1
	    text = -1
        to_user_id = -1
	    from_user = -1
	    id = -1
        from_user_id = -1
	    iso_language_code = -1
	    source = -1
	    profile_image_url = -1
	    geo_type = -1
	    geo_coordinates_0 = -1
	    geo_coordinates_1 = -1
	    created_at = -1
	    time = -1 
    }

    def setTextIDOnly() = {
        numColumns = 2

 	    archivesource = -1
	    text = 1
        to_user_id = -1
	    from_user = -1
	    id = 0
        from_user_id = -1
	    iso_language_code = -1
	    source = -1
	    profile_image_url = -1
	    geo_type = -1
	    geo_coordinates_0 = -1
	    geo_coordinates_1 = -1
	    created_at = -1
	    time = -1 
    }

    def setDefault() = {
        numColumns = 14

 	    archivesource = 0
	    text = 1
        to_user_id = 2
	    from_user = 3
	    id = 4
        from_user_id = 5
	    iso_language_code = 6
	    source = 7
	    profile_image_url = 8
	    geo_type = 9
	    geo_coordinates_0 = 10
	    geo_coordinates_1 = 11
	    created_at = 12
	    time = 13
    }
}

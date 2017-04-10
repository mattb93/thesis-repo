package edu.vt.dlib.api.dataStructures

import org.apache.avro.generic.GenericRecord

class AvroTweet(dataRecord: GenericRecord) extends Tweet(dataRecord.get("text").toString, dataRecord.get("id").toString) {
	
	var archivesource = dataRecord.get("archivesource").toString
	var to_user_id = dataRecord.get("to_user_id").toString
	var from_user = dataRecord.get("from_user").toString
	var from_user_id = dataRecord.get("from_user_id").toString
	var iso_language_code = dataRecord.get("iso_language_code").toString
	var source = dataRecord.get("source").toString
	var profile_image_url = dataRecord.get("profile_image_url").toString
	var geo_type = dataRecord.get("geo_type").toString
	var geo_coordinates_0 = dataRecord.get("geo_coordinates_0").toString.toDouble
	var geo_coordinates_1 = dataRecord.get("geo_coordinates_1").toString.toDouble
	var created_at = dataRecord.get("created_at").toString
	var time = dataRecord.get("time").toString.toInt

	def filterByArchiveSource(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.archivesource == filter) == keepIf)

        return this
    }

    def filterByToUserId(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.to_user_id == filter) == keepIf)

        return this
    }

    def filterByFromUser(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.from_user == filter) == keepIf)

        return this
	}

	    def filterByFromUserId(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.from_user_id == filter) == keepIf)

        return this
    }

    def filterByIsoLanguageCode(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.iso_language_code == filter) == keepIf)

        return this
    }

    def filterBySource(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.source == filter) == keepIf)

        return this
    }

    def filterByProfileImageURL(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.profile_image_url == filter) == keepIf)

        return this
    }

    def filterByGeoType(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.geo_type == filter) == keepIf)

        return this
    }

    def filterByGeoCoordinates0(filter: Double, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan){
            collection = collection.filter(tweet => tweet.geo_coordinates_0 > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.geo_coordinates_0 < filter)
        }

        return this
    }

    def filterByGeoCoordinates1(filter: Double, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan) {
            collection = collection.filter(tweet => tweet.geo_coordinates_1 > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.geo_coordinates_1 < filter)
        }

        return this
    }

    def filterByCreatedAt(filter: String, keepIf: Boolean = true) : TweetCollection[T] = {

        collection = collection.filter(tweet => (tweet.created_at == filter) == keepIf)

        return this
    }

    def filterByTime(filter: Int, greaterThan: Boolean = true) : TweetCollection[T] = {

        if(greaterThan) {
            collection = collection.filter(tweet => tweet.time > filter)
        }
        else {
            collection = collection.filter(tweet => tweet.time < filter)
        }

        return this
	}
}

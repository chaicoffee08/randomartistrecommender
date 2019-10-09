package com.chai.datascience.recommender

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object RunRecommender {

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().getOrCreate()
		spark.sparkContext.setCheckpointDir("hdfs:///tmp/")
		
		
		spark.sparkContext.setLogLevel("ERROR")
		val base = "hdfs:///user/root/"
		val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
		val rawArtistData = spark.read.textFile(base + "artist_data.txt")
		val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

		val runRecommender = new RunRecommender(spark)
		
		// user-based collaborative filtering
		// runRecommender.recommend_ubcf(rawUserArtistData, rawArtistData, rawArtistAlias)

		// item-based collaborative filtering
		runRecommender.recommend_ibcf(rawUserArtistData, rawArtistData, rawArtistAlias)
	}
}

class RunRecommender(private val spark: SparkSession) {

	import spark.implicits._

	def recommend_ubcf(
		rawUserArtistData: Dataset[String],
		rawArtistData: Dataset[String],
		rawArtistAlias: Dataset[String]): Unit = {

	val userDF = rawUserArtistData.map { line =>
		val Array(user, _*) = line.split(' ')
		(user.toInt)
	}.toDF("user")

	val sampleUserDS = userDF.sample(true, 0.000001)

	val artistByID = buildArtistByID(rawArtistData)
	
	val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

	val trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()

	val model = new ALS().
      		setSeed(Random.nextLong()).
      		setImplicitPrefs(true).
      		setRank(10).
      		setRegParam(0.01).
      		setAlpha(1.0).
      		setMaxIter(5).
      		setUserCol("user").
      		setItemCol("artist").
      		setRatingCol("count").
      		setPredictionCol("prediction").
      		fit(trainData)


	model.userFactors.select("features").show(truncate = false)

	val userID = sampleUserDS.head.getInt(0)

      	val existingArtistIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()

	artistByID.filter($"id" isin (existingArtistIDs:_*)).show(truncate = false)
	
	val topRecommendations = model.recommendForUserSubset(sampleUserDS, 10)
	topRecommendations.show(truncate = false)

	val topRecommendationsForUserID = topRecommendations.filter($"id" isin (userID))
	topRecommendationsForUserID.show(truncate = false)

	val recommendedArtistIDs = topRecommendationsForUserID.select(explode(col("recommendations")("artist"))).as[Int].collect()

	artistByID.filter($"id" isin (recommendedArtistIDs:_*)).show(truncate = false)
	}

	def recommend_ibcf(
		rawUserArtistData: Dataset[String],
		rawArtistData: Dataset[String],
		rawArtistAlias: Dataset[String]): Unit = {

		println("Running Item Based Collaborative Filtering...")
		println("Searching for seed Artist...")

		val UserArtistDF = rawUserArtistData.map { line =>
     			val Array(user, artist, _*) = line.split(' ')
     			(user.toInt, artist.toInt)
     			}.toDF("user", "artist")

		val artistByID = buildArtistByID(rawArtistData)

		val ArtistDF = UserArtistDF.select(col("artist"))

		val sampleArtistsDS = ArtistDF.sample(0.000001)

		val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

		val artistID = sampleArtistsDS.head.getInt(0)

		artistByID.filter($"id" isin (artistID)).show(truncate = false)
		
		val trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()
		
		println("Building ALS Model...")

		val model = new ALS().
                	setSeed(Random.nextLong()).
                	setImplicitPrefs(true).
                	setRank(10).
                	setRegParam(0.01).
                	setAlpha(1.0).
                	setMaxIter(5).
                	setUserCol("user").
                	setItemCol("artist").
                	setRatingCol("count").
                	setPredictionCol("prediction").
                	fit(trainData)

		model.userFactors.select("features").show(truncate = false)

		val topUsersForSampleArtistsDS = model.recommendForItemSubset(sampleArtistsDS, 5)

		val topUsersForArtistID = topUsersForSampleArtistsDS.filter($"artist" isin (artistID))
	
		val topUsersForArtistIDMinusRating = topUsersForArtistID.select(explode(col("recommendations")("user")).as("user"))

		val topUserIDForArtistID = topUsersForArtistIDMinusRating.head.getInt(0)
		
		val topArtistsForTopUsers = model.recommendForUserSubset(topUsersForArtistIDMinusRating, 5)

		val topArtistsForTopUserForArtistID = topArtistsForTopUsers.filter($"user" isin (topUserIDForArtistID))
		
		val topSimilarArtists = topArtistsForTopUserForArtistID.select(explode(col("recommendations")("artist")).as("top artists"))

		val recommendedArtistIDs = topSimilarArtists.select("top artists").as[Int].collect()
		
		println("Top Similar Artists...")

		artistByID.filter($"id" isin (recommendedArtistIDs:_*)).show(truncate = false)	

	}

	def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
        rawArtistData.flatMap { line =>
        	val (id, name) = line.span(_ != '\t')
        	if (name.isEmpty) {
        		None
      		} else {
        		try {
          		 Some((id.toInt, name.trim))
        		} catch {
          		 case _: NumberFormatException => None
       			 }
      			}
    	}.toDF("id", "name")
       }

       def buildArtistAlias(rawArtistAlias: Dataset[String]): Map[Int,Int] = {
    		rawArtistAlias.flatMap { line =>
      			val Array(artist, alias) = line.split('\t')
      			if (artist.isEmpty) {
        			None
      			} else {
        			Some((artist.toInt, alias.toInt))
      			}
    		}.collect().toMap
  	}

	def buildCounts(
      		rawUserArtistData: Dataset[String],
      		bArtistAlias: Broadcast[Map[Int,Int]]): DataFrame = {
    			rawUserArtistData.map { line =>
      				val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      				val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      				(userID, finalArtistID, count)
    			}.toDF("user", "artist", "count")
  	}	
	
}
	

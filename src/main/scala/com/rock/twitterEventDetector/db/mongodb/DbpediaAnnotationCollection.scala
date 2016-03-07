package com.rock.twitterEventDetector.db.mongodb

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.{MongoDBObject, TypeImports}
import com.mongodb.{BasicDBList, BasicDBObject, MongoException}
import com.rock.twitterEventDetector.configuration.Constant._
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.model.Tweets.Tweet
 import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import org.apache.spark.rdd.RDD


/**
  * Created by rocco on 03/02/2016.
  */
object DbpediaAnnotationCollection {

  val db=MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)


  def inserDbpediaAnnotationsBulk(  annotations:List[(Long, List[DbpediaAnnotation])]  )={


    val collection: MongoCollection =db("annotazioniSpark")
    val bulkWrites=collection.initializeUnorderedBulkOperation
     annotations.foreach(annotation=>{
       bulkWrites.insert(MongoDBObject("_id"->annotation._1,"annotations"->annotation._2.map(ann=>ann.toMaps)))
    })

    println(" ADDING "+annotations.size+ " annotations to db")
    bulkWrites.execute(WriteConcern.Unacknowledged)


  }

  def inserDbpediaAnnotationsBulk2(  annotations:Iterator[(Long, List[DbpediaAnnotation])])={
  println(" sto aggiungendo ")

    val collection=MongoCLientSingleton.myMongoClient(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
    val bulkWrites=collection.initializeUnorderedBulkOperation()
    annotations.foreach(annotation=>{
      bulkWrites.insert(MongoDBObject("_id"->annotation._1,"annotations"->annotation._2.map(ann=>ann.toMaps)))
    })

    println(" ADDING "+annotations.size+ " annotations to db")
    bulkWrites.execute()


  }

  /**
    *
    * @param annotations
    */
  def inserDbpediaAnnotations(  annotations:Iterator[(Long, List[DbpediaAnnotation])])={


    val collection=MongoCLientSingleton.myMongoClient(MONGO_DB_NAME).getCollection("viola")
     annotations.foreach(annotation=>{
       try{
       collection.insert(MongoDBObject("_id"->annotation._1,"annotations"->annotation._2.map(ann=>ann.toMaps)))

       }catch {
         case foo: MongoException => println(foo)
       }
    })



  }

  def insertDbpediaAnnotationsOfTweet(idTweet:Long,annotations:List[DbpediaAnnotation])={

    val collection=MongoCLientSingleton.myMongoClient(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
    try{
      collection.insert(MongoDBObject("_id"->idTweet,"annotations"->annotations.map(annotation=>annotation.toMaps)))

    }catch {
      case foo: MongoException => println(foo)
    }


  }


  def onlyRelevantTweets()={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)

    val collectionAnnotation=db("relevantTweets")
    val tweetCollection=db("tweets")
    val onlyReev=db("onlyRelevantTweets")
    collectionAnnotation.par.foreach(x=>

    {

      val query = MongoDBObject("_id" -> x.getAs[Long]("_id").get)
      val tweet: TypeImports.DBObject =tweetCollection.findOne(query).get
      onlyReev.insert(tweet)

    }
    )

  }




  def updateTweetsAnnotated()={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)

    val collectionAnnotation=db("annotazioniSpark")
  collectionAnnotation.par.foreach(x=>

    {

      val query = MongoDBObject("_id" -> x.getAs[Long]("_id").get)
      val update = MongoDBObject(
        "$set" -> MongoDBObject("annotated" -> true)
      )
      val result = db("tweets").update( query, update )
      println(result)

     }
  )

   }



  def insertAnnotazioniRelevants()={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)

    val onlyRelevantTweets=db("onlyRelevantTweets")
    val annotations=db("annotazioniSpark")
    val annotazioniSparkRelevant=db("annotazioniSparkRelevant2")
    onlyRelevantTweets.par.foreach(x=>

    {

      val query = MongoDBObject("_id" -> x.getAs[Long]("_id").get)
      val annotazione=annotations.findOne(query)
      if(!annotazione.isEmpty){

       val result= annotazioniSparkRelevant.insert(annotazione.get,WriteConcern.Unacknowledged)
        println(result)
      }

    }
    )

  }



  def updateTweetsNotAnnotated()={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)
    val query =MongoDBObject(
      "annotated" -> MongoDBObject("$exists" -> false)
    )
    val tweets=db("tweets")
    tweets.find(query).foreach(x=>

    {

      if(!x.containsField("annotated")){
        val query = MongoDBObject("_id" -> x.getAs[Long]("_id").get)

        val update = MongoDBObject(
          "$set" -> MongoDBObject("annotated" -> false)
        )
        val result = db("onlyRelevantTweets").update( query, update )
        println(result)
      }



    }
    )

  }
  /**
    * retrive the annotations of tweet
    * it will reurn Some(of the list made of Dbpedia Annotations object]
    * None if the tweet isn't altready annotated through dbpedia Spootlight
    *
    * @param idTweet
    * @return
    */
  def getAnnotationsOfTweet(idTweet:Long):Option[List[DbpediaAnnotation]]={


    val collection=MongoCLientSingleton.myMongoClient(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
    val result =collection.find(MongoDBObject("_id"->idTweet)).one().asInstanceOf[BasicDBObject]
    if(result==null){
      None
    }else{
  val annotations= result.get("annotations").asInstanceOf[BasicDBList].toList.map(x=>new DbpediaAnnotation(x.asInstanceOf[BasicDBObject]))
        //asInstanceOf[List[BasicDBObject]].map(bson=>new DbpediaAnnotation(bson))

      Some(annotations)
      None
    }
  }
  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @return
    */
  def annotateTweetsAndSave(tweets: RDD[(Long, Tweet)]) = {

    // Load documents (one per line).
   val c=  tweets.mapPartitions {
      it => {
        // val analyzer = new MyAnalyzer()
        val dbpediaSpootligth=new DbpediaSpootLightAnnotator
        val annotations=   it.flatMap { case (idtweet, tweet) =>



          val dbpediaAnnotations =
            dbpediaSpootligth.annotateTweet(tweet).getOrElse(List.empty[DbpediaAnnotation])
          Some(idtweet, dbpediaAnnotations)


        }
        inserDbpediaAnnotations(annotations)

        annotations
      }

    }
    c.count()



  }



  def main(args: Array[String]) {

    //TweetCollection.findTweetInInterval(date1,date2)
     //annotateTweetsAndSave2(tweets)
    //onlyRelevantTweets()
    //updateTweetsNotAnnotated()
    //updateTweetsNotAnnotated()
    //insertAnnotazioniRelevants
    updateTweetsAnnotated()
  }


}

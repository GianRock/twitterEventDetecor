package com.rock.twitterEventDetector.db.mongodb

import java.util.concurrent.Executors

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.{MongoDBObject, TypeImports}
import com.mongodb.{casbah, BasicDBList, BasicDBObject, MongoException}
import com.rock.twitterEventDetector.configuration.Constant._
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.model.Tweets.Tweet
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._
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

  def removeSuffixDbpedia(uriDbpedia:String):String={
    uriDbpedia.substring(28,uriDbpedia.length)
  }
  /**
    *
    */
  def updateInLinkTweets() ={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)
    val inLinkTweets=db("inLinkTweets")
    val collectionAnnotation: MongoCollection =db("annotazioniSpark")

    val executorService = Executors.newFixedThreadPool(50)

    implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
    val i=0
    val hashingTF = new HashingTF(Math.pow(2,24).toInt)
    val futures = collectionAnnotation.find().limit(5).map(x=>Future {

      val id=x.getAs[Long]("_id")
      println("started "+id)
      val annotationList=x.getAs[List[DBObject]]("value").getOrElse(List.empty[DBObject])

      val uris: List[Set[Int]] = annotationList.flatMap{
        x=>
          val uri=(x.getAs[String]("uriDBpedia").get)
          val name= uri.substring(28,uri.length)
          val optionInLinkSet=DbpediaCollection.findDbpediaResourceInLinks(name)
          println(id + " "+optionInLinkSet.get.size)

          optionInLinkSet match {
            case Some(inLinkSet:Set[Int])=>Some(inLinkSet + hashingTF.indexOf(name))
            case None=>None
          }
        //name

        //DbpediaCollection.findDbpediaResourceByName(name).getOrElse(Set.empty[String])
      }

      val tweetInLinks=uris.foldLeft(Set.empty[Int])((r,c) =>r ++ c)
      // val stringInLinks=tweetInLinks.mkString(" ")
      if(uris.size>0){
        val result=MongoDBObject("_id"->id,"inLinks"->tweetInLinks)
        inLinkTweets.insert(result)
      }

      println("ended "+id)
    }
    )

    Future.sequence(futures)

  }


  def updateRelevantTWeets()={
    val db: MongoDB =MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)

    val collectionAnnotation=db("relevantTweets")
    collectionAnnotation.par.foreach(x=>

    {

      val query = MongoDBObject("_id" -> x.getAs[Long]("_id").get)
      val update = MongoDBObject(
        "$set" -> MongoDBObject("cluster" -> x.getAs[Long]("value"))
      )
      val result = db("onlyRelevantTweets").update( query, update )
      println(result)

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
    * retrive the annotations of tweet
    * it will reurn Some(of the list made of Dbpedia Annotations object]
    * None if the tweet isn't altready annotated through dbpedia Spootlight
    *
    * @param idTweet
    * @return
    */
  def getUrisDbpediaOfTweet(idTweet:Long,client:  MongoClient): Option[List[String]] = {


    val collection = client(MONGO_DB_NAME)("annotazioniSpark")
    val result = collection.findOne(MongoDBObject("_id" -> idTweet))
     result match {
      case Some(obj) =>

        val annotations = obj.get("value").asInstanceOf[java.util.List[BasicDBObject]].asScala.toList
        val uris = annotations.flatMap {
          ann =>
            ann.getAs[String]("uriDBpedia")
        }.toSet


        Some(uris.toList)


      case None => None
    }
  }

  def getUrisDbpediaOfTweets(idTweets:Iterable[Long]): Map[Long, Set[String]] = {


    val collection = MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)("annotazioniSpark")
    val results: MongoCursor = collection.find (MongoDBObject("_id" -> MongoDBObject("$in"->idTweets)))

    results.map{
      result=>
        val annotations = result.get("value").asInstanceOf[java.util.List[BasicDBObject]].asScala.toList
        val uris = annotations.map {
          ann =>
            val uri= ann.getAs[String]("uriDBpedia").get
            uri.substring(28,uri.length)
        }.toSet


        (result.getAs[Long]("_id").get,uris)
    }.toMap


  }

  /**
    * retrive the annotations of tweet
    * it will reurn Some(of the list made of Dbpedia Annotations object]
    * None if the tweet isn't altready annotated through dbpedia Spootlight
    *
    * @param idTweet
    * @return
    */
  def getUrisDbpediaOfTweet(idTweet:Long): Option[List[String]] = {
    getUrisDbpediaOfTweet(idTweet, MongoCLientSingleton.myMongoClient)
  }










  /**ee
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



    val c=DbpediaAnnotationCollection.getUrisDbpediaOfTweets(List(255842566090670081L,1L,255835150355664898L))
    println(c)
    /*
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("clustering")
      .set("spark.executor.memory ", "10g")
      .set("spark.driver.maxResultSize","8g")
      .set("spark.local.dir", "/tmp/spark-temp")

    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Long] =sc.parallelize(List(256589114432954368L,255835150355664898L,255835112829222912L,11L,255835112829222912L,255835112829222912L,255835112829222912L,255835112829222912L,255835112829222912L,255835112829222912L,255835112829222912L,1L))

    rdd.map(id=>DbpediaAnnotationCollection.getUrisDbpediaOfTweet(id)).collect().foreach(println)*/
   }
  def main2(args: Array[String]) {

    //TweetCollection.findTweetInInterval(date1,date2)
    //annotateTweetsAndSave2(tweets)
    //onlyRelevantTweets()
    //updateTweetsNotAnnotated()
    //updateTweetsNotAnnotated()
    //insertAnnotazioniRelevants
    //val executorService = Executors.newFixedThreadPool(50)

    // implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

    val futures= updateInLinkTweets()

    /*
           def producer(): Future[List[Unit]] = {

            val list: List[Future[Unit]] = (1 to 100).map {
              i =>
                Future{
                  println("start " + i)
                   Thread.sleep(3000)
                  println("stop " + i)
                }



            }.toList
            Future.sequence(list)
          }*/

    Await.result(futures, Duration.Inf)
    System.exit(0)


  }


}

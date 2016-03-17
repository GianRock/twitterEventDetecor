package com.rock.twitterEventDetector.db.mongodb

import java.util.Date
import java.util.concurrent.Executors

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObject
import com.rock.twitterEventDetector.configuration.Constant
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.model.Tweets.Tweet
 import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.utils.ProprietiesConfig._
import org.joda.time.{DateTime, Period}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
object TweetCollection {

  /*s
  def generateCouplesFromListTailRecursive(lista:List[Long]):Set[(Long,Long)]={

    def generate(list:List[Long],acc:Set[(Long,Long)])= {
      lista match {
        case Nil=>acc
        case head::h2::tail=>
      }

    }
    // require(lista.size>1)
    lista match{
      case Nil=>Nila
      //  case head::Nil=>Nil
      case head::head2::tail=>
        (head,head2)::generateCouplesFromList(head::tail):::generateCouplesFromList(head2::tail)
    }
  }*/




  /**
    * get the min or max date value present in the "tweets" collections
    * more specifically if @minValue is true it will return the min date value,
    * max date  value otherwise
    * the minValue, if is it -1
    *
    * @param minValue
    * @return
    */
  def findMinMaxValueDate(minValue: Boolean = true): Date = {
    val sortingOrder = if (minValue == true) 1 else -1
    val doc = MongoCLientSingleton.myMongoClient(Constant.MONGO_DB_NAME)
      .getCollection("tweets").find(MongoDBObject(), MongoDBObject("_id" -> 0, "created_at" -> 1)).sort(MongoDBObject("created_at" -> sortingOrder)).limit(1).one()

    doc.get("created_at").asInstanceOf[Date]

  }

  /**
    *
    * @param idTweet
    * @return
    */
  def findTweetById(idTweet: Long): Option[Tweet] = {
    val res =MongoCLientSingleton.myMongoClient(Constant.MONGO_DB_NAME).getCollection(Constant.MONGO_TWEET_COLLECTION_NAME).findOne(MongoDBObject("_id" -> idTweet))

    if (res != null) {
      Some(new Tweet(res))

    }
    else None

  }
  /**
    *
    * @param idTweet
    * @return
    */
  def findRelevantTweetById(idTweet: Long): Option[Long] = {
    val res =MongoCLientSingleton.myMongoClient(Constant.MONGO_DB_NAME)("relevantTweets").findOne(MongoDBObject("_id" -> idTweet))
    println(res)
    res match{
      case None=>None
      case Some(x)=>x.getAs[Long]("value")
    }

  }

  /**
    *
    * @param idTweet
    * @return
    */
  def checkRelevant(idTweet: Long): Boolean = {
    val res =MongoCLientSingleton.myMongoClient(Constant.MONGO_DB_NAME)("relevantTweets").count(MongoDBObject("_id" -> idTweet))
     if (res >0) {
     true

    }
    else false

  }

  def countTweetsInTimeInterval(timeStart: Date, timeEnd: Date): String = {
    val collection = MongoCLientSingleton.myMongoClient(Constant.MONGO_DB_NAME).getCollection(Constant.MONGO_TWEET_COLLECTION_NAME)

    //val query2 = MongoDBObject(("foo" -> "bar"), ("baz" -> "qux"))
    val fields = MongoDBObject("_id" -> 1) //"cleaned_text"->1)
    val query = MongoDBObject("$and" -> List(
        MongoDBObject("created_at" -> MongoDBObject("$gte" -> timeStart)),
        MongoDBObject("created_at" -> MongoDBObject("$lt" -> timeEnd))


      ))
    //println(query)
    val results = collection.find(query, fields).sort(MongoDBObject("_id" -> -1)).one()

    "db.dpbediaAnnotations.find({ \"_id\" : NumberLong(\"" + results.get("_id") + "\")}).count()"
    // println(results)

    // val q = "email" $exists true


    //$and( ( "price" $lt 5 $gt 1 ) :: ( "stock" $gte 1 ) )


  }




  def annotateTweets(start: Date, end: Date) = Future {
    //
    println("futures ")
    val tweets = findTweetsBetweenDates(start, end)
    println(tweets.size)
    tweets.size

    if(tweets.size>0)
    {
      val dbpediaSpootLightAnnotator=new DbpediaSpootLightAnnotator

      val annotationList: List[(Long, List[DbpediaAnnotation])] =tweets.map(x => (x.id, dbpediaSpootLightAnnotator.annotateText(x.text).getOrElse(List.empty[DbpediaAnnotation])))
      DbpediaAnnotationCollection.inserDbpediaAnnotationsBulk(annotationList)
    }
    //)
    //annotationList.size

  }

  /**
    *
    * @param start
    * @param end
    * @return
    */
  def findTweetsBetweenDates(start: Date, end: Date): List[Tweet] = {

    println((start,end))
    val collection: MongoCollection =MongoCLientSingleton.myMongoClient(tweetdb)("tweets")

    val q = $and("annotated" $exists  false ,("created_at" $gte start $lt end))
    println(q.toString)
    val result = collection
      .find(q)
    result.map(doc => new Tweet(doc)).toList

  }






  /**
    *
    * @param from
    * @param to
    * @param step
    * @return
    */
  def dateRangeString(from: DateTime, to: DateTime, step: Period)
  = {
    val list = Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to)).toList

    val finalList = if (list(list.size - 1).isBefore(to))
      list ++ List(to)
    else
      list

    //finalList.sliding(2,1).toList.map(x=>(x.head,x.tail.head))
    finalList.sliding(2, 1).toList.map(x => (x.mkString(" ")))

  }


  /**
    *
    * @param from
    * @param to
    * @param step
    * @return
    */
  def dateRange(from: DateTime, to: DateTime, step: Period): List[(Date, Date)]
  = {
    val list = Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to)).toList

    val finalList = if (list(list.size - 1).isBefore(to))
      list ++ List(to)
    else
      list

    finalList.sliding(2, 1).toList.map(x => (x.head.toDate, x.tail.head.toDate))

  }


  def main(args: Array[String]) {




    //TweetCollection.findAllTweets()
    /*
   val tweet= TweetCollection.findTweetById(256230354485145600L)
    tweet match{
      case(Some(x))=>print(x)
      case (None)=>println(" id notFound")
    }
  }*/
    val lista = List(1L, 2L, 3L, 4L)
    println(TweetCollection.findMinMaxValueDate(true))
    println(TweetCollection.findMinMaxValueDate(false))


    val minDateValue = TweetCollection.findMinMaxValueDate()
    val maxDateValue =TweetCollection.findMinMaxValueDate(false)
    //new Date(minDateValue.getTime+3600*6000)

     //

    val diff = maxDateValue.getTime - minDateValue.getTime
    val numberOFHours = math.ceil(diff / 3600000)
    println("NUMBER OF HOURS " + numberOFHours)


    val hourMilliSec = 3.6e+6.toLong


    var timeStartInterval = minDateValue
    var timeEndInterval = new Date(timeStartInterval.getTime + 1)
    val hourSizeInterval = 6
    var interval: Int = 1
    while (timeEndInterval.getTime <= maxDateValue.getTime) {
      timeEndInterval = new Date((hourMilliSec * hourSizeInterval) + timeStartInterval.getTime)
      //  println("INTERVAL "+interval +" ("+timeStartInterval+" - "+timeEndInterval+")")
      interval = interval + 1
      timeStartInterval = new Date(timeEndInterval.getTime + 1)


    }

    val tweet = TweetCollection.findTweetById(256230354485145600L)
   println(tweet)


    val from = new DateTime(minDateValue)
    val to = new DateTime(maxDateValue)
   val itString=dateRangeString(from,to,Period.hours(6))
    println(itString.length)

   // val call:String="./bin/spark-submit   --class com.rock.twitterEventDetector.mongoSpark.MainAnnotator   --master local[*]   provaSpark-assembly-1.0.jar"
    val callJar="java -Xmx20g -cp provaSpark-assembly-1.0.jar com.rock.twitterEventDetector.mongoSpark.MainAnnotator"
    itString.foreach(x=>println(callJar+" "+x+" >> out.log"))



    val iterator: List[(Date, Date)] = dateRange(from, to, Period.hours(1))





    val executorService = Executors.newFixedThreadPool(1000)
    implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
    var i = 1
  //  val call:String="./bin/spark-submit   --class com.rock.twitterEventDetector.mongoSpark.MainAnnotator   --master local[*]   TwitterEventDetectorSpark-assembly-1.0.jar"
    //import ExecutionContext.Implicits.global

/*
    val futures=  iterator.toList.tail.map {
      x =>
        //sprintln(call+" "+x)
        println("INTERVALLO "+i+" "+x)
      i=i+1
      val future=  annotateTweets(x._1, x._2)

        future onFailure {
          case t => println("An error has occured: " + t.getMessage)
        }
        future onSuccess {
          case posts =>  println(posts)
        }
        future


    }
    //val f: Unit =Future.sequence(futures).onSuccess { case i => println(i) }

    for (f <- futures) Await.ready(f, Duration.Inf)



*/

  }


}


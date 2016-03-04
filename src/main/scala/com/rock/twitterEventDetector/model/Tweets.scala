package com.rock.twitterEventDetector.model

import java.util.Date

import com.mongodb.casbah.Imports
import com.mongodb.{BasicDBList, BasicDBObject}
import com.rock.twitterEventDetector.model.Model.{DbpediaResource, DbpediaAnnotation}
import org.apache.spark.mllib.linalg.SparseVector

import com.mongodb.casbah.Imports._

import scala.annotation.tailrec

/**
  * Created by rocco on 12/02/16.
  */
object Tweets {
  def extractSplittedHahsTagFromBson(bsonDoc: BasicDBObject):Option[String]={

    if(bsonDoc.containsField("splitted_hashtags"))
      Some(bsonDoc.getString("splitted_hashtags"))
    else
      None
  }
  def extractSplittedHahsTagFromBson(bsonDoc: DBObject):Option[String]={

    if(bsonDoc.containsField("splitted_hashtags"))
      Some(bsonDoc.get("splitted_hashtags").asInstanceOf[String])
    else
      None
  }
  def extractHashtags(bsonDoc: BasicDBObject):List[String]={
    bsonDoc.get("entities").asInstanceOf[BasicDBObject].get("hashtags").asInstanceOf[BasicDBList].toList.map(x=>x.asInstanceOf[BasicDBObject].getString("text"))
  }
  def extractHashtags(bsonDoc: DBObject):List[String]={
  bsonDoc.getAs[DBObject]("entities").get.getAs[List[DBObject]]("hashtags").map(x=>x.map(hashtag=>hashtag.getAsOrElse[String]("text",""))).getOrElse(List.empty[String])
   // bsonDoc.getAs[DBObject]("entities").get.getAs("hashtags").toList.map(x=>x.asInstanceOf[DBObject].getAs[String]("text").getOrElse(""))
  }


  /**
    *
    * @param id
    * @param text
    * @param createdAt
    * @param hashTags
    * @param splittedHashTags
    */
  case class Tweet(id:Long,text:String,createdAt:Date,hashTags:List[String],splittedHashTags:Option[String]){
    def this(bsonDoc:BasicDBObject) ={



      this(bsonDoc.getLong("_id"),bsonDoc.getString("cleaned_text"),bsonDoc.getDate("created_at"),extractHashtags(bsonDoc),extractSplittedHahsTagFromBson(bsonDoc))

    }
    def this(bsonDoc:DBObject) ={



      this(
        bsonDoc.getAs[Long]("_id").get,
        bsonDoc.getAs[String]("cleaned_text").get,
        bsonDoc.getAs[Date]("created_at").get,
        extractHashtags(bsonDoc),
        extractSplittedHahsTagFromBson(bsonDoc))

    }









    def timeSimilarity(that:Tweet):Double={

      0d

    }
  }

  /**
    *
    * @param text the text of the hashtags
    * @param indices offsets (start,end) of the hashtag refferring to the text of the tweet
    */
  case class HashTag(text:String,indices:Tuple2[Int,Int])
  case class VectorTweet(val tweet:Tweet,val tfIdfVector:SparseVector)
  case class AnnotatedTweet(val tweet:Tweet,val tfIdfVector:SparseVector,val urisDbpedia:List[DbpediaAnnotation])
  case class AnnotatedTweetWithDbpediaResources(val tweet:Tweet,val tfIdfVector:SparseVector, val dbpediaResoruceSet:Set[DbpediaResource])




  def generateCouplesFromList(list:List[Array[Double]])={
    @tailrec
    def generateCoupleTailRec(list:List[Array[Double]], acc: List[(Array[Double],Array[Double])]):List[(Array[Double],Array[Double])]={
      list match {
        case head::Nil=> acc
        case head :: tail =>
          val couples=tail.map(x=>(head,x))

          generateCoupleTailRec(tail, acc ++ couples)
      }
    }

    generateCoupleTailRec(list, List())
  }
}

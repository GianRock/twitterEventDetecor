package com.rock.twitterEventDetector.db.mongodb

import com.mongodb.BasicDBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.{TypeImports, MongoDBObject}
import com.rock.twitterEventDetector.configuration.Constant._
import com.rock.twitterEventDetector.model.{Model, Distances}
import com.rock.twitterEventDetector.model.Model.DbpediaResource
import org.apache.spark.mllib.feature.HashingTF

import scala.collection.JavaConverters._

/**
  * Created by rocco on 03/02/2016.
  */
object DbpediaCollection {


  def findDbpediaResourceByURI(uriDBpedia:String):Option[DbpediaResource]={
    val res =MongoCLientSingleton.myMongoClient("dbpedia").getCollection("pagelinks").findOne(MongoDBObject ("_id"->uriDBpedia))
    if(res!=null){
      val links=res.get("value").asInstanceOf[java.util.List[String]].asScala.toSet



      Some(new DbpediaResource(uriDBpedia,links))

    }
    else None

  }

  /**
    *
    * @param uriDBpedia
    * @return
    */
  def   findDbpediaResourceByName(uriDBpedia:String): Option[Set[String]] ={
    println(uriDBpedia)
    val res: Option[TypeImports.DBObject] =
      MongoCLientSingleton.myMongoClient("tweetEventDataset")("dbpediaInLinksStrings").findOne(MongoDBObject ("_id"->uriDBpedia))
      res match {
      case Some(obj)=>
        Some(obj.get("value").asInstanceOf[String].split(" ").toSet)


      case None=>None
    }


  }

  /**t
    * rimuove il prefisso http://dbpedia.org/resource/
    *
    * @param uriDbpedia
    * @return
    */
  def removeSuffixDbpedia(uriDbpedia:String):String={
    uriDbpedia.substring(28,uriDbpedia.length)
  }
  /**
    *
    * @param uriDBpedia
    * @return
    */
  def   findInLinksByUri(uriDBpedia:String): Option[Set[Int]] ={

    findDbpediaResourceInLinks(removeSuffixDbpedia(uriDBpedia))

  }




  /**
    *
    * @param dbpediaResourceName
    * @return
    */
  def   findDbpediaResourceInLinks(dbpediaResourceName:String): Option[Set[Int]] ={
     val res: Option[TypeImports.DBObject] =
      MongoCLientSingleton.myMongoClient("tweetEventDataset")("dbpediaInLinksHashed").findOne(MongoDBObject ("_id"->dbpediaResourceName))
    res match {
      case Some(obj)=>
        Some(obj.get("value").asInstanceOf[java.util.List[Int]].asScala.toSet)


      case None=>None
    }


  }


  /**
    *
    * @param dbpediaResourceName
    * @return
    */
  def   findDbpediaResourceInLinks(dbpediaResourceName:String,clientMongo:MongoClient): Option[Set[Int]] ={
    val res: Option[TypeImports.DBObject] =
      clientMongo("tweetEventDataset")("dbpediaInLinksHashed").findOne(MongoDBObject ("_id"->dbpediaResourceName))
    res match {
      case Some(obj)=>
        Some(obj.get("value").asInstanceOf[java.util.List[Int]].asScala.toSet)


      case None=>None
    }


  }



  def findDbpediaResourceInLinks(dbpediaNames:Iterable[String]): Map[String, Set[Int]] = {


    val collection = MongoCLientSingleton.myMongoClient(MONGO_DB_NAME)("dbpediaInLinksHashed")
    val results: MongoCursor = collection.find (MongoDBObject("_id" -> MongoDBObject("$in"->dbpediaNames)))

    results.map{
      result=>
        //println(result)
        val inLinks = result.get("value").asInstanceOf[java.util.List[Int]].asScala.toSet
        (result.getAs[String]("_id").get,inLinks)
    }.toMap


  }

  def main(args: Array[String]) {
   // val adam=findDbpediaResourceByURI("Adam_Goodes").get
   // val melbourne=findDbpediaResourceByURI("http://dbpedia.org/resource/Bari").get

//   // val fedSquare=findDbpediaResourceByURI("http://dbpedia.org/resource/Teatro_Petruzzelli").get
//    val a=findDbpediaResourceByName("Adam_Goodes")
//    val b=findDbpediaResourceByName("Federation_Square")
//    val c=findDbpediaResourceByName("Melbourne")
//    val set1: Set[String] =a.get.union(b.get)
//    val set2: Set[String] =a.get.union(c.get)
//
//    val dist=Distances.calculateNormalizedGoogleDistance(set2,set1,13994253)
//    print(1-dist)
    val hashingTF = new HashingTF(Math.pow(2,24).toInt)

    val a=findDbpediaResourceInLinks("Bari").get+ (  hashingTF.indexOf("Bari"))
    val b=findDbpediaResourceInLinks("Teatro_Petruzzelli").get+ (  hashingTF.indexOf("Teatro_Petruzzelli"))
    val dist=Distances.calculateNormalizedGoogleDistanceInt(a,b,13994253)
       print(1-dist)



    val m=findDbpediaResourceInLinks(List("Bari","Teatro_Petruzzelli"))
     println(m)
   }


}

package com.rock.twitterEventDetector.db.mongodb

import com.mongodb.casbah.commons.MongoDBObject
import com.rock.twitterEventDetector.model.Model.DbpediaResource

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

  def main(args: Array[String]) {
    println(findDbpediaResourceByURI("http://dbpedia.org/resource/Capay,_CA"))
  }


}

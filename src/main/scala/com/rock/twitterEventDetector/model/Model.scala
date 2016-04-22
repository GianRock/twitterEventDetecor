
package com.rock.twitterEventDetector.model

import java.io.Serializable
import java.util.Date

import com.mongodb.{BasicDBList, DBObject, BasicDBObject}
import com.mongodb.casbah.commons.MongoDBObject
import com.rock.twitterEventDetector.model.AnnotationType.AnnotationType
import com.rock.twitterEventDetector.model.AnnotationType.AnnotationType
import com.rock.twitterEventDetector.model.{AnnotationType, Similarity}
import org.apache.spark.mllib.linalg.SparseVector
import org.bson.BSONObject
import com.mongodb.casbah.Imports._
import Distances._

/**
  * Created by rocco on 01/02/2016.
  */
object Model {
val WIKIPEDIA_N=4806150 //  11517454




  /**
    *
    * @param uriDBpedia
    * @param inLinks
    */
  case class DbpediaResource(val uriDBpedia:String,val inLinks:Set[String])
    extends Similarity[DbpediaResource] with Serializable{
    override def calculateSimilarity(that:DbpediaResource)=1d-calculateNormalizedGoogleDistance(this.inLinks,that.inLinks,WIKIPEDIA_N)
  }



  case class DbpediaAnnotation(val surfaceText:String, val start:Int, val kindOf:AnnotationType, val uriDBpedia:String){
    def this(bsonDoc:BasicDBObject)=this(bsonDoc.getString("surfaceText"),bsonDoc.getInt("start"),AnnotationType.withName(bsonDoc.getString("kindOf")),bsonDoc.getString("uriDBpedia"))
    def toMaps =dbpediaAnnotationToMap(this)
  }




  def dbpediaAnnotationToMap(dbpediaAnnotation: DbpediaAnnotation) = {

    dbpediaAnnotation match {
      case DbpediaAnnotation(surfaceText, start, kindOf, uriDBpedia)
      =>MongoDBObject("surfaceText"->surfaceText,"start"->start,"kindOf"->kindOf.toString,"uriDBpedia"->uriDBpedia)
      // case _=>  RuntimeException()
    }



  }


}

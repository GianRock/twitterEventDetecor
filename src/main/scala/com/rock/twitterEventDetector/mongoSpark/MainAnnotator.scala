package com.rock.twitterEventDetector.mongoSpark

import java.util

import com.mongodb.DBObject
import com.mongodb.casbah.commons.Imports
import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.model.Tweets.{AnnotatedTweet, Tweet}
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import com.rock.twitterEventDetector.utils.ProprietiesConfig._
import org.apache.spark.graphx._
import com.rock.twitterEventDetector.configuration.Constant
import org.bson.{BSONObject, Document}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration
import com.mongodb. hadoop.{MongoOutputFormat, BSONFileOutputFormat}
import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
  * Created by rocco on 16/02/16.
  */
object MainAnnotator {

  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
     * @return
    */
  def nlpPipeLine(tweets: RDD[(Long, Tweet)]) = {

     // Load documents (one per line).
    val annotatedTweets = tweets mapPartitions {
      it => {
       // val analyzer = new MyAnalyzer()
        val dbpediaSpootligth=new DbpediaSpootLightAnnotator
        it.flatMap { case (idtweet, tweet) =>



          val dbpediaAnnotations =
              dbpediaSpootligth.annotateText(tweet.text).getOrElse(List.empty[DbpediaAnnotation])
            Some(idtweet, dbpediaAnnotations.map(x=>x.toMaps).asJava)


        }
      }
    }


    annotatedTweets
  }
  def main(args: Array[String]) {

    if (args.size < 2)
      throw new IllegalArgumentException(" args must be 2 dates")


    val minDateString = args(0)
    val maxDateString = args(1)


    val mindate = DateTime.parse(minDateString)
    val maxDate = DateTime.parse(maxDateString)




    val conf = new SparkConf().setAppName("MainAnnotator")
      .setMaster("local[*]")
     . set("spark.executor.memory ", "20g")

    val sc = new SparkContext(conf)

    val tweets: RDD[(Long, Tweet)] = SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc, mindate.toDate, maxDate.toDate)
    println(tweets.count())


    val annotations: RDD[(VertexId, util.List[Imports.DBObject])] =nlpPipeLine(tweets)

    val outputConfig = new Configuration()
    val mongoUri=if(auth){
      "mongodb://"+usr+":"+"sparkmongo"+"@"+host+":27017/"+ tweetdb+".annotazioniSpark?authSource="+authdb
    }else{
      "mongodb://"+host+":27017/"+ tweetdb+".annotazioniSpark"
    }
    outputConfig.set("mongo.output.uri",
      mongoUri)




    annotations.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[DBObject],
      classOf[MongoOutputFormat[Object, DBObject]],
      outputConfig)


  }


}

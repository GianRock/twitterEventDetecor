package com.rock.twitterEventDetector.nlp

import java.io.InputStream
import java.util
import javax.annotation.Resource

import com.rock.twitterEventDetector.db.mongodb.TweetCollection
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import edu.stanford.nlp.ling.TaggedWord
import scala.io.Source._
import scala.collection.immutable.TreeMap
import scala.reflect.io.Path

/**
  * Created by rocco on 30/01/2016.
  */
object as {

    def main(args: Array[String]) {
      val annotator = new DbpediaSpootLightAnnotator

      val idList = List(265619505517060096L, 265619505449926656L, 265619505462509568L, 265619505563189249L, 265619505240215552L)
      val tweets = idList.map {
        id => {
          val tweet = TweetCollection.findTweetById(id)
          (tweet, annotator.annotateText(tweet.get.text).getOrElse(List.empty[DbpediaAnnotation]))
        }


      }
      tweets.foreach(println)


    }


}

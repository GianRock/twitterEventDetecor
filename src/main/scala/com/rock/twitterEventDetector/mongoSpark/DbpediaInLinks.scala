package com.rock.twitterEventDetector.mongoSpark

import java.io.{File, PrintWriter}

import com.mongodb.DBObject
import com.mongodb.hadoop.MongoOutputFormat
import com.rock.twitterEventDetector.utils.ProprietiesConfig._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.bson.BSONObject
import org.mapdb.{DB, DBMaker}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import scala.language.implicitConversions
import scala.util.Try

/**
  * Created by rocco on 11/04/16.
  */
object DbpediaInLinks {


  class InLinkPartioner(numParts: Int)  extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int = {
      val bucket = key.asInstanceOf[String]
      Math.abs(bucket.hashCode).toInt %numPartitions

    }

    override def equals(other: Any): Boolean = other match {
      case dnp: InLinkPartioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }

  }

  def removeSuffixDbpedia(uriDbpedia:String):String={
        uriDbpedia.substring(28,uriDbpedia.length)
  }



  def main(args: Array[String]) {

    //val dbUrl: DB = DBMaker.newMemoryDB().transactionDisable().cacheSize(1000000).make()
   // val redirectUrls= dbUrl.createTreeMap("redirectedUrl").valuesOutsideNodesEnable().make()

    try{
    val mongoUri=if(auth){
      "mongodb://"+usr+":"+"sparkmongo"+"@"+host+":27017/"+ tweetdb+".dbpediaInLinksHashed?authSource="+authdb
    }else{
      "mongodb://"+host+":27017/"+ tweetdb+".dbpediaInLinks22"
    }
    println(mongoUri)


    val maxMemory = Try(args(0)).getOrElse("50G")
   val path=Try(args(1)).getOrElse("page_links_en.ttl")
  //val path: String = "/home/rocco/page_links_en.ttl.bz2"
     val conf: SparkConf = new SparkConf()
       .setAppName("Simple Application").setMaster("local[*]")
       .set("spark.executor.memory", maxMemory)
       .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc=new SparkContext(conf)
    val timeStart=System.currentTimeMillis()

    val lines: RDD[String] = sc.textFile(path, 16)
     val inLinks: RDD[(String, String)] =lines.filter(_.startsWith("<")).flatMap{
     line=>




       /**
         * rimuovo le parentesi angolari e http://dbpedia.org/resource/
         */
       try{
         val parts=line.split(" ")
         val inLink= removeSuffixDbpedia(parts(0).substring(1, parts(0).length - 1))
         val outLink=removeSuffixDbpedia(parts(2).substring(1, parts(2).length - 1))

         Some((outLink,inLink))
       }catch {
         case ioe: IndexOutOfBoundsException =>
           print(line)
           None

        }


    }.partitionBy(new InLinkPartioner(64)).persist(StorageLevel.MEMORY_AND_DISK)

    val buf = scala.collection.mutable.ArrayBuffer.empty[String]
    val grouped: RDD[(String, ArrayBuffer[String])] = inLinks.aggregateByKey(buf,inLinks.partitioner.get)(
      (buf, v) => buf += v,
      (set1, set2) => set1 ++= set2)



    /**
      *
      * Per ogni id a raggruppo in un insieme gli id degli oggetti nel viciniato di a
      *
      */
    //val inLinkPiccolo=sc.parallelize(inLinks.take(100000))
    //val inLinksGrouped: RDD[(String, Iterable[String])] =inLinks.groupByKey()
  /*

      inLinks.aggregateByKey(zeroListElem)(
      (list, inLink) => list:+inLink,
      (list1, list2) => list1 ++ list2)
 //  inLinksGrouped.collect().foreach(println)*/
    //.persist(StorageLevel.MEMORY_AND_DISK)

     //val groupedInLinks=inLinks.groupByKey().count()
   // println(groupedInLinks)

     val outputConfig = new Configuration()

    outputConfig.set("mongo.output.uri",
      mongoUri)
     val hashingTF = new HashingTF(Math.pow(2,24).toInt)
   val u=grouped.mapValues{
    list=>list.toSet[String].map(inLink=>hashingTF.indexOf(inLink)).asJava
  }


    u.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)

     val timeEnd=System.currentTimeMillis()
    val executionTime=timeEnd-timeStart
    println(" TEMPO ESECUZIONE"+executionTime)

    }catch {
      case ioe: Exception =>

        val pw = new PrintWriter(new File("fileException.txt"))
        ioe.printStackTrace(pw)
        pw.close();


    }
   }



}

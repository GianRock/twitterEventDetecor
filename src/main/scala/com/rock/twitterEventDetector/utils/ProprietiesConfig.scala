package com.rock.twitterEventDetector.utils

import java.io.FileInputStream

import com.rock.twitterEventDetector.configuration.Constant


/**
  * Created by rocco on 03/03/16.
  */
object ProprietiesConfig {

  val (host, port,auth,usr,psw,authdb,tweetdb) =
    try {
      val prop = new java.util.Properties()

      prop.load(ProprietiesConfig.getClass.getResourceAsStream("/config.properties"))
    // prop.load(new FileInputStream("config.properties"))
      (
      prop.getProperty("mongo.host"),
      prop.getProperty("mongo.port"),
      prop.getProperty("mongo.auth").toBoolean,
      prop.getProperty("mongo.usr"),
      prop.getProperty("mongo.psw"),
      prop.getProperty("mongo.authdb") ,
      prop.getProperty("mongo.tweetDBName") )

     } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }



  val stringURI=if(auth){


    "mongodb://"+usr+":"+"sparkmongo"+"@"+host+":27017/?authSource="+authdb//+"&connectTimeoutMS=3000000"

  }else{
    "mongodb://"+host+":27017"

  }

  def main(args: Array[String]): Unit = {

    println(tweetdb)
  }

}

package com.rock.twitterEventDetector.utils

import java.io.FileInputStream

import com.rock.twitterEventDetector.configuration.Constant


/**
  * Created by rocco on 03/03/16.
  */
object ProprietiesConfig {

  val (host, port,auth,usr,psw,authdb) =
    try {
      val prop = new java.util.Properties()

      prop.load(ProprietiesConfig.getClass.getResourceAsStream("config.properties"))
     // prop.load(new FileInputStream("config.properties"))
      (prop.getProperty("mongo.host"),
      prop.getProperty("mongo.port"),
      prop.getProperty("mongo.auth").toBoolean,
      prop.getProperty("mongo.usr"),
      prop.getProperty("mongo.psw"),
      prop.getProperty("mongo.authdb"))

     } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }




  def main(args: Array[String]): Unit = {

    println(port)
  }

}

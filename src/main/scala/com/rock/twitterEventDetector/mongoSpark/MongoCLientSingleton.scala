package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah.{MongoClientURI, MongoClient}
import com.rock.twitterEventDetector.utils.ProprietiesConfig._
import com.rock.twitterEventDetector.configuration.Constant
/**
  * Created by rocco on 05/02/2016.
  */
object MongoCLientSingleton {



    val stringURI=if(auth){


        "mongodb://"+usr+":"+"sparkmongo"+"@"+host+":27017/?authSource="+authdb

    }else{
        "mongodb://"+host+":27017"

    }

        println(stringURI)
      val clientMongo=MongoClient( MongoClientURI(stringURI))
    //MongoClientOptions mo = MongoClientOptions.builder.connectionsPerHost(100).build;
    //val clientMongo= MongoClient(url,port)
}
/*
object MongoCLientSingleton{
   def apply(url:String=MONGO_URL,port:Int=27017)=new MongoCLientSingleton(url,port)
}*/

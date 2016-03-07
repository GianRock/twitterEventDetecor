package com.rock.twitterEventDetector.db.mongodb

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.rock.twitterEventDetector.utils.ProprietiesConfig._
/**
  * Created by rocco on 05/02/2016.
  */
object MongoCLientSingleton {



    val stringURI=if(auth){


        "mongodb://"+usr+":"+"sparkmongo"+"@"+host+":27017/?authSource="+authdb

    }else{
        "mongodb://"+host+":27017"

    }

       // println(stringURI)
    /**
    * client a mongo
    * in realtà rappresenta un pool di connessioni
    */
     val myMongoClient=MongoClient( MongoClientURI(stringURI))

}

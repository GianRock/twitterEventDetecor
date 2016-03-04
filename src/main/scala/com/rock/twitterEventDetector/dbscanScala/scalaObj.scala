package com.rock.twitterEventDetector.dbscanScala

import java.io.{FileInputStream, ObjectInputStream, ObjectOutputStream, FileOutputStream}

import com.rock.twitterEventDetector.lsh.Hasher
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.Future


/**
  * Created by rocco on 18/02/16.
  */
object scalaObj {
  def main(args: Array[String]) {
    /**
      * val logFile: String = "target/first100100saasasBoool240/hasher/part-00000"

      * val conf: SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[16]").set("spark.executor.memory", "1g")
      * //SparkConf conf = new SparkConf().setAppName("Simple Application");
      * val sc: JavaSparkContext = new JavaSparkContext(conf)
      * val lines: RDD[String] = sc.textFile(logFile)
      * lines.take(10).foreach(println)
      **/
/*
    val c: Vector[Boolean] = (1 to 5000000).map(x => x % 2 == 1).toVector
    // c.foreach(println)
    val oos = new ObjectOutputStream(new FileOutputStream("vectorBol22"))
    oos.writeObject(c)
    oos.close
*/

    // (3) read the object back in

    val ois = new ObjectInputStream(new FileInputStream("vectorBol22"))
    val obj = ois.readObject().asInstanceOf[Vector[Boolean]]
    println(obj.size)
    // obj.foreach(println)

  /* val cas = for(i <- 1 to 1000){

      b<-Future Hasher(Math.pow(2, 19).toInt))

      //(1 to Math.pow(2, 19).toInt).map(x => as.apply(x)).foreach(println)
    }yield(b)*/


  }
}
package com.rock.twitterEventDetector.nlp;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class DbpediaApp {
    public static void main(String[] args) {
        String logFile = "/home/rocco/page_links_en.ttl.bz2"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set("spark.executor.memory","8g");
        //SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);






        JavaRDD<String> lines = sc.textFile(logFile, 16);
        //  JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        JavaPairRDD<String,String> dataSet= lines.mapToPair(line->{String[] parts=line.split(" ");

            String a="";
            String b="";
            try {
                /**
                 * rimuovo le parentesi angolari
                 */
                a=parts[0].substring(1,parts[0].length()-1);
                b=parts[2].substring(1,parts[2].length()-1);
            } catch (Exception e) {
                // TODO: handle exception
                System.out.println(line);
            }


            return new Tuple2<String,String>(a,b); });


        JavaPairRDD<String,List<String>> dataSetList=dataSet.combineByKey(

                x->{List<String> acc=new LinkedList<String>(); acc.add(x); return acc;    },
                (acc, x)->{acc.add(x); return acc;},
                (acc1, acc2)->{acc1.addAll(acc2); return acc1;}
        );
        Configuration outputConfig = new Configuration();

String uri=
        "mongodb://kdde:sparkmongo@193.204.187.132:27017/tweetEventDataset.dbpediaInLinks?authSource=admin";
        outputConfig.set("mongo.output.uri",
                uri);
        dataSetList.saveAsNewAPIHadoopFile(
                "file:///this-is-completely-unused",
                String.class,
                BSONObject.class,
                MongoOutputFormat.class,
                outputConfig
        );
        ///System.out.println(lines.count());
        sc.close();
    }
}
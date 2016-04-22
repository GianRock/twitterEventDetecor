import sbt._
import Keys._

name := "provaSpark"

version := "1.0"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/",

  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven")

libraryDependencies++=Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided" ,
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.2",
  "com.ankurdave" %"part_2.10" % "0.1",
  "amplab" % "spark-indexedrdd" % "0.3" ,
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2",
  "org.mongodb" % "casbah_2.10" % "3.1.0",
   "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.2",
  "org.apache.httpcomponents" % "httpcore" % "4.2.4",
  "org.apache.httpcomponents" % "httpclient" % "4.2.4" ,
  "org.apache.commons" % "commons-pool2" % "2.3",
  "org.apache.lucene" % "lucene-core" % "5.2.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "5.2.1",
  "org.apache.lucene" % "lucene-queryparser" % "5.2.1",
  "org.json" % "json" % "20151123",
  "org.mapdb" % "mapdb" % "1.0.6"

).map(_.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))).map(_.excludeAll(ExclusionRule(organization = "javax.servlet")))

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*)  => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*)             => MergeStrategy.first
  case PathList("com", "google", xs @ _*)            => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
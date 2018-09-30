name := "scala-git"
version := "0.1"
scalaVersion := "2.11.12"

//resourceDirectory in Compile := baseDirectory.value / "resources"
val sparkVersion = "2.2.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "0.10.0.1",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "mysql" % "mysql-connector-java" % "5.1.46",
  //spary-json
  "io.spray" %% "spray-json" % "1.3.4",
  "org.json4s" %% "json4s-jackson" % "3.6.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.0",

  "com.alibaba" % "druid" % "1.1.10"
  //"com.alibaba" % "fastjson" % "1.2.47",
  /**
    * jackson

  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.6",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6"
    */
)
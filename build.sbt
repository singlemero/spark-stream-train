name := "scala-git"
version := "0.1"
scalaVersion := "2.11.12"

//resourceDirectory in Compile := baseDirectory.value / "resources"
val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion ,
  "org.apache.kafka" % "kafka-clients" % "0.10.2.2",
  "mysql" % "mysql-connector-java" % "5.1.46",
  //json4s
  "org.json4s" %% "json4s-jackson" % "3.6.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.0",
  "com.zaxxer" % "HikariCP" % "3.2.0",
  "io.lettuce" % "lettuce-core" % "5.1.3.RELEASE",
"org.scalanlp" %% "breeze" % "1.0-RC2"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  //case "application.conf"                            => MergeStrategy.concat
  //case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
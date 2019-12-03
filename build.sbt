name := "data-mover"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
libraryDependencies ++= Seq(
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11" % "provided"
)

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "5.5.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.7"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"



libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"



assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
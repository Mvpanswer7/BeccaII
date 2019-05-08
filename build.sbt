name := "xmatrix"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"

// sbt assembly 时, 排除lib下的本地包, spark集群的外部依赖包统一归档存储
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "tispark-core-2.0-SNAPSHOT-jar-with-dependencies.jar"}
  cp filter {_.data.getName == "clickhouse-jdbc-0.1-SNAPSHOT.jar"}
  cp filter {_.data.getName == "apache-carbondata-1.5.0-bin-spark2.3.2-hadoop2.7.2.jar"}
}

dependencyOverrides += "org.lz4" % "lz4" % "1.3.0" % "provided"

// for scala
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.10",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.10" % Test,
  "com.typesafe.akka" %% "akka-http" % "10.1.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.11",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.1.1" % Test,
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  "de.heikoseeberger" %% "akka-http-json4s" % "1.20.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

// for java
libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17" % "provided",
  "org.antlr" % "antlr4-runtime" % "4.7.1" % "provided",
  "org.joda" % "joda-convert" % "1.8.1",
  "joda-time" % "joda-time" % "2.9.3" % "provided",
  "org.apache.velocity" % "velocity-engine-core" % "2.0",
  "org.ansj" % "ansj_seg" % "5.1.6",
  "org.nlpcn" % "nlp-lang" % "1.7.7",
  "org.tensorflow" % "libtensorflow" % "1.5.0-rc1",
  "com.google.guava" % "guava" % "11.0.2",
  "org.tensorflow" % "libtensorflow_jni_gpu" % "1.5.0-rc1",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7" ,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7" ,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
)

// for spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
)

// utils
libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.22" % "provided",
  "org.elasticsearch" % "elasticsearch-hadoop" % "6.3.0" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided" ,
  "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided" ,
  "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided" ,
  "net.liftweb" %% "lift-json" % "3.2.0",
  "com.alibaba" % "druid" % "1.1.12",
  "org.apache.kudu" %% "kudu-spark2" % "1.7.1" % "provided"
)

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case "reference.conf" => MergeStrategy.concat
//  case x => MergeStrategy.first
//}

publishMavenStyle := true

crossPaths := false

publishTo := {
  val nexus = "http://192.168.48.15:8081/repository/maven-releases/"
    Some("snapshot"  at nexus + "")
}

credentials += Credentials("Sonatype Nexus Repository Manager", "192.168.48.15", "admin", "admin123")

//assemblyJarName in assembly := s"xmatrix-${(version in ThisBuild).value}_${sparkVersion}.jar"

updateOptions := updateOptions.value.withGigahorse(false)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("release"))
}

addArtifact(artifact in (Compile, assembly), assembly)

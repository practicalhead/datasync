name := "hive-cross-cluster-transfer"
version := "1.0.0"
scalaVersion := "2.12.15"

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.hive" % "hive-jdbc" % "3.1.3" % "provided",
  "com.typesafe" % "config" % "1.4.2",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Cloudera repository for CDH/CDP dependencies
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

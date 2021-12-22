name := "lab2-spark-scala-bq"
version := "1.0.1"
scalaVersion := "2.12.11"
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.22.2" % "provided",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "hadoop3-1.2.0" % "provided",
  "com.github.scopt" %% "scopt" % "4.0.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

name := "bd-processing-project"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion)
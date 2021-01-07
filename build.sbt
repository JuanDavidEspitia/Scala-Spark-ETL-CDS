import sbt.Keys.libraryDependencies
name := "Scala-Spark-ETL-CDS"

crossScalaVersions := Seq("2.11.12", "2.12.10")
version := "0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided"


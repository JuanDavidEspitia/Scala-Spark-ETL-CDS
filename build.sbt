name := "Scala-Spark-ETL-CDS"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided

)


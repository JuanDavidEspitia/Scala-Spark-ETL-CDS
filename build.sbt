name := "Scala-Spark-ETL-CDS"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming"% sparkVersion % Provided,
  "com.databricks" %% "spark-csv" % "1.5.0" % Provided,
  "com.crealytics" %% "spark-excel" % "0.8.3"

)


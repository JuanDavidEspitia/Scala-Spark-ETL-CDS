package com.spark.cds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.functions
import scala.sys.process._

object DataIngest
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)
    // Creacion de la variablee para punto de partida de la aplicacion
    val spark = SparkSession.builder().appName("DataIngest-CDS").master("local[*]").getOrCreate()
    // Declaramos la variable de tiempo para calcular cuando se demora la ejecucion del artefacto
    val startTimeMillis = System.currentTimeMillis()
    println(startTimeMillis) // Imprimimos el tiempo de inicio en segundos

    /*
    * Declaramos los parametros -> Rutas
    */

    val delimiter = "|"
    val path_clientes = "input/ods_clientes*.csv"
    val path_producto = "input/ods_productos_2020.csv"
    val path_homologacion_doc = "input/homologacion_documentos.csv"

    var dfClientes = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", true)
      .option("inferSchema", true)
      .option("nullValue", "null")
      .option("mergeSchema", "true")
      .load(path_clientes)
    dfClientes.show(5)
    println("Cantidad de registros cargados en memoria: " + dfClientes.count())

    /*
     * Otra forma de leer los archivos CSV
    val dfClientes = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("mergeSchema", "true")
      .csv("input/ods_clientes*.csv")
    dfClientes.show()

     */
    var dfProductos = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", true)
      .option("inferSchema", true)
      .option("nullValue", "null")
      .option("mergeSchema", "true")
      .load(path_producto)
    dfProductos.show(5)

    var dfHomologacionDoc = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", true)
      .option("inferSchema", true)
      .option("nullValue", "null")
      .option("mergeSchema", "true")
      .load(path_homologacion_doc)
    dfHomologacionDoc.show(5)





    val endTimeMillis = System.currentTimeMillis()
    println(endTimeMillis)

  }

}

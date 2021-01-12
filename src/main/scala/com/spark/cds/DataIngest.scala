package com.spark.cds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._


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

    println("******************************************************* \n" +
            "           EXTRACCION DE LOS ARCHIVOS PLANOS            \n" +
            "******************************************************* \n" )

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

    println("******************************************************* \n" +
            "        LIMPIEZA DE ENCABEZADOS EN LOS DATAFRAMES       \n" +
            "******************************************************* \n" )

    /*
    Creamos un ciclo para limpiar los espacios en los encabezados
    */
    for (field <- dfClientes.columns)
    {
      dfClientes = dfClientes.withColumnRenamed(field,field.replaceAll(" ", "_"))
    }
    dfClientes.show(2)

    /*
    Del dataframe de producto, creamos una nueva columna, para la descripcion del estado
     */
    dfProductos = dfProductos.withColumn("DESC_ESTADO", when(col("ESTADO") === "1","Activo")
      .when(col("ESTADO") === "0","Inactivo")
      .otherwise("Unknown"))
    dfProductos.show(5)

    /*
    * Para el dataframe de homologacion documento, renombramos la columna VALOR
    */
    dfHomologacionDoc =dfHomologacionDoc.withColumnRenamed("VALOR", "TIPO_IDENTIFICACION")
      .withColumnRenamed("TIPO_ID", "ID_TIPO")
    dfHomologacionDoc.show(1)

    /*
    * Ahora agregamos una nueva columna a dfHomologacion con la descripcion
    * de cada ID_TIPO
    */
    dfHomologacionDoc = dfHomologacionDoc.withColumn("DESCRIPCION",
         when(col("ID_TIPO")==="1", "Cedula de Ciudadania")
        .when(col("ID_TIPO")==="2", "Cedula de Extrangeria")
        .when(col("ID_TIPO")==="3", "Pasaporte")
        .when(col("ID_TIPO")==="4", "Tarjeta de Identidad"))
    dfHomologacionDoc.show()

    /*
    Cruzamos el dfClientes con dfHomologacion para cambiar los valores de idTipo
    Por tipo de identificacion
    */

    var dfClientesHomologados = dfClientes.as("a").join(dfHomologacionDoc.as("b"),
      col("a.TIPO_ID") === col("b.ID_TIPO"),"inner")
      .select(col("a.ID_CLIENTE"),
        col("b.TIPO_IDENTIFICACION"),
        col("a.NOMBRE_CLIENTE").as("NOMBRE"),
        col("a.CORREO_ELECTRONICO").as("EMAIL"),
        col("a.CIUDAD"),
        col("a.NUMERO_CUENTA"),
        col("a.FECHA_ACTIVACION"),
        col("ID_PROUCTO"))
    dfClientesHomologados.show()

    /**
     * Cruzamos el df de Clientes ya homologados con los productos, para conocer cual es el
     * producto que le correspode a cada cliente.
     *
     * Realizamos los cruces mediante SQL Join
     * */
    // SQL Join
    dfClientesHomologados.createOrReplaceTempView("CLIENTES")
    dfProductos.createOrReplaceTempView("PRODUCTOS")
    var dfClienteProducto = spark.sql("SELECT * FROM CLIENTES a INNER JOIN PRODUCTOS b ON a.ID_PROUCTO == b.ID_PRODUCTO")
    println("Join de Clientes Homolagados con productos")
    dfClienteProducto.show(false)

    /**
     * Añadimos una nueva columna llamada esquema, que dependiendo del estado
     * le añade el valor "Maestra" o "Transaccion", para identificar
     * el esquema origen y en que carpeta de particion destino debe quedar
     * */
    dfClienteProducto = dfClienteProducto.withColumn("SCHEMA",
      expr("case when ESTADO = '0' then 'Maestro' " +
        "when ESTADO = '1' then 'Transaccion' " +
        "else 'N/A' end"))
    dfClienteProducto.show(false)

    /**
     * Procedemos a concatenar las columnas TIPO_IDENTIFICACION+ID_CLIENTE, para
     * ofuscarlo con el algoritmo sha256
     * */









    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis)/1000
    println("El tiempo empleado en segundos para el procesamiento es: " + durationSeconds + "sg")

  }

}

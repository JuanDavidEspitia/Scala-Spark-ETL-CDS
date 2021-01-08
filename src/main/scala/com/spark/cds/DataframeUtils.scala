package com.spark.cds

import java.util.Arrays
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import javax.crypto.spec.IvParameterSpec
import org.apache.commons.codec.binary.Base64.decodeBase64
import org.apache.commons.codec.binary.Base64.encodeBase64
import com.spark.cds.Utils._

//import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.functions.{col, lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataframeUtils {
  val tableSecurity ="[esquema].[tabla]"

  implicit class DataframeValidation(dataFrame: DataFrame) {

    //Función para obtener el nombre de las columas no null
    def getSchemaNnotNull: List[String] = {
      val schemaNnotNull = dataFrame.schema.toList.flatMap(x => if (!x.nullable) Some(x.name.toLowerCase) else None)
      return schemaNnotNull
    }

    //Función para obtener el nombre de las columas y el tipo de dato
    def getSchemaNT: List[String] = {
      val schemaNT = dataFrame.schema.toList.map(x => x.name.toLowerCase + "-!-" + x.dataType.typeName)
      return schemaNT
    }

    //Función para obtener el nombre de las columas
    def getSchemaN: List[String] = {
      val schemaN = dataFrame.schema.toList.map(x => x.name.toLowerCase)
      return schemaN
    }

    //Función para añadir columna con una condición
    def addColumn(nameColumn: String, condition: org.apache.spark.sql.Column): DataFrame = {
      val spark = SparkSession.builder.appName(nameApp).master("local[*]").getOrCreate()

      val dfResult = dataFrame.withColumn(nameColumn, condition)
      printInfo("Add column " + nameColumn)
      dfResult
    }

    //Función para convertir una columna a otro datetype
    def castColumn(column: String, dataTypeDestiny: DataType): DataFrame = {
      var df = dataFrame
      //Nombre columna temporal
      var columnNew = column + "_new"
      try {
        //Creación df temporal
        var tempDf1 = dataFrame.withColumn(columnNew, df(column).cast(dataTypeDestiny))

        tempDf1 = tempDf1.filter(column + " IS DISTINCT FROM " + columnNew + " OR " + columnNew + " is null")


        //Validar si en la conversión se cambian o pierden datos
        if (tempDf1.count > 0) {
          printError("Information will be lost for the following records")
          tempDf1.show
        }
        //Convertir columna
        df = dataFrame.withColumn(column, dataFrame(column).cast(dataTypeDestiny))
        printWarn(column + " transform to " + dataTypeDestiny)

      } catch {
        case _: Throwable =>
          //En caso de error se atrapa la excepción
          printError("It is not possible to convert the values of column '" + column + "' to '" + dataTypeDestiny + "', the values will be left in null", true)
          df = dataFrame.withColumn(column, lit(null: String).cast(dataTypeDestiny))

      }
      return df
    }

    //Función para guardar dataframe con saveastable
    def saveDataFrame(tableFormat: String, outTableName: String, locationTable: String, partitionTableList: Array[String], tableExist: Boolean = false) = {
      try {
        //Inicializar variables
        //var partitionTableList = new Array[String](0)
        //Validar si esta vacio
        if (dataFrame.count > 0) {
          // Actualizar bitacora
          bitacora.updateRowsDestiny(dataFrame.count.toInt)
          printInfo(s"Writing table '$outTableName' on hive")

          //Iniciar writer df
          var writeDf = dataFrame.write.format(tableFormat).mode("append")
          // Si no existe la tabla se añade location de donde se guardara
          if (!tableExist) {
            writeDf = writeDf.option("path", locationTable)
          }
          //Si se le crean particiones en la escritura se
          if (partitionTableList.nonEmpty) {
            printInfo("Validacion particiones")
            writeDf = writeDf.partitionBy(partitionTableList: _*)
          }
          dataFrame.show(2)
          writeDf.saveAsTable(outTableName)
          printInfo("Writing completed!")
        } else {
          printError("It was not possible to write the information, Dataframe is empty", true, true)
        }
      } catch {
        case e: Exception =>
          printError("It was not possible to write the information:  \n" + e, true, true)
      }
    }

    //Función para guardar dataframe con inserttable
    def insertDataFrame(outTableName: String, dfDestino: DataFrame = null) = {
      try {
        var dfInsert = dataFrame

        //Validar si esta vacio
        if (dfInsert.count > 0) {
          spark.sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
          bitacora.updateRowsDestiny(dfInsert.count.toInt)
          //writing table
          printInfo(s"Writing table '$outTableName' on Hive")
          if (dfDestino != null) {
            //Crear lista de columnas ordenadas segun el esquema de destino
            val schemaDestCol = dfDestino.schema.toList.map(x => col(x.name))
            dfInsert = dfInsert.select(schemaDestCol: _*)
          }
          dfInsert.write.mode("append").insertInto(outTableName)

          printInfo("Writing completed!", true)
        } else {
          printError("It was not possible to write the information, Dataframe is empty", true, true)
        }

      } catch {
        case e: Exception =>
          if (outTableName != bitacora.table) {
            printError("It was not possible to write the information:" + e, false, true)
          }
          else {
            printError("It was not possible to write the information:" + e)
            System.exit(1)
          }
      }
    }
  }

  def takeAesColumn(Column: String, dataFrame: DataFrame, key: String, cont: Int, exitError: Boolean = false): DataFrame = {
    printInfo("takeAesColumn")
    dataFrame.show(10)
    val coder: (String => String) = (arg: String) => {
      if (arg != null) {
        //cryptTest(key, arg)
        crypt(key, arg)
      } else {
        ""
      }
    }
    printInfo(s"VALOR LLAVE $key")
    printInfo(s"LLAVE BYTES ${key.getBytes()}")
    printInfo(s"TAMANIO LLAVE ${key.getBytes().length}")
    val sqlfunc = udf(coder)
    val newdf = dataFrame.withColumn("AESValue", sqlfunc(col(Column.toString)))
    printInfo("RESULTADO TAKE AES COLUMN ")
    newdf.show(1)
    newdf
  }

  def decrypt(key1: String, encrypted: String): String = {
    val skeySpec = new SecretKeySpec(key1.getBytes(), 0, key1.getBytes().length, "AES")
    val cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.DECRYPT_MODE, skeySpec)
    val original = cipher.doFinal(decodeBase64(encrypted))
    new String(original)
  }

  def crypt(key1: String, valor: String): String = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val skeySpec = new SecretKeySpec(key1.getBytes(), "AES")
    val ivParameterSpec = new IvParameterSpec(Arrays.copyOf(key1.getBytes(), 16))
    cipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivParameterSpec)
    //cipher.init(Cipher.ENCRYPT_MODE, skeySpec)

    val crypted = encodeBase64(cipher.doFinal(valor.getBytes("UTF-8")))
    new String(crypted)

  }

  def cryptTest(key1: String, valor: String): String = {
    "UDF PRUEBA UDF"
  }

  /**
   * Function that performs the homologation of the document type field.
   */
  /*def homologation(dataFrame: DataFrame, tableHom: String, ColumnIn: String, ColumnOut: String): DataFrame = {
    var homologationDf = spark.emptyDataFrame
    var newdf = dataFrame
    try {
      printInfo(s"query homologacion a ejecutar select * from $tableHom")
      homologationDf = spark.sqlContext.sql(s"select codigo_id, homologacion, tabla, columna from $tableHom")
    }
    catch {
      case _: Throwable =>
        printError(s"$tableHom no exiexists.", true)
    }

    newdf = dataFrame.join(homologationDf,
      col(ColumnIn) === homologationDf.col("A") ||
        col(ColumnIn) === homologationDf.col("B") ||
        col(ColumnIn) === homologationDf.col("C") ||
        col(ColumnIn) === homologationDf.col("D") ||
        col(ColumnIn) === homologationDf.col("E") ||
        col(ColumnIn) === homologationDf.col("F") ||
        col(ColumnIn) === homologationDf.col("G"), "inner")
      .select(dataFrame.col("*"),
        homologationDf.col(ColumnOut)
      ).drop(s"$ColumnIn").withColumnRenamed(ColumnOut, s"$ColumnIn")

    newdf
  }*/

  /**
   * Funcion que obtiene los valores a homologar por tabla
   * @param tableHom
   * @param tableIn
   */
  def getDataHomologation(tableHom: String, tableIn: String): DataFrame ={
    var homologationDf = spark.emptyDataFrame
    val tableRules = tableIn.toUpperCase
    printInfo(s"Reglas a consultar $tableRules")
    try {
      printInfo(s"query homologacion a ejecutar select distinct codigo_id, homologacion, tabla from $tableHom where tabla =  '$tableRules'")
      homologationDf = spark.sqlContext.sql(s"select distinct codigo_id, homologacion, tabla from $tableHom where tabla =  '$tableRules'")
    }
    catch {
      case _: Throwable =>
        printError(s"$tableHom no exiexists.", true)
    }
    homologationDf
  }

  def homologation(dataFrame: DataFrame, homologationDf: DataFrame, ColumnIn: String): DataFrame = {
    printInfo(s"Inicia proceso de homologacion")
    printInfo(s"Columna a trabajar $ColumnIn")
    var newdf = dataFrame
    try {
      printInfo(s"Creando newdt")
      newdf = dataFrame.join(homologationDf,
        col(ColumnIn) === homologationDf.col("homologacion"),"inner")
        .select(dataFrame.col("*"),
          homologationDf.col("codigo_id")
        ).drop(s"$ColumnIn").withColumnRenamed("codigo_id", s"$ColumnIn")
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        printError(s"Is not possible homologation process. ${t.getMessage}", true, true)
    }
    newdf
  }

  /**
   * Funcion encargada de obtener la tabla a trabajar
   * @param tableIn
   * @param condition
   * @return
   */
  def getDataTable(tableIn: String, condition: String): DataFrame = {
    var df = spark.emptyDataFrame
    try {
      printInfo(s"query a ejecutar select * from $tableIn $condition")
      df = spark.sqlContext.sql(s"select * from $tableIn $condition").persist()
      printInfo(s"dataFrame obtenido $df")
    } catch {
      case _: Throwable =>
        printError("Table no exists.", true, true)
    }
    df
  }

  /**
   * Funcion encargada de actualizar el flag a 1
   * @param tableIn
   * @param condition
   */
  def setUpdateDataTable(tableIn: String, condition: String): Unit = {
    try {
      printInfo(s"query a ejecutar update $tableIn set flag = '1' $condition")
      spark.sqlContext.sql(s"update $tableIn set flag = '1' $condition")
    } catch {
      case _: Throwable =>
        printError("Table no exists.", true, true)
    }
  }

  /**
   * Function responsible for performing hashing and encryption in Hive
   *
   * @param hashcolumn
   * @param tableIn
   * @param key
   * @param dataTable
   * @return
   */
  /*def applyHashAes(hashcolumn: Array[String], tableIn: String, condition: String, key: String, dataTable: DataFrame): DataFrame = {
    //dataTable.createOrReplaceTempView("tmpData")
    printInfo("Include Hashing - AES")
    printInfo("Validando conexion Secretos Google")
    printError("Error consumo Secretos Google")
    printWarn("Realizando encripcion de datos con llave de entrada")
    var df: DataFrame = null
    try {
      if (hashcolumn != null) {
        var shacol = "sha2("
        var aes = "base64(aes_encrypt("
        var concatCol = "concat("
        for (x <- hashcolumn) {
          concatCol += s"cast($x as string), "
        }
        concatCol = concatCol.substring(0, concatCol.length - 2) + ")"
        shacol += concatCol + ", 256) as HashValue"
        aes += concatCol + s", '$key')) as AESValue"
        printInfo(s"Columna Hash ${shacol}")
        printInfo(s"Columna Hash ${key}")
        printInfo(s"Consulta hash a ejecutar: select *, ${shacol}, ${aes} from $tableIn")
        //df = spark.sql(s" select *, ${shacol}, ${aes} from $tableIn").drop(hashcolumn(0)).drop(hashcolumn(1))
        df = spark.sql(s" select *, ${shacol}, ${concatCol} as valorconcat from $tableIn")
        df.show(1)
        //for (x <- hashcolumn) {
        df = DataframeUtils.takeAesColumn("valorconcat", df, key) //.drop(hashcolumn(0)).drop(hashcolumn(1))
        //}
        printInfo(s"dataframe daliendo de takeAesColumn")
        df.show(1)
        printInfo(s"Include Hashing - AES  ${df.count.toInt} total rows ")
      }
    } catch {
      case e: Exception =>
        printError("Is not possible apply hash data: " + e, true)
    }
    df
  }*/


  def applyHashAes(hashcolumn: Array[Array[String]], tableIn: String, condition: String, key: String, dataTable: DataFrame): DataFrame = {
    //dataTable.createOrReplaceTempView("tmpData")
    printInfo("Include Hashing - AES")
    printInfo("Validando conexion Secretos Google")
    printError("Error consumo Secretos Google")
    printWarn("Realizando encripcion de datos con llave de entrada")
    var df: DataFrame = null
    try {
      if (hashcolumn != null && hashcolumn.length > 0) {
        printInfo(s"TAMANIO hashcolumn ${hashcolumn.length}")
        var cont = 1
        var columns = new String
        var columnConcat = new String
        hashcolumn.foreach{u =>
          printInfo(s"TAMANIO hashcolumn(u) ${u.length}")
          var shacol = "sha2("
          var aes = "base64(aes_encrypt("
          var concatCol = "concat("
          u.foreach{x =>
            printError(s"VALOR X ${x}")
            concatCol += s"cast($x as string), "
          }
          columnConcat += s"valorconcat$cont,"
          concatCol = concatCol.substring(0, concatCol.length - 2) + ")"
          shacol += concatCol + s", 256) as HashValue$cont, "
          aes += concatCol + s", '$key')) as AESValue$cont, "
          concatCol += s" as valorconcat$cont, "
          columns += shacol + concatCol
          cont = cont+1
        }

        columns = columns.substring(0, columns.length()-2)
        columnConcat = columnConcat.substring(0, columnConcat.length()-1)
        val arrColumnConcat = columnConcat.split(",")

        printInfo(s"COLUMNAS PARA LA ENCRIPCION ${columns}")
        printInfo(s"QUERY A EJECUTAR CONSULTA select *, ${columns} from $tableIn")
        //df = spark.sql(s" select *, ${shacol}, ${aes} from $tableIn").drop(hashcolumn(0)).drop(hashcolumn(1))
        //dataTable.createOrReplaceTempView("tmptable")
        df = spark.sql(s" select *, ${columns} from $tableIn")
        //df = spark.sql(s" select *, ${columns} from tmptable")
        df.show(1)
        df.createOrReplaceTempView("tmptable")
        df = spark.sql(s"select * from tmptable")
        printInfo(s"DF ANTES DE TAKECOLUMN")
        df.show(1)
        cont = 1
        for (x <- arrColumnConcat) {
          df = DataframeUtils.takeAesColumn(x, df, key,cont) //.drop(hashcolumn(0)).drop(hashcolumn(1))
          cont = cont+1
        }
        printInfo(s"dataframe saliendo de takeAesColumn")
        df.show(1)
        printInfo(s"Include Hashing - AES  ${df.count.toInt} total rows ")
      }else{
        printInfo("Is not possible apply hash data, no data into array: ", true)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        printError("Error al aplicar HASH: "+e.getMessage)
        printError("Is not possible apply hash data: " + e, true)
    }
    df
  }

  /**
   *
   * @param df
   * @param paramsValid
   * @param outTableName
   * @param tableFormat
   * @param locationTable
   */
  def moveData(df: DataFrame, paramsValid: ParamsClass, outTableName: String, tableFormat: String, locationTable: String, partitionTableList: Array[String]): Unit = {

    try {
      //Valida si existe la tabla
      if (paramsValid.validTableExist(outTableName)) {
        // Si existe la tabla, se valida que los esquemas sean iguales
        val dfDestiny = spark.sqlContext.sql(s"select * from $outTableName limit 1")
        if (dfDestiny.schema == df.schema) {
          df.insertDataFrame(outTableName)
        }
        else {
          printError("Validate the scheme of the destination table, it is not equal to the destiny", true, true)
        }
      }
      else {
        // Si no existe la tabla, la copia sin problema
        printWarn(s"Create table $outTableName")
        printInfo(s"$tableFormat, $outTableName, $locationTable")
        df.saveDataFrame(tableFormat, outTableName, locationTable, partitionTableList)
      }
    } catch {
      case e: Exception =>
        printError("Validate access, is not possible create data:  \n" + e)
    }
  }


  /**
   * Function responsible for obtaining information from the safety table
   *
   * @return
   */
  def getDataSecurityTable(): DataFrame = {
    var seguridadData = spark.emptyDataFrame
    try {
      if (spark.catalog.tableExists(tableSecurity)) {
        seguridadData = spark.sqlContext.sql(s"select * from $tableSecurity")
      } else {
        printError("Validate existence of the table '" + tableSecurity + "', is not found", true, true)
      }
    } catch {
      case e: Exception =>
        printError("Validate access, is not possible consult security data :  \n" + e, true)
    }
    seguridadData
  }

  /**
   * Function responsible for insert information from the safety table
   *
   * @param seguridadData
   */
  def setDataSecurityTable(seguridadData: DataFrame): Unit = {
    try {
      if (spark.catalog.tableExists(tableSecurity)) {
        seguridadData.insertDataFrame(tableSecurity)
      } else {
        printError("Validate existence of the table '" + tableSecurity + "', is not found", true, false)
      }
    } catch {
      case e: Exception =>
        printError("Validate access, is not possible insert security data :  \n" + e, true)
    }
  }

  /**
   * Function responsible for get information to insert the safety table
   *
   * @param dfSeguridad
   * @param dfData
   * @return
   */
  def getHashAEStoInsert(dfSeguridad: DataFrame, dfData: DataFrame): DataFrame = {
    printInfo("Inicio Join data vs seguridad")
    dfData.show(1)
    printInfo("Inicio Join data vs seguridad dfSeguridad")
    dfSeguridad.show(1)
    var newdf = spark.emptyDataFrame
    try {
      if (dfSeguridad.count().toInt > 0) {
        // spark.sqlContext.sql(s"SELECT clientes.identificacion_cliente, clientes.tii_tipo_identificacion FROM maestro.securitytable RIGHT JOIN ods_stag.clientes on securitytable.hashvalue = clientes.identificacion_cliente WHERE securitytable.hashvalue is null;")
        newdf = dfSeguridad.join(dfData, dfSeguridad.col("hashvalue") === dfData.col("hashvalue1"), "right")
          .select(dfData.col("hashvalue1"), dfData.col("aesvalue"))
          .where(dfSeguridad.col("hashvalue").isNull)
      } else {
        newdf = dfData
      }
    }
    catch {
      case e: Throwable =>
        printError(s"Error join table." + e, true)
    }
    newdf.where("hashvalue1 is not null")
  }

  /**
   * Funcion encargada de obtener las columnas a homologar
   * @param hashColumn
   * @return
   */
  def getColumnsToWork(hashColumn: DataFrame): Array[String] ={
    var columnas: String = ""
    printInfo(s"DF a trabajar columnas ${hashColumn.show}")
    val arc = hashColumn.collect()
    printInfo(s"arc ${arc.length}")
    arc.foreach { row =>
      printInfo(s"Columns identifided to work INIT ${row.getString(0)}, ${row.getString(1)}, ${row.getString(2)}")
      columnas = columnas + row.getString(2) + ","
      printInfo(s"Columns identifided to work FINALL $columnas")
    }
    printInfo(s"Columns identifided to work $columnas")
    columnas.split(",")
  }

  /**
   * Funcion encargada de obtener las tuplas de columnas a encriptar
   * @param hashColumn
   * @return
   */
  def getTuplasColumnsToWork(hashColumn: DataFrame): Array[Array[String]] ={
    var arrColumnas: Array[Array[String]] = new Array[Array[String]](hashColumn.count().toInt)
    var cont = 0
    val arc = hashColumn.collect()
    arc.foreach { row =>
      val columnas: Array[String] = new Array[String](2)
      printInfo(s"VALOR row.getString(1) ${row.getString(1)}")
      printInfo(s"VALOR row.getString(2) ${row.getString(2)}")
      columnas(0) = row.getString(1)
      columnas(1) = row.getString(2)
      printInfo(s"VALOR COLUMNAS(0) ${columnas(0)}")
      printInfo(s"VALOR COLUMNAS(1) ${columnas(1)}")
      arrColumnas(cont) = columnas
      cont = cont+1
    }
    printInfo(s"Columns identifided to encrtipted $arrColumnas")
    arrColumnas
  }

}

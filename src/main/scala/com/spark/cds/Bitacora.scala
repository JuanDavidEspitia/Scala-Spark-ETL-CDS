package com.spark.cds

import java.sql.{Date, Timestamp}
import java.util.UUID

import Utils._
import DataframeUtils._

class Bitacora(fuente: String, destino: String, description:String=null, forced: Boolean, var rowsOrigin: Int = 0) {

  val nameProcess = nameApp
  val table = "usrcif.bitacora"
  val timeProcess = getCurrentdateTimeStamp
  val dateProcess = getCurrentDate
  var rowsDestiny = 0
  var resultInfo = ""
  var statusBitacora = "OK"
  val id = UUID.randomUUID().toString

  //Clase utilizada para crear el dataframe maestro
  case class BitacoraData(fuente: String, destino: String, nombreProceso: String, timestampEvento: Timestamp,
                          estado: String, descripcion: String, cantidadRegistrosOrigen: Int,
                          cantidadRegistrosCargados: Int, cargaForzada: Boolean, idProceso:String, fechaEvento: Date)

  var bitacoraList = Seq(
    BitacoraData(fuente, destino, nameProcess, timeProcess, "Init", description, rowsOrigin, 0, forced, id, dateProcess)
  )

  //Actualiza numero de filas origen
  def updateRowsOrigin(rows: Int): Unit = {
    rowsOrigin = rows
  }

  //Actualiza numero de filas destino
  def updateRowsDestiny(rows: Int): Unit = {
    rowsDestiny = rows
  }

  def printBitacora() {
    println(bitacoraList)
  }

  //Añade fila a la bitacora
  def appendBitacora(status: String=statusBitacora, description: String = resultInfo): Unit = {
    val timeProcess = getCurrentdateTimeStamp
    val dateProcess = getCurrentDate

    bitacoraList = bitacoraList :+ BitacoraData(fuente, destino, nameProcess, timeProcess, status, description,
      rowsOrigin, rowsDestiny, forced, id, dateProcess)
  }

  // Guardar información de la bitacora
  def saveBitacora: Unit = {
    try {
      if (spark.catalog.tableExists(table)){
        val bitacoraData = spark.sqlContext.createDataFrame(bitacoraList)
        bitacoraData.insertDataFrame(table)
      }else{
        printError("Validate existence of the table bitacora '" + table + "', is not found",true, false)
      }
    } catch {
      case e: Exception =>
        printError("Validate save information bitacora in '" + table + "', error found: "+e,false, false)
    }
  }

}

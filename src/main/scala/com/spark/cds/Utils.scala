package com.spark.cds

import java.time.LocalDateTime

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.sql.{Date, Timestamp}
import java.time.LocalDate


object Utils {

  val nameApp = "DataTransformation"
  //Create session from spark, assign application name, processers and enables hive
  val spark = SparkSession.builder.appName(nameApp).enableHiveSupport().getOrCreate()


  //Init Bitacora
  var bitacora = new Bitacora("", "", "",false)

  def initBitacora(origin: String, destiny: String, description:String, forced: Boolean = false): Unit = {

    printInfo("Init bitacora to " + bitacora.nameProcess)
    bitacora = new Bitacora(origin, destiny, description, forced)
  }

  def printInfo(message: String, saveBitacora:Boolean=false) = {

    Logger.getLogger("spark").info(message)
    if(saveBitacora){
      bitacora.resultInfo += message + ", "
    }
  }

  def printWarn(message: String, saveBitacora:Boolean=false) = {

    Logger.getLogger("spark").warn(message)
    if(saveBitacora){
      bitacora.statusBitacora = "WARN"
      bitacora.resultInfo += message + ", "
    }
  }

  def printError(message: String, saveBitacora:Boolean=false, exit:Boolean=false ) = {

    Logger.getLogger("spark").error(message)

    if(exit){
      bitacora.updateRowsDestiny(0)
      bitacora.appendBitacora("KO", message)
      bitacora.saveBitacora
      System.exit(1)
    }
    else if(saveBitacora){
      bitacora.statusBitacora = "WARN"
      bitacora.resultInfo += message + ", "
    }

  }
  def differenceSchemaList(listO:List[String], listDos:List[String]) = {
    listO.filterNot(listDos.contains(_))
  }
  def commonSchemaList(listO:List[String], listDos:List[String]) = {
    listO.filter(listDos.contains(_))
  }
  def listColumnsPrint (listColumns:List[String]) = {
    var result = ""
    listColumns.map(x => result += "\n- "+ x)
    result
  }

  def getCurrentdateTimeStamp: Timestamp ={
    val today = LocalDateTime.now()
    val re = Timestamp.valueOf(today)
    re
  }

  def getCurrentDate: Date ={
    val today = LocalDate.now()
    val re = Date.valueOf(today)
    re
  }
}
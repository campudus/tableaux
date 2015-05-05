package com.campudus.tableaux.controller

/**
 * Controller for resetting the database.
 * Mostly this is need for Demo & Test data.
 */
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database.Tableaux._
import com.campudus.tableaux.database._
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.core.json.Json
import org.vertx.scala.platform.Verticle

import scala.concurrent.Future

class DemoController(val verticle: Verticle, val database: DatabaseConnection) extends DatabaseAccess with StandardVerticle {
  val demoJson = Json.obj(
    "tableName" -> "Regierungsbezirke Bayern",
    "columns" -> Json.arr(
      Json.obj("kind" -> "text", "name" -> "Regierungsbezirk", "ordering" -> 1),
      Json.obj("kind" -> "text", "name" -> "Hauptstadt", "ordering" -> 2),
      Json.obj("kind" -> "numeric", "name" -> "Einwohner", "ordering" -> 3),
      Json.obj("kind" -> "numeric", "name" -> "Fläche in km²", "ordering" -> 4)),
    "rows" -> Json.arr(
      Json.obj("values" -> Json.arr("Oberbayern", "München", 4469342, 17530.21)),
      Json.obj("values" -> Json.arr("Niederbayern", "Landshut", 1189153, 10328.65)),
      Json.obj("values" -> Json.arr("Oberpfalz", "Regensburg", 1077991, 9690.19)),
      Json.obj("values" -> Json.arr("Oberfranken", "Bayreuth", 1056365, 7231.49))))

  val demoJson2 = Json.obj(
    "tableName" -> "Bundesländer Deutschlands",
    "columns" -> Json.arr(
      Json.obj("kind" -> "text", "name" -> "Land", "ordering" -> 1),
      Json.obj("kind" -> "text", "name" -> "Hauptstadt", "ordering" -> 2),
      Json.obj("kind" -> "numeric", "name" -> "Fläche in km²", "ordering" -> 3),
      Json.obj("kind" -> "numeric", "name" -> "Einwohner", "ordering" -> 4)),
    "rows" -> Json.arr(
      Json.obj("values" -> Json.arr("Baden-Württemberg", "Stuttgart", 35751, 10631278)),
      Json.obj("values" -> Json.arr("Bayern", "München", 70550, 12604244)),
      Json.obj("values" -> Json.arr("Berlin", "Berlin", 892, 3440991)),
      Json.obj("values" -> Json.arr("Brandenburg", "Potsdam", 29655, 2449193))))

  lazy val tableaux = new Tableaux(verticle, database)

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset database")
    tableaux.resetDB()
  }

  def createDemoTables(): Future[DomainObject] = {
    createTable(demoJson.getString("tableName"), jsonToSeqOfColumnNameAndType(demoJson), jsonToSeqOfRowsWithValue(demoJson))
    createTable(demoJson2.getString("tableName"), jsonToSeqOfColumnNameAndType(demoJson2), jsonToSeqOfRowsWithValue(demoJson2))
  }

  private def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableaux.createCompleteTable(tableName, columns, rowsValues)
  }
}

package com.campudus.tableaux.controller

import com.campudus.tableaux.{TableauxTestBase, TestConfig}
import com.campudus.tableaux.database.domain.CreateSimpleColumn
import com.campudus.tableaux.database.model.{StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.database.{DatabaseConnection, SingleLanguage, TextType}
import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class SystemControllerTest extends TableauxTestBase with TestConfig {

  def createSystemController(): SystemController = {
    val dbConnection = DatabaseConnection(tableauxConfig)

    val systemModel = SystemModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection)
    val structureModel = StructureModel(dbConnection)

    SystemController(tableauxConfig, systemModel, tableauxModel, structureModel)
  }

  @Test
  def resetSystemTables(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok")

    for {
      result <- sendRequest("POST", "/reset")
    } yield {
      assertEquals(expectedJson, result)
    }
  }

  @Test
  def resetDemoData(): Unit = okTest {
    val expectedJson1 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        Json.obj("id" -> 1),
        Json.obj("id" -> 2)
      )
    )

    val expectedJson2 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Bundesländer Deutschlands"),
        Json.obj("id" -> 2, "name" -> "Regierungsbezirke")
      )
    )

    for {
      _ <- sendRequest("POST", "/reset")
      result <- sendRequest("POST", "/resetDemo")
      tablesResult <- sendRequest("GET", "/tables")
    } yield {
      assertEquals(expectedJson1, result)
      assertEquals(expectedJson2, tablesResult)
    }
  }
}

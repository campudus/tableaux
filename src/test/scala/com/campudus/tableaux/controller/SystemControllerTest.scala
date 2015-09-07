package com.campudus.tableaux.controller

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.{StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.router.SystemRouter
import com.campudus.tableaux.{TableauxTestBase, TestConfig}
import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._

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

    val nonce = SystemRouter.generateNonce()

    for {
      result <- sendRequest("POST", s"/reset?nonce=$nonce")
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
      _ <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/reset?nonce=$nonce")
      }
      result <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/resetDemo?nonce=$nonce")
      }
      tablesResult <- sendRequest("GET", "/tables")
    } yield {
      assertEquals(expectedJson1, result)
      assertEquals(expectedJson2, tablesResult)
    }
  }
}

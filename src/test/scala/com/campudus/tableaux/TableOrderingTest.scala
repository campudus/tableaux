package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class TableOrderingTest extends TableauxTestBase {

  private def createEmptyTable(name: String) = {
    sendRequest("POST", "/tables", Json.obj("name" -> name)).map(_.getLong("id"))
  }

  private def regularTableJson(id: Long, name: String) = Json.obj("id" -> id, "name" -> name, "hidden" -> false)

  @Test
  def createTablesShouldStayInOrderOfCreation(implicit c: TestContext): Unit = okTest {
    for {
      tableId1 <- createEmptyTable("First")
      tableId2 <- createEmptyTable("Second")
      tableId3 <- createEmptyTable("Third")
      tableId4 <- createEmptyTable("Fourth")
      tableId5 <- createEmptyTable("Fifth")
      tables <- sendRequest("GET", "/tables")
    } yield {

      val expected = Json.obj("status" -> "ok", "tables" -> Json.arr(
        regularTableJson(1, "First"),
        regularTableJson(2, "Second"),
        regularTableJson(3, "Third"),
        regularTableJson(4, "Fourth"),
        regularTableJson(5, "Fifth")
      ))

      assertEquals(expected, tables)
    }
  }

  @Test
  def orderTableCanBeSetToStart(implicit c: TestContext): Unit = okTest {
    for {
      tableId1 <- createEmptyTable("First")
      tableId2 <- createEmptyTable("Second")
      tableId3 <- createEmptyTable("Third")
      tableId4 <- createEmptyTable("Fourth")
      tableId5 <- createEmptyTable("Fifth")
      orderResult <- sendRequest("POST", s"/tables/$tableId5/order", Json.obj("location" -> "start"))
      tables <- sendRequest("GET", "/tables")
    } yield {

      val expected = Json.obj("status" -> "ok", "tables" -> Json.arr(
        regularTableJson(5, "Fifth"),
        regularTableJson(1, "First"),
        regularTableJson(2, "Second"),
        regularTableJson(3, "Third"),
        regularTableJson(4, "Fourth")
      ))

      assertEquals(expected, tables)
    }
  }
}

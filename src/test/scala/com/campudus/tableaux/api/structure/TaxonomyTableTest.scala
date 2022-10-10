package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class TaxonomyTableTest extends TableauxTestBase {

  def initTable(): Future[TableId] = {
    for {
      result <- sendRequest(
        "POST",
        "/tables",
        Json.obj("name" -> "categories", "type" -> "taxonomy", "displayName" -> Json.obj("de" -> "Tax"))
      )
    } yield result.getLong("id")
  }

  def fetchTable(tableId: TableId) = sendRequest("GET", s"/completetable/$tableId")

  @Test
  def createTaxonomyTable(implicit c: TestContext) = okTest {
    def expectedTable = Json.obj(
      "name" -> "categories",
      "type" -> "taxonomy",
      "displayName" -> Json.obj("de" -> "Tax"),
      "columns" -> Json.arr(
          Json.obj("name" -> "title", "kind" -> "shorttext"),
          Json.obj("name" -> "ordering", "kind" -> "numeric"),
          Json.obj("name" -> "code", "kind" -> "shorttext"),
          Json.obj("name" -> "parent", "kind" -> "link")
        ),
        "rows" -> Json.emptyArr()
    )
    for {
      tableId <- initTable()
      table <- fetchTable(tableId)
    } yield {
      assertJSONEquals(expectedTable, table)
    }
  }

  @Test
  def changeTitleColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1", Json.obj ("name" -> "anything"))
    } yield ()
  }
  @Test
  def changeOrderingColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2", Json.obj ("name" -> "anything"))
    } yield ()
  }
  @Test
  def changeCodeColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("POST", s"/tables/$tableId/columns/3", Json.obj ("name" -> "anything"))
    } yield ()
  }
  @Test
  def changeParentColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("POST", s"/tables/$tableId/columns/4", Json.obj ("name" -> "anything"))
    } yield ()
  }

  @Test
  def deleteTitleColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1")
    } yield ()
  }
  @Test
  def deleteOrderingColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/2")
    } yield ()
  }
  @Test
  def deleteCodeColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/3")
    } yield ()
  }
  @Test
  def deleteParentColumn (implicit c: TestContext) = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- initTable()
      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/4")
    } yield ()
  }

  @Test
  def addOptionalColumn(implicit c: TestContext) = okTest {
    def expectedTable = Json.obj(
      "name" -> "categories",
      "type" -> "taxonomy",
      "displayName" -> Json.obj("de" -> "Tax"),
      "columns" -> Json.arr(
          Json.obj("name" -> "title", "kind" -> "shorttext"),
          Json.obj("name" -> "ordering", "kind" -> "numeric"),
          Json.obj("name" -> "code", "kind" -> "shorttext"),
          Json.obj("name" -> "parent", "kind" -> "link"),
          Json.obj ("name" -> "fifth-column", "kind" -> "text")
        ),
        "rows" -> Json.emptyArr()
    )
    for {
      tableId <- initTable()
      _ <- sendRequest("POST",  s"/tables/$tableId/columns", Json.obj("name" -> "fifth-column", "kind" -> "text"))
      table <- fetchTable(tableId)
    } yield {
      assertJSONEquals(expectedTable, table)
    }
  }

  @Test
  def changeOptionalColumn(implicit c: TestContext) = okTest {
    def expectedTable = Json.obj(
      "name" -> "categories",
      "type" -> "taxonomy",
      "displayName" -> Json.obj("de" -> "Tax"),
      "columns" -> Json.arr(
          Json.obj("name" -> "title", "kind" -> "shorttext"),
          Json.obj("name" -> "ordering", "kind" -> "numeric"),
          Json.obj("name" -> "code", "kind" -> "shorttext"),
          Json.obj("name" -> "parent", "kind" -> "link"),
          Json.obj ("name" -> "fifth-columbia", "kind" -> "text")
        ),
        "rows" -> Json.emptyArr()
    )
    for {
      tableId <- initTable()
      _ <- sendRequest("POST",  s"/tables/$tableId/columns", Json.obj("name" -> "fifth-column", "kind" -> "text"))
      _ <- sendRequest("POST",  s"/tables/$tableId/columns/5", Json.obj("name" -> "fifth-columbia"))
      table <- fetchTable(tableId)
    } yield {
      assertJSONEquals(expectedTable, table)
    }
  }

  @Test
  def deleteOptionalColumn(implicit c: TestContext) = okTest {
    def expectedTable = Json.obj(
      "name" -> "categories",
      "type" -> "taxonomy",
      "displayName" -> Json.obj("de" -> "Tax"),
      "columns" -> Json.arr(
          Json.obj("name" -> "title", "kind" -> "shorttext"),
          Json.obj("name" -> "ordering", "kind" -> "numeric"),
          Json.obj("name" -> "code", "kind" -> "shorttext"),
          Json.obj("name" -> "parent", "kind" -> "link"),
        ),
        "rows" -> Json.emptyArr()
    )
    for {
      tableId <- initTable()
      _ <- sendRequest("POST",  s"/tables/$tableId/columns", Json.obj("name" -> "fifth-column", "kind" -> "text"))
      _ <- sendRequest("DELETE",  s"/tables/$tableId/columns/5")
      table <- fetchTable(tableId)
    } yield {
      assertJSONEquals(expectedTable, table)
    }
  }
}

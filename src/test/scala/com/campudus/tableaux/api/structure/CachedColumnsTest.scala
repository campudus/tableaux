package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import org.vertx.scala.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._

@RunWith(classOf[VertxUnitRunner])
class CachedColumnsTest extends TableauxTestBase {

  @Test
  def changeColumnNameAndOrdering(implicit c: TestContext): Unit = {
    okTest {
      val postJson = Json.obj("name" -> "New testname", "ordering" -> 4)

      val expectedStringBefore = "Test Column 1"
      val expectedStringAfter = "New testname"

      for {
        _ <- createDefaultTable()

        resultGetBefore <- sendRequest("GET", "/tables/1/columns/1")
        resultPost <- sendRequest("POST", "/tables/1/columns/1", postJson)
        resultGetAfter <- sendRequest("GET", "/tables/1/columns/1")
      } yield {
        assertEquals(expectedStringBefore, resultGetBefore.getString("name"))
        assertEquals(expectedStringAfter, resultGetAfter.getString("name"))

        assertEquals(4, resultGetAfter.getInteger("ordering"))

        assertEquals(resultPost, resultGetAfter)
      }
    }
  }

  @Test
  def addColumn(implicit c: TestContext): Unit = {
    okTest {
      val createStringColumnJson = Json
        .obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 3", "identifier" -> true)))

      for {
        tableId <- createDefaultTable()

        columnsBefore <- sendRequest("GET", s"/tables/$tableId/columns")

        column1 <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson)
        columnId1 = column1.getJsonArray("columns").getJsonObject(0).getLong("id").toLong

        columnsAfter <- sendRequest("GET", s"/tables/$tableId/columns")
      } yield {
        assertNotSame(columnsBefore, columnsAfter)

        assertFalse(
          columnsBefore
            .getArray("columns")
            .asScala
            .map(_.asInstanceOf[JsonObject])
            .exists(_.getLong("id") == columnId1))
        assertTrue(
          columnsAfter
            .getArray("columns")
            .asScala
            .map(_.asInstanceOf[JsonObject])
            .exists(_.getLong("id") == columnId1))
      }
    }
  }

  @Test
  def deleteColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createDefaultTable()

        columnsBefore <- sendRequest("GET", s"/tables/$tableId/columns")

        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1")

        columnsAfter <- sendRequest("GET", s"/tables/$tableId/columns")
      } yield {
        assertNotSame(columnsBefore, columnsAfter)

        assertTrue(
          columnsBefore
            .getArray("columns")
            .asScala
            .map(_.asInstanceOf[JsonObject])
            .exists(_.getLong("id") == 1))
        assertFalse(
          columnsAfter
            .getArray("columns")
            .asScala
            .map(_.asInstanceOf[JsonObject])
            .exists(_.getLong("id") == 1))
      }
    }
  }
}

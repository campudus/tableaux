package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class ChangeStructureTest extends TableauxTestBase {

  @Test
  def changeTableName(implicit c: TestContext): Unit = okTest {
    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "New testname",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    for {
      _ <- createDefaultTable(name = "Testname")
      test1 <- sendRequest("POST", "/tables/1", Json.obj("name" -> "Testname"))
      test1 <- sendRequest("POST", "/tables/1", Json.obj("name" -> "New testname"))
      test2 <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedTableJson, test1)
      assertEquals(expectedTableJson, test2)
    }
  }

  @Test
  def changeTableNameToExistingTableName(implicit c: TestContext): Unit = exceptionTest("error.request.unique.table") {
    for {
      _ <- createDefaultTable(name = "Testname 1")
      _ <- createDefaultTable(name = "Testname 2")
      test1 <- sendRequest("POST", "/tables/2", Json.obj("name" -> "Testname 1"))
    } yield ()
  }

  @Test
  def changeColumnName(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname")
    val expectedString = "New testname"

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/1", postJson)
      resultGet <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedString, resultGet.getString("name"))
      assertEquals(resultPost, resultGet)
    }
  }

  @Test
  def changeColumnOrdering(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("ordering" -> 5)
    val expectedOrdering = 5

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/1", postJson)
      resultGet <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedOrdering, resultGet.getInteger("ordering"))
      assertEquals(resultPost, resultGet)
    }
  }

  @Test
  def changeColumnKind(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("kind" -> "text")
    val expectedKind = "text"

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/2", postJson)
      resultGet <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedKind, resultGet.getString("kind"))
      assertEquals(resultPost, resultGet)
    }
  }

  @Test
  def changeColumnKindWhichShouldFail(implicit c: TestContext): Unit = {
    okTest{
      val kindTextJson = Json.obj("kind" -> "text")
      val kindNumericJson = Json.obj("kind" -> "numeric")

      val failed = Json.obj("failed" -> "failed")

      for {
        _ <- createDefaultTable()

        _ <- sendRequest("POST",
          "/tables/1/rows",
          Json.obj(
            "rows" ->
              Json.obj("values" ->
                Json.arr("Test", 5))))

        // change numeric column to text column
        changeToText <- sendRequest("POST", "/tables/1/columns/2", kindTextJson)

        // change text column to numeric column, which should fail
        failedChangeToNumeric <- sendRequest("POST", "/tables/1/columns/1", kindNumericJson)
          .recoverWith({ case _ => Future.successful(failed) })

        columns <- sendRequest("GET", "/tables/1/columns")
      } yield {
        assertEquals(columns.getJsonArray("columns").getJsonObject(1).mergeIn(Json.obj("status" -> "ok")), changeToText)

        assertEquals(failed, failedChangeToNumeric)

        assertEquals("text", columns.getJsonArray("columns").getJsonObject(0).getString("kind"))
        assertEquals("text", columns.getJsonArray("columns").getJsonObject(1).getString("kind"))
      }
    }
  }

  @Test
  def changeColumn(implicit c: TestContext): Unit = {
    okTest{
      val postJson = Json.obj("name" -> "New testname", "ordering" -> 5, "kind" -> "text")
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "id" -> 2,
        "name" -> "New testname",
        "kind" -> "text",
        "ordering" -> 5,
        "multilanguage" -> false,
        "identifier" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj()
      )

      for {
        tableId <- createDefaultTable()
        resultPost <- sendRequest("POST", s"/tables/$tableId/columns/2", postJson)
        resultGet <- sendRequest("GET", s"/tables/$tableId/columns/2")
      } yield {
        assertEquals(expectedJson2, resultGet)
        assertEquals(resultPost, resultGet)
      }
    }
  }
}

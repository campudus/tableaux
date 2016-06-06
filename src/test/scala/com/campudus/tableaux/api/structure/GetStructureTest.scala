package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class GetStructureTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Table 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
  val createBooleanColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))

  @Test
  def retrieveTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveAllTables(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedJson = Json.obj("status" -> "ok", "tables" -> Json.arr(
      baseExpected.copy().mergeIn(Json.obj("id" -> 1, "name" -> "Test Table 1")),
      baseExpected.copy().mergeIn(Json.obj("id" -> 2, "name" -> "Test Table 2"))
    ))

    for {
      _ <- createDefaultTable("Test Table 1")
      _ <- createDefaultTable("Test Table 2")
      test <- sendRequest("GET", "/tables")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveColumns(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> true, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())
    ))

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveStringColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> true, "displayName" -> Json.obj(), "description" -> Json.obj())

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveNumberColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
}
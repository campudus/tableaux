package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class CreateColumnTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createShortTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.ShortTextCol(name)).getJson

  def createRichTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.RichTextCol(name)).getJson

  def createNumberColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.NumericCol(name)).getJson

  def createCurrencyColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.CurrencyCol(name)).getJson

  def createBooleanColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.BooleanCol(name)).getJson

  @Test
  def createTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createTextColumnJson("column1")
    val createColumn2 = createTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createShortTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createShortTextColumnJson("column1")
    val createColumn2 = createShortTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createRichTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createRichTextColumnJson("column1")
    val createColumn2 = createRichTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createNumberColumnJson("column1")
    val createColumn2 = createNumberColumnJson("column2")
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createCurrencyColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createCurrencyColumnJson("column1")
    val createColumn2 = createCurrencyColumnJson("column2")
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createBooleanColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createBooleanColumnJson("column1")
    val createColumn2 = createBooleanColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createMultipleColumns(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
      Json.obj("kind" -> "text", "name" -> "Test Column 2")))

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "ordering" -> 1, "kind" -> "numeric", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 2, "ordering" -> 2, "kind" -> "text", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createMultipleColumnsWithOrdering(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2),
      Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1)))
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 2, "ordering" -> 1, "kind" -> "text", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 1, "ordering" -> 2, "kind" -> "numeric", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class CreateTableTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createTableJsonWithHiddenFlag = Json.obj("name" -> "Test Nr. 1", "hidden" -> true)
  val createTableJsonWithLangtags = Json.obj("name" -> "Test Nr. 1", "langtags" -> Json.arr("de-DE", "en-GB", "en-US"))

  @Test
  def createTableWithName(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJson)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJson)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJson)
      test2 <- sendRequest("POST", "/tables", createTableJson)
    } yield {
      assertEquals(expectedJson1, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTableWithHiddenFlag(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJsonWithHiddenFlag)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJsonWithHiddenFlag)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJsonWithHiddenFlag)
      test2 <- sendRequest("POST", "/tables", createTableJsonWithHiddenFlag)
    } yield {
      assertEquals(expectedJson1, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTableWithLangtags(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJsonWithLangtags)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJsonWithLangtags)

    for {
      test1Post <- sendRequest("POST", "/tables", createTableJsonWithLangtags)
      test2Post <- sendRequest("POST", "/tables", createTableJsonWithLangtags)

      test1Get <- sendRequest("GET", "/tables/1")
      test2Get <- sendRequest("GET", "/tables/2")
    } yield {
      assertEquals(expectedJson1, test1Post)
      assertEquals(expectedJson2, test2Post)

      assertEquals(test1Get, test1Post)
      assertEquals(test2Get, test2Post)
    }
  }

}
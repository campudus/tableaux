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

  def createShortTextColumnJson(name: String) = {
    RequestCreation.Columns().add(RequestCreation.ShortTextCol(name)).getJson
  }

  def createRichTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.RichTextCol(name)).getJson

  def createNumberColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.NumericCol(name)).getJson

  def createIntegerColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.IntegerCol(name)).getJson

  def createCurrencyColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.CurrencyCol(name)).getJson

  def createBooleanColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.BooleanCol(name)).getJson

  @Test
  def createTextColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createTextColumnJson("column1")
      val createColumn2 = createTextColumnJson("column2")

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createShortTextColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createShortTextColumnJson("column2")

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createRichTextColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createRichTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createNumberColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createNumberColumnJson("column1")
      val createColumn2 = createNumberColumnJson("column2")
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createIntegerColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createIntegerColumnJson("column1")
      val createColumn2 = createIntegerColumnJson("column2")
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createCurrencyColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createCurrencyColumnJson("column1")
      val createColumn2 = createCurrencyColumnJson("column2")
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createBooleanColumn(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createBooleanColumnJson("column1")
      val createColumn2 = createBooleanColumnJson("column2")

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 1,
                 "ordering" -> 1,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0)))
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 2,
                 "ordering" -> 2,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0)))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createMultipleColumns(implicit c: TestContext): Unit = {
    okTest {
      val jsonObj = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
                              Json.obj("kind" -> "text", "name" -> "Test Column 2")))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "ordering" -> 1,
            "kind" -> "numeric",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
            "description" -> Json.obj()
          )
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("POST", "/tables/1/columns", jsonObj)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createMultipleColumnsWithOrdering(implicit c: TestContext): Unit = {
    okTest {
      val jsonObj = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2),
                              Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1)))
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 2,
            "ordering" -> 1,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 1,
            "ordering" -> 2,
            "kind" -> "numeric",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
            "description" -> Json.obj()
          )
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("POST", "/tables/1/columns", jsonObj)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }
  def createMultipleColumnsWithAttributes(implicit c: TestContext): Unit = {
    okTest {
      val attributeObject = Json.obj("arrayAttribute" -> Json.arr("test",2, Json.obj("arrayObj" -> "test")), "numberAttribute" -> 42, "stringAttribute" -> "DONT PANIC", "objAttribute" -> Json.obj("objObjAttribute" -> "test"))

      val jsonObj = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2, "attributes" -> attributeObject),
          Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1, "attributes" -> attributeObject)))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 2,
            "ordering" -> 1,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "attributes" -> attributeObject,
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 1,
            "ordering" -> 2,
            "kind" -> "numeric",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "attributes" -> attributeObject,
            "description" -> Json.obj()
          )
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("POST", "/tables/1/columns", jsonObj)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }
}

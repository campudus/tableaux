package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

@RunWith(classOf[VertxUnitRunner])
class CreateColumnTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createShortTextColumnJson(name: String) = {
    RequestCreation.Columns().add(RequestCreation.ShortTextCol(name)).getJson
  }

  def createRichTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.RichTextCol(name)).getJson

  def createNumberColumnJson(name: String, decimalDigits: Option[Int] = None) =
    RequestCreation.Columns().add(RequestCreation.NumericCol(name, decimalDigits)).getJson

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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
  def createNumberColumnWithCustomDecimalDigits(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createNumberColumnJson("column1", None)
      val createColumn2 = createNumberColumnJson("column2", Some(10))
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertNull(test1.getJsonArray("columns").getJsonObject(0).getInteger("decimalDigits"))
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createNumberColumnWithCustomDecimalDigitsTooHigh(implicit c: TestContext): Unit = {
    exceptionTest("error.json.decimalDigits") {
      val createColumn = createNumberColumnJson("column2", Some(11))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("POST", "/tables/1/columns", createColumn)
      } yield ()
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )
      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
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
        "columns" -> Json.arr(
          Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
          Json.obj("kind" -> "text", "name" -> "Test Column 2")
        )
      )

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
        "columns" -> Json.arr(
          Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2),
          Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1)
        )
      )
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

@RunWith(classOf[VertxUnitRunner])
class ColumnHiddenTest extends CreateColumnTest {

  @Test
  def createMultipleHiddenColumns(implicit c: TestContext): Unit = {
    okTest {
      val jsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "hidden" -> true),
          Json.obj("kind" -> "text", "name" -> "Test Column 2", "hidden" -> true)
        )
      )

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
            "description" -> Json.obj(),
            "hidden" -> true
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "hidden" -> true
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
  def createMultipleColumnsWithoutHiddenFlag(implicit c: TestContext): Unit = {
    okTest {
      val jsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
          Json.obj("kind" -> "text", "name" -> "Test Column 2")
        )
      )

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
            "description" -> Json.obj(),
            "hidden" -> false
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "hidden" -> false
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
  def createMultipleVisibleColumns(implicit c: TestContext): Unit = {
    okTest {
      val jsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "hidden" -> false),
          Json.obj("kind" -> "text", "name" -> "Test Column 2", "hidden" -> false)
        )
      )

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
            "description" -> Json.obj(),
            "hidden" -> false
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "text",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "hidden" -> false
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
  def changeColumnHiddenFlag(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj(
      "columns" -> Json.arr(
        Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "hidden" -> true)
      )
    )

    val changeObj = Json.obj(
      "hidden" -> false
    )

    val expectedHidden = false

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      resultPost <- sendRequest("POST", "/tables/1/columns", jsonObj)
      resultChange <- sendRequest("POST", "/tables/1/columns/1", changeObj)
      resultGet <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedHidden, resultGet.getBoolean("hidden"))
    }
  }
}

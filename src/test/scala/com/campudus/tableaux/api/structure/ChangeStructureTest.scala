package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.{JsonArray, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

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
      assertJSONEquals(expectedTableJson, test1)
      assertJSONEquals(expectedTableJson, test2)
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
  def changeColumnDecimalDigits(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("decimalDigits" -> 10)

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/2", postJson)
      resultGet <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(10, resultGet.getInteger("decimalDigits"))
      assertEquals(resultPost, resultGet)
    }
  }

  @Test
  def changeColumnDecimalDigitsTooHigh(implicit c: TestContext): Unit = exceptionTest("error.json.decimalDigits") {
    val postJson = Json.obj("decimalDigits" -> 11)

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/2", postJson)
    } yield ()
  }

  @Test
  def changeColumnFormatPatternForbiddenForNonGroupColumns(implicit c: TestContext): Unit =
    exceptionTest("error.request.forbidden.column") {
      val postJson = Json.obj("formatPattern" -> "{{1}} {{2}}")

      for {
        _ <- createDefaultTable()
        resultPost <- sendRequest("POST", "/tables/1/columns/2", postJson)
      } yield ()
    }

  @Test
  def changeColumnKindWhichShouldFail(implicit c: TestContext): Unit = {
    okTest {
      val kindTextJson = Json.obj("kind" -> "text")
      val kindNumericJson = Json.obj("kind" -> "numeric")

      val failed = Json.obj("failed" -> "failed")

      for {
        _ <- createDefaultTable()

        _ <- sendRequest(
          "POST",
          "/tables/1/rows",
          Json.obj(
            "rows" ->
              Json.obj("values" ->
                Json.arr("Test", 5))
          )
        )

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
    okTest {
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
        assertJSONEquals(expectedJson2, resultGet)
        assertJSONEquals(resultPost, resultGet)
      }
    }
  }

  @Test
  def changeSeparatorFlagValue(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("separator" -> true)
    val expectedSeparatorValue = true

    for {
      _ <- createDefaultTable()
      resultPost <- sendRequest("POST", "/tables/1/columns/2", postJson)
      resultGet <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedSeparatorValue, resultGet.getBoolean("separator"))
      assertEquals(resultPost, resultGet)
    }
  }

  private def percentageAttribute(
      kind: String = "integer",
      multilanguage: Boolean = false,
      name: String = "percentage"
  ): JsonObject = {
    Json.obj(
      "name" -> name,
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "kind" -> kind,
      "multilanguage" -> multilanguage
    )
  }

  private def createLinkColumn(
      linkAttributes: JsonArray = Json.arr(),
      formatPattern: Option[String] = None
  ): Future[ColumnId] = {
    val baseJson =
      Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2, "linkAttributes" -> linkAttributes)
    val columnJson = formatPattern match {
      case Some(pattern) => baseJson.mergeIn(Json.obj("formatPattern" -> pattern))
      case None => baseJson
    }
    val postJson = Json.obj("columns" -> Json.arr(columnJson))

    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable("Test Table 2", 2)
      result <- sendRequest("POST", "/tables/1/columns", postJson)
    } yield result.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
  }

  @Test
  def createLinkColumnWithLinkAttributes(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def createLinkColumnWithTooManyLinkAttributesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(), percentageAttribute()))
    }

  @Test
  def createLinkColumnWithDisallowedLinkAttributeKindFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(kind = "link")))
    }

  @Test
  def changeLinkColumnAddLinkAttributes(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn()
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute()))
      )
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def changeLinkColumnToTooManyLinkAttributesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(), percentageAttribute()))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnLinkAttributesForbiddenForNonLinkColumns(implicit c: TestContext): Unit =
    exceptionTest("error.request.forbidden.column") {
      for {
        _ <- createDefaultTable()
        _ <- sendRequest(
          "POST",
          "/tables/1/columns/1",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute()))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnWithExplicitEmptyLinkAttributesClearsValues(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> Json.arr()))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertFalse(column.containsKey("linkAttributes"))
      assertFalse(cell.getJsonArray("value").getJsonObject(0).containsKey("attributes"))
    }
  }

  @Test
  def changeLinkColumnOmittingLinkAttributesLeavesItUntouched(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("name" -> "renamed"))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def changeLinkColumnRenamingLinkAttributePreservesValues(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(name = "percentage")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      // rename only - same kind, same multilanguage flag, just a different name/displayName label
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "percent")))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("percent", column.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  @Test
  def changeLinkColumnLinkAttributesKindMigrationSucceeds(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "numeric")))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("numeric", column.getJsonArray("linkAttributes").getJsonObject(0).getString("kind"))
      val migratedValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getValue(0)
      assertEquals(50.0, migratedValue.asInstanceOf[Number].doubleValue(), 0.001)
    }
  }

  @Test
  def changeLinkColumnLinkAttributesKindMigrationFailsAndRollsBack(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("abc")))))
    val failed = Json.obj("failed" -> "failed")

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      changeResult <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "integer")))
      ).recoverWith({ case _ => Future.successful(failed) })
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(failed, changeResult)
      assertEquals("text", column.getJsonArray("linkAttributes").getJsonObject(0).getString("kind"))
      assertEquals("abc", cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0))
    }
  }

  @Test
  def changeLinkColumnMultilanguageFalseToTrueDuplicatesValue(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = false)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      val attributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      assertEquals(50, attributeValue.getInteger("de-DE"))
      assertEquals(50, attributeValue.getInteger("en-GB"))
    }
  }

  @Test
  def changeLinkColumnMultilanguageTrueToFalseCollapsesValue(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50, "en-GB" -> 75))))
      )
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  @Test
  def changeLinkColumnFormatPatternAccepted(implicit c: TestContext): Unit = okTest {
    val pattern = "{{value}} ({{attributes.percentage}}%)"

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("formatPattern" -> pattern))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals(pattern, result.getString("formatPattern"))
    }
  }

  @Test
  def changeLinkColumnFormatPatternRejectedForUnknownToken(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("formatPattern" -> "{{attributes.doesNotExist}}")
        )
      } yield ()
    }

  @Test
  def createLinkColumnFormatPatternAccepted(implicit c: TestContext): Unit = okTest {
    val pattern = "{{value}} ({{attributes.percentage}}%)"

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some(pattern))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals(pattern, result.getString("formatPattern"))
    }
  }

  @Test
  def createLinkColumnFormatPatternRejectedForUnknownToken(implicit c: TestContext): Unit =
    // same pattern that changeLinkColumnFormatPatternRejectedForUnknownToken rejects on the change path -
    // creating a link column must be rejected the same way instead of silently storing a broken pattern
    exceptionTest("unprocessable.entity") {
      createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some("{{attributes.doesNotExist}}"))
    }

}

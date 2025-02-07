package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.testtools.{TableauxTestBase, TestCustomException}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class CellLevelAnnotationsTest extends TableauxTestBase {

  @Test
  def addAnnotationWithoutLangtags(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedColumn1Flags = Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

        assertJSONEquals(exceptedColumn1Flags, rowJson1.getJsonArray("annotations").getJsonArray(0))
        assertNull(rowJson1.getJsonArray("annotations").getJsonArray(1))
      }
    }
  }

  @Test
  def retrieveSingleCellAnnotation(implicit c: TestContext): Unit = {
    okTest {
      val annotationJson = Json.obj("type" -> "info", "value" -> "I am an annotation")

      for {
        tableId <- createEmptyDefaultTable()
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")
        cellAnnotationUrl = s"/tables/$tableId/columns/1/rows/$rowId/annotations"

        _ <- sendRequest(
          "POST",
          cellAnnotationUrl,
          annotationJson
        )

        storedAnnotation <- sendRequest("GET", cellAnnotationUrl)
      } yield {

        val cellAnnotations = storedAnnotation.getJsonArray("annotations").getJsonArray(0)
        val cellAnnotation = cellAnnotations.getJsonObject(0)
        assertEquals(1, cellAnnotations.size())

        assertEquals(cellAnnotation.getString("value"), annotationJson.getString("value"))
        assertEquals(cellAnnotation.getString("type"), annotationJson.getString("type"))
      }
    }
  }

  @Test
  def retrieveMultipleCellAnnotations(implicit c: TestContext): Unit = {
    okTest {
      val annotationJson1 = Json.obj("type" -> "info", "value" -> "I am an annotation")
      val annotationJson2 = Json.obj("type" -> "error", "value" -> "I am another annotation")

      for {
        tableId <- createEmptyDefaultTable()
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")
        cellAnnotationUrl = s"/tables/$tableId/columns/1/rows/$rowId/annotations"

        _ <- sendRequest(
          "POST",
          cellAnnotationUrl,
          annotationJson1
        )
        _ <- sendRequest(
          "POST",
          cellAnnotationUrl,
          annotationJson2
        )

        storedAnnotation <- sendRequest("GET", cellAnnotationUrl)
      } yield {

        val cellAnnotations = storedAnnotation.getJsonArray("annotations").getJsonArray(0)
        assertEquals(2, cellAnnotations.size())

        val storedAnnotation1 = cellAnnotations.getJsonObject(0)
        assertEquals(storedAnnotation1.getString("value"), annotationJson1.getString("value"))
        assertEquals(storedAnnotation1.getString("type"), annotationJson1.getString("type"))

        val storedAnnotation2 = cellAnnotations.getJsonObject(1)
        assertEquals(storedAnnotation2.getString("value"), annotationJson2.getString("value"))
        assertEquals(storedAnnotation2.getString("type"), annotationJson2.getString("type"))
      }
    }
  }

  @Test
  def retrieveIgnoresRowLevelAnnotations(implicit c: TestContext): Unit = {
    val setFinal = Json.obj("final" -> true)
    val cellAnnotation = Json.obj("type" -> "info", "value" -> "some cell annotation")

    okTest {
      for {
        tableId <- createEmptyDefaultTable()
        result <- sendRequest("POST", s"/tables/$tableId/rows")

        rowId = result.getLong("id")
        annotationUrl = s"/tables/$tableId/rows/$rowId/annotations"
        cellAnnotationUrl = s"/tables/$tableId/columns/1/rows/$rowId/annotations"

        _ <- sendRequest("PATCH", annotationUrl, setFinal)
        _ <- sendRequest("POST", cellAnnotationUrl, cellAnnotation)

        annotations <- sendRequest("GET", cellAnnotationUrl)

      } yield {
        val storedAnnotations = annotations.getJsonArray("annotations").getJsonArray(0)

        assertEquals(1, storedAnnotations.size())
        val storedAnnotation = storedAnnotations.getJsonObject(0)
        assertEquals(storedAnnotation.getString("type"), cellAnnotation.getString("type"))
        assertEquals(storedAnnotation.getString("value"), cellAnnotation.getString("value"))
      }
    }
  }

  @Test
  def addAndDeleteMultipleAnnotationsWithoutLangtags(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", Json.obj("type" -> "error"))
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/annotations", Json.obj("type" -> "warning"))
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId/annotations",
          Json.obj("type" -> "error", "value" -> "this is another comment")
        )

        row <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        uuid = row.getJsonArray("annotations").getJsonArray(0).getJsonObject(0).getString("uuid")
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/$rowId/annotations/$uuid")

        rowAfterDelete <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedColumn1Annotations = Json.arr(
          Json.obj("type" -> "error", "value" -> null),
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )

        val exceptedColumn2Annotations = Json.arr(
          Json.obj("type" -> "warning", "value" -> null),
          Json.obj("type" -> "error", "value" -> "this is another comment")
        )

        val column1Annotations = row.getJsonArray("annotations").getJsonArray(0)
        val column2Annotations = row.getJsonArray("annotations").getJsonArray(1)

        assertJSONEquals(exceptedColumn1Annotations, column1Annotations)
        assertJSONEquals(exceptedColumn2Annotations, column2Annotations)

        val exceptedColumn1AnnotationsAfterDelete =
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

        val column1AnnotationsAfterDelete = rowAfterDelete.getJsonArray("annotations").getJsonArray(0)

        assertJSONEquals(exceptedColumn1AnnotationsAfterDelete, column1AnnotationsAfterDelete)

        // assert that it does NOT include the deleted annotation in the array.
        assertTrue(column1AnnotationsAfterDelete.size() == 1)
      }
    }
  }

  @Test
  def addMultipleAnnotationsWithLangtags(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("gb"), "type" -> "info", "value" -> "this is a comment")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("gb"), "type" -> "warning")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> "this is another comment")
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedColumn1Flags = Json.arr(
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> null),
          Json.obj("langtags" -> Json.arr("gb"), "type" -> "info", "value" -> "this is a comment")
        )

        val exceptedColumn2Flags = Json.arr(
          Json.obj("langtags" -> Json.arr("gb"), "type" -> "warning", "value" -> null),
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> "this is another comment")
        )

        val rowJson1Column1Annotations = rowJson1.getJsonArray("annotations").getJsonArray(0)
        val rowJson1Column2Annotations = rowJson1.getJsonArray("annotations").getJsonArray(1)

        import scala.collection.JavaConverters._

        // assert that each annotation has a UUID
        assertTrue(rowJson1Column1Annotations.asScala.map(_.asInstanceOf[JsonObject]).forall(_.containsKey("uuid")))

        assertJSONEquals(exceptedColumn1Flags, rowJson1Column1Annotations)
        assertJSONEquals(exceptedColumn2Flags, rowJson1Column2Annotations)
      }
    }
  }

  @Test
  def addAnnotationWithLangtagsOnLanguageNeutralCell(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        tableId <- createDefaultTable("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error")
        )

      } yield ()
    }
  }

  @Test
  def deleteAnnotationWithLangtagOnLanguageNeutralCell(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        tableId <- createDefaultTable("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "DELETE",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations/bb879679-38d5-44ab-980c-e78e7b1b4a7e/en"
        )

      } yield ()
    }
  }

  @Test
  def addInvalidAnnotations_invalidType(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", Json.obj("type" -> "invalid"))

      } yield ()
    }
  }

  @Test
  def addInvalidAnnotations_invalidValue(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", Json.obj("type" -> "flag"))

      } yield ()
    }
  }

  @Test
  def addAnnotations_emptyLangtags(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("type" -> "flag", "value" -> "needs_translation", "langtags" -> Json.emptyArr())
        )

      } yield ()
    }
  }

  @Test
  def addSameAnnotationsWithDifferentLangtags(implicit c: TestContext): Unit = {
    okTest {
      // Tests what happens when annotation with same type and/or value is posted multiple times.

      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de", "gb"), "type" -> "flag", "value" -> "needs_translation")
        )
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("fr", "es"), "type" -> "flag", "value" -> "needs_translation")
        )
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("cs", "nl"), "type" -> "flag", "value" -> "needs_translation")
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations with the same type and value will be merged
        assertEquals(1, rowJson1.getJsonArray("annotations").getJsonArray(0).size())

        val langtags = rowJson1
          .getJsonArray("annotations")
          .getJsonArray(0)
          .getJsonObject(0)
          .getJsonArray("langtags")

        assertJSONEquals(Json.arr(Seq("de", "gb", "fr", "es", "cs", "nl").sorted: _*), langtags)
      }
    }
  }

  @Test
  def addSameCommentAnnotations_annotationsWithTypeErrorInfoWaringShouldBeCreateableMultipleTimes(
      implicit c: TestContext
  ): Unit = {
    okTest {

      val checkMeFlag = """{"type": "flag", "value": "check-me"}"""
      val errorComment = """{"type": "error", "value": "error comment"}"""
      val warningComment = """{"type": "warning", "value": "warning comment"}"""
      val infoComment = """{"type": "info", "value": "info comment"}"""

      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        // type 'flag' should only be set once
        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", checkMeFlag)
        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", checkMeFlag)

        // any comment type can be set multiple times
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/annotations", errorComment)
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/annotations", errorComment)

        _ <- sendRequest("POST", s"/tables/$tableId/columns/3/rows/$rowId/annotations", warningComment)
        _ <- sendRequest("POST", s"/tables/$tableId/columns/3/rows/$rowId/annotations", warningComment)

        _ <- sendRequest("POST", s"/tables/$tableId/columns/4/rows/$rowId/annotations", infoComment)
        _ <- sendRequest("POST", s"/tables/$tableId/columns/4/rows/$rowId/annotations", infoComment)

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val annotations = rowJson1.getJsonArray("annotations")

        val column1Annotations = annotations.getJsonArray(0)
        val column2Annotations = annotations.getJsonArray(1)
        val column3Annotations = annotations.getJsonArray(2)
        val column4Annotations = annotations.getJsonArray(3)

        assertEquals(1, column1Annotations.size())
        assertEquals(2, column2Annotations.size())
        assertEquals(2, column3Annotations.size())
        assertEquals(2, column4Annotations.size())

        assertEquals("check-me", column1Annotations.getJsonObject(0).getString("value"))
        assertEquals("error comment", column2Annotations.getJsonObject(1).getString("value"))
        assertEquals("warning comment", column3Annotations.getJsonObject(0).getString("value"))
        assertEquals("warning comment", column3Annotations.getJsonObject(1).getString("value"))
        assertEquals("info comment", column4Annotations.getJsonObject(0).getString("value"))
        assertEquals("info comment", column4Annotations.getJsonObject(1).getString("value"))
      }
    }
  }

  @Test
  def deleteLangtagFromExistingAnnotation(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        annotation <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de", "en"), "type" -> "error")
        )

        rowJson <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- sendRequest(
          "DELETE",
          s"/tables/$tableId/columns/1/rows/$rowId/annotations/${annotation.getString("uuid")}/en"
        )

        rowJsonAfterDelete <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedFlags = Json
          .arr(Json.obj("langtags" -> Json.arr("de", "en"), "type" -> "error", "value" -> null))

        val exceptedFlagsAfterDelete = Json
          .arr(Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> null))

        assertJSONEquals(exceptedFlags, rowJson.getJsonArray("annotations").getJsonArray(0))
        assertJSONEquals(exceptedFlagsAfterDelete, rowJsonAfterDelete.getJsonArray("annotations").getJsonArray(0))
      }
    }
  }

  def addLangtag(tableId: TableId, columnId: ColumnId, rowId: RowId, langtag: String): Future[_] = {
    sendRequest(
      "POST",
      s"/tables/$tableId/columns/$columnId/rows/$rowId/annotations",
      Json.obj("langtags" -> Json.arr(langtag), "type" -> "flag", "value" -> "needs_translation")
    )
  }

  def removeLangtag(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      uuid: String,
      langtag: String
  ): Future[_] = {
    sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId/annotations/$uuid/$langtag")
  }

  @Test
  def addAndDeleteAnnotationWithLangtagsConcurrently(implicit c: TestContext): Unit = {
    okTest {

      for {
        (tableId, _) <- createTableWithMultilanguageColumns("test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- Future.sequence(
          Seq(
            addLangtag(tableId, 1, rowId, "de"),
            addLangtag(tableId, 1, rowId, "en"),
            addLangtag(tableId, 1, rowId, "fr"),
            addLangtag(tableId, 1, rowId, "es"),
            addLangtag(tableId, 1, rowId, "it"),
            addLangtag(tableId, 1, rowId, "cs"),
            addLangtag(tableId, 1, rowId, "pl")
          )
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        annotationUuid = rowJson1.getJsonArray("annotations").getJsonArray(0).getJsonObject(0).getString("uuid")

        _ <- Future.sequence(
          Seq(
            removeLangtag(tableId, 1, rowId, annotationUuid, "de"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "en"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "fr"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "es"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "it"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "cs"),
            removeLangtag(tableId, 1, rowId, annotationUuid, "pl")
          )
        )

        rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedColumn1Flags = Json.arr(
          Json.obj(
            "type" -> "flag",
            "value" -> "needs_translation",
            "langtags" -> Json.arr(
              "cs",
              "de",
              "en",
              "es",
              "fr",
              "it",
              "pl"
            )
          )
        )

        assertJSONEquals(exceptedColumn1Flags, rowJson1.getJsonArray("annotations").getJsonArray(0))
        assertNull(rowJson1.getJsonArray("annotations").getJsonArray(1))

        val exceptedColumn1FlagsAfterDelete = Json.arr(
          Json.obj(
            "type" -> "flag",
            "value" -> "needs_translation"
          )
        )

        assertJSONEquals(exceptedColumn1FlagsAfterDelete, rowJson2.getJsonArray("annotations").getJsonArray(0))
        assertFalse(
          "should not include null field",
          rowJson2
            .getJsonArray("annotations")
            .getJsonArray(0)
            .getJsonObject(0)
            .containsKey("langtaga")
        ); // JSON serialization from the server should not include null fields, such as "versionedFlows": null

        assertNull(rowJson2.getJsonArray("annotations").getJsonArray(1))
      }
    }
  }

  @Test
  def deleteLastLangtagFromAnnotationShouldDeleteAnnotationCompletely(implicit c: TestContext): Unit = {
    okTest {

      for {
        (tableId, _) <- createTableWithMultilanguageColumns("test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- Future.sequence(
          Seq(
            addLangtag(tableId, 1, rowId, "de"),
            addLangtag(tableId, 1, rowId, "en")
          )
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
        annotationUuid = rowJson1.getJsonArray("annotations").getJsonArray(0).getJsonObject(0).getString("uuid")

        _ <- removeLangtag(tableId, 1, rowId, annotationUuid, "de")
        rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- removeLangtag(tableId, 1, rowId, annotationUuid, "en")
        rowJson3 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      } yield {
        assertJSONEquals(
          Json.arr("de", "en"),
          rowJson1.getJsonArray("annotations").getJsonArray(0).getJsonObject(0).getJsonArray("langtags")
        )
        assertJSONEquals(
          Json.arr("en"),
          rowJson2.getJsonArray("annotations").getJsonArray(0).getJsonObject(0).getJsonArray("langtags")
        )
        assertNull(rowJson3.getJsonArray("annotations").getJsonArray(0).getJsonObject(0))
      }
    }
  }

  @Test
  def addAnnotationToConcatColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // make second column an identifier
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2", Json.obj("identifier" -> true))

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/0/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations array should have length 3 and only one annotation for first column
        val exceptedFlags = Json.arr(
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment")),
          null,
          null
        )

        assertJSONEquals(exceptedFlags, rowJson1.getJsonArray("annotations"))
      }
    }
  }

  @Test
  def deleteAnnotationFromConcatColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // make second column an identifier
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2", Json.obj("identifier" -> true))

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        annotation <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/0/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <-
          sendRequest("DELETE", s"/tables/$tableId/columns/0/rows/$rowId/annotations/${annotation.getString("uuid")}")

        rowJson1AfterDelete <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations array should have length 3 and only one annotation for first column
        val exceptedFlags = Json.arr(
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment")),
          null,
          null
        )

        assertJSONEquals(exceptedFlags, rowJson1.getJsonArray("annotations"))
        assertNull(rowJson1AfterDelete.getJsonArray("annotations"))
      }
    }
  }

  @Test
  def addAnnotationToNonExistingColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/0/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, _, 404) => Future.successful(())
          })

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/3/rows/$rowId/annotations",
          Json.obj("type" -> "info", "value" -> "this is a comment")
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, _, 404) => Future.successful(())
          })
      } yield ()
    }
  }

  @Test
  def retrieveAnnotationsFromSpecificTable(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId2 = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId1/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "info", "value" -> "this is a comment")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId2/annotations",
          Json.obj("langtags" -> Json.arr("gb"), "type" -> "error", "value" -> "this is another comment")
        )

        annotations <- sendRequest("GET", s"/tables/$tableId/annotations")
      } yield {
        val expectedAnnotationsByRow1 = Json.obj(
          "id" -> rowId1.toInt,
          "annotationsByColumns" -> Json.arr(
            Json.obj(
              "id" -> 1,
              "annotations" -> Json.arr(
                Json.obj(
                  "type" -> "info",
                  "value" -> "this is a comment",
                  "langtags" -> Json.arr("de")
                )
              )
            )
          )
        )

        val expectedAnnotationsByRow2 = Json.obj(
          "id" -> rowId2.toInt,
          "annotationsByColumns" -> Json.arr(
            Json.obj(
              "id" -> 2,
              "annotations" -> Json.arr(
                Json.obj(
                  "type" -> "error",
                  "value" -> "this is another comment",
                  "langtags" -> Json.arr("gb")
                )
              )
            )
          )
        )

        val annotationsByRows1 = annotations.getJsonArray("annotationsByRows").getJsonObject(0)
        val annotationsByRows2 = annotations.getJsonArray("annotationsByRows").getJsonObject(1)

        (annotationsByRows1.getInteger("id").toInt, annotationsByRows2.getInteger("id").toInt) match {
          case (1, 2) =>
            assertJSONEquals(expectedAnnotationsByRow1, annotationsByRows1)
            assertJSONEquals(expectedAnnotationsByRow2, annotationsByRows2)
          case (2, 1) =>
            assertJSONEquals(expectedAnnotationsByRow1, annotationsByRows2)
            assertJSONEquals(expectedAnnotationsByRow2, annotationsByRows1)
          case _ =>
            fail("row ids shouldn't be other than 1, 2 or 2, 1")
        }
      }
    }
  }

  @Test
  def retrieveAnnotationsFromSpecificTableAfterDeletingColumns(implicit c: TestContext): Unit = {
    okTest {

      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj(
            "columns" ->
              Json.arr(
                Json.obj("kind" -> "link", "singleDirection" -> true, "name" -> "Test Column 8", "toTable" -> tableId)
              )
          )
        )

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId2 = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId1/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "info", "value" -> "this is a comment")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/8/rows/$rowId1/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> "this is another comment")
        )

        annotationsBefore <- sendRequest("GET", s"/tables/$tableId/annotations")

        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/2")
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/8")

        annotationsAfter <- sendRequest("GET", s"/tables/$tableId/annotations")
      } yield {
        // no annotation after column was deleted
        assertFalse(annotationsBefore.getJsonArray("annotationsByRows").isEmpty)
        assertTrue(annotationsAfter.getJsonArray("annotationsByRows").isEmpty)
      }
    }
  }
}

package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.testtools.{TableauxTestBase, TestCustomException}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

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

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("type" -> "info", "value" -> "this is a comment"))

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedColumn1Flags = Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

        assertContainsDeep(exceptedColumn1Flags, rowJson1.getJsonArray("annotations").getJsonArray(0))
        assertNull(rowJson1.getJsonArray("annotations").getJsonArray(1))
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
        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("type" -> "info", "value" -> "this is a comment"))
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/annotations", Json.obj("type" -> "warning"))
        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/2/rows/$rowId/annotations",
                         Json.obj("type" -> "error", "value" -> "this is another comment"))

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

        assertContainsDeep(exceptedColumn1Annotations, column1Annotations)
        assertContainsDeep(exceptedColumn2Annotations, column2Annotations)

        val exceptedColumn1AnnotationsAfterDelete =
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

        val column1AnnotationsAfterDelete = rowAfterDelete.getJsonArray("annotations").getJsonArray(0)

        assertContainsDeep(exceptedColumn1AnnotationsAfterDelete, column1AnnotationsAfterDelete)

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

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("de"), "type" -> "error"))

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("gb"), "type" -> "info", "value" -> "this is a comment"))

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/2/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("gb"), "type" -> "warning"))

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/2/rows/$rowId/annotations",
          Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> "this is another comment"))

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

        assertContainsDeep(exceptedColumn1Flags, rowJson1Column1Annotations)
        assertContainsDeep(exceptedColumn2Flags, rowJson1Column2Annotations)
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

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("de"), "type" -> "error"))

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

        _ <- sendRequest("DELETE",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations/bb879679-38d5-44ab-980c-e78e7b1b4a7e/en")

      } yield ()
    }
  }

  @Test
  def addInvalidAnnotations(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        tableId <- createDefaultTable("Test")

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/annotations", Json.obj("type" -> "invalid"))

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

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("de", "gb"), "type" -> "flag", "value" -> "needs_translation"))
        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("fr", "es"), "type" -> "flag", "value" -> "needs_translation"))
        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                         Json.obj("langtags" -> Json.arr("cs", "nl"), "type" -> "flag", "value" -> "needs_translation"))

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations with the same type and value will be merged
        assertEquals(1, rowJson1.getJsonArray("annotations").getJsonArray(0).size())

        val langtags = rowJson1
          .getJsonArray("annotations")
          .getJsonArray(0)
          .getJsonObject(0)
          .getJsonArray("langtags")

        assertContainsDeep(Json.arr(Seq("de", "gb", "fr", "es", "cs", "nl").sorted: _*), langtags)
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

        annotation <- sendRequest("POST",
                                  s"/tables/$tableId/columns/1/rows/$rowId/annotations",
                                  Json.obj("langtags" -> Json.arr("de", "en"), "type" -> "error"))

        rowJson <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- sendRequest("DELETE",
                         s"/tables/$tableId/columns/1/rows/$rowId/annotations/${annotation.getString("uuid")}/en")

        rowJsonAfterDelete <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val exceptedFlags = Json
          .arr(Json.obj("langtags" -> Json.arr("de", "en"), "type" -> "error", "value" -> null))

        val exceptedFlagsAfterDelete = Json
          .arr(Json.obj("langtags" -> Json.arr("de"), "type" -> "error", "value" -> null))

        assertContainsDeep(exceptedFlags, rowJson.getJsonArray("annotations").getJsonArray(0))
        assertContainsDeep(exceptedFlagsAfterDelete, rowJsonAfterDelete.getJsonArray("annotations").getJsonArray(0))
      }
    }
  }

  @Test
  def addAndDeleteAnnotationWithLangtagsConcurrently(implicit c: TestContext): Unit = {
    okTest {

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
          ))

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
          ))

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
          ))

        assertContainsDeep(exceptedColumn1Flags, rowJson1.getJsonArray("annotations").getJsonArray(0))
        assertNull(rowJson1.getJsonArray("annotations").getJsonArray(1))

        val exceptedColumn1FlagsAfterDelete = Json.arr(
          Json.obj(
            "type" -> "flag",
            "value" -> "needs_translation",
            "langtags" -> null
          ))

        assertContainsDeep(exceptedColumn1FlagsAfterDelete, rowJson2.getJsonArray("annotations").getJsonArray(0))
        assertNull(rowJson2.getJsonArray("annotations").getJsonArray(1))
      }
    }
  }

  @Test
  def addAnnotationToConcatColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // make second column an identifer
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2", Json.obj("identifier" -> true))

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/0/rows/$rowId/annotations",
                         Json.obj("type" -> "info", "value" -> "this is a comment"))

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations array should have length 3 and only one annotation for first column
        val exceptedFlags = Json.arr(
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment")),
          null,
          null
        )

        assertContainsDeep(exceptedFlags, rowJson1.getJsonArray("annotations"))
      }
    }
  }

  @Test
  def deleteAnnotationFromConcatColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // make second column an identifer
        _ <- sendRequest("POST", s"/tables/$tableId/columns/2", Json.obj("identifier" -> true))

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        annotation <- sendRequest("POST",
                                  s"/tables/$tableId/columns/0/rows/$rowId/annotations",
                                  Json.obj("type" -> "info", "value" -> "this is a comment"))

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- sendRequest("DELETE",
                         s"/tables/$tableId/columns/0/rows/$rowId/annotations/${annotation.getString("uuid")}")

        rowJson1AfterDelete <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        // annotations array should have length 3 and only one annotation for first column
        val exceptedFlags = Json.arr(
          Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment")),
          null,
          null
        )

        assertContainsDeep(exceptedFlags, rowJson1.getJsonArray("annotations"))
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

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/0/rows/$rowId/annotations",
                         Json.obj("type" -> "info", "value" -> "this is a comment"))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, _, 404) => Future.successful(())
          })

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/3/rows/$rowId/annotations",
                         Json.obj("type" -> "info", "value" -> "this is a comment"))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, _, 404) => Future.successful(())
          })
      } yield ()
    }
  }
}

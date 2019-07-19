package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class AnnotationCountTest extends TableauxTestBase {

  @Test
  def retrieveFlagCountForOneTableWithTwoRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId2 = result.getLong("id")

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId1/annotations",
                         Json.obj("langtags" -> Json.arr("en"), "type" -> "flag", "value" -> "needs_translation"))

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId2/annotations",
                         Json.obj("langtags" -> Json.arr("en"), "type" -> "flag", "value" -> "needs_translation"))

        annotationCount <- sendRequest("GET", s"/tables/annotationCount")
      } yield {
        val expectedAnnotationCount = Json.obj(
          "tables" -> Json.arr(
            Json.obj(
              "id" -> tableId.toInt,
              "totalSize" -> 2,
              "annotationCount" -> Json.arr(
                Json.obj(
                  "type" -> "flag",
                  "value" -> "needs_translation",
                  "langtag" -> "en",
                  "count" -> 2
                )
              )
            )
          )
        )

        assertJSONEquals(expectedAnnotationCount, annotationCount)
      }
    }
  }

  @Test
  def retrieveFlagCountForTwoTablesWithOneRowEach(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId1, _) <- createTableWithMultilanguageColumns("Test1")
        (tableId2, _) <- createTableWithMultilanguageColumns("Test2")

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId1/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId2/rows")
        rowId2 = result.getLong("id")

        _ <- sendRequest("POST",
                         s"/tables/$tableId1/columns/1/rows/$rowId1/annotations",
                         Json.obj("langtags" -> Json.arr("en"), "type" -> "flag", "value" -> "needs_translation"))

        _ <- sendRequest("POST",
                         s"/tables/$tableId2/columns/1/rows/$rowId2/annotations",
                         Json.obj("langtags" -> Json.arr("en"), "type" -> "flag", "value" -> "needs_translation"))

        annotationCount <- sendRequest("GET", s"/tables/annotationCount")
      } yield {
        val expectedAnnotationCount = Json.obj(
          "tables" -> Json.arr(
            Json.obj(
              "id" -> tableId1.toInt,
              "totalSize" -> 1,
              "annotationCount" -> Json.arr(
                Json.obj(
                  "type" -> "flag",
                  "value" -> "needs_translation",
                  "langtag" -> "en",
                  "count" -> 1
                )
              )
            ),
            Json.obj(
              "id" -> tableId2.toInt,
              "totalSize" -> 1,
              "annotationCount" -> Json.arr(
                Json.obj(
                  "type" -> "flag",
                  "value" -> "needs_translation",
                  "langtag" -> "en",
                  "count" -> 1
                )
              )
            )
          )
        )

        assertJSONEquals(expectedAnnotationCount, annotationCount)
      }
    }
  }

  @Test
  def retrieveInfoCountForOneTableWithTwoRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, _) <- createTableWithMultilanguageColumns("Test")

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId2 = result.getLong("id")

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId1/annotations",
                         Json.obj("type" -> "info", "value" -> "Test Row 1"))

        _ <- sendRequest("POST",
                         s"/tables/$tableId/columns/1/rows/$rowId2/annotations",
                         Json.obj("type" -> "info", "value" -> "Test Row 2"))

        annotationCount <- sendRequest("GET", s"/tables/annotationCount")
      } yield {
        /* values of annotations other than type flag will be ignored */

        val expectedAnnotationCount = Json.obj(
          "tables" -> Json.arr(
            Json.obj(
              "id" -> tableId.toInt,
              "totalSize" -> 2,
              "annotationCount" -> Json.arr(
                Json.obj(
                  "type" -> "info",
                  "value" -> null,
                  "langtag" -> null,
                  "count" -> 2
                )
              )
            )
          )
        )

        assertJSONEquals(expectedAnnotationCount, annotationCount)
      }
    }
  }
}

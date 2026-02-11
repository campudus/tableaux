package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.JsonAssertable.JsonObject
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.UnionTableTestHelper

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import java.net.URLEncoder
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class GetStructureTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def retrieveTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "attributes" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertJSONEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveAllTables(implicit c: TestContext): Unit = {
    okTest {
      val baseExpected = Json.obj(
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "attributes" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )
      val expectedJson = Json.obj(
        "status" -> "ok",
        "tables" -> Json.arr(
          baseExpected.copy().mergeIn(Json.obj("id" -> 1, "name" -> "Test Table 1")),
          baseExpected.copy().mergeIn(Json.obj("id" -> 2, "name" -> "Test Table 2"))
        )
      )

      for {
        _ <- createDefaultTable("Test Table 1")
        _ <- createDefaultTable("Test Table 2")
        test <- sendRequest("GET", "/tables")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveColumns(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "name" -> "Test Column 1",
            "kind" -> "text",
            "ordering" -> 1,
            "multilanguage" -> false,
            "identifier" -> true,
            "displayName" -> Json.obj(),
            "attributes" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "name" -> "Test Column 2",
            "kind" -> "numeric",
            "ordering" -> 2,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "attributes" -> Json.obj(),
            "description" -> Json.obj()
          )
        )
      )

      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveColumnsFilteredByIds(implicit c: TestContext): Unit = {
    okTest {
      val expectedJsonAll = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj("id" -> 1, "name" -> "Test Column 1"),
          Json.obj("id" -> 2, "name" -> "Test Column 2"),
          Json.obj("id" -> 3, "name" -> "Test Column 3"),
          Json.obj("id" -> 4, "name" -> "Test Column 4"),
          Json.obj("id" -> 5, "name" -> "Test Column 5"),
          Json.obj("id" -> 6, "name" -> "Test Column 6"),
          Json.obj("id" -> 7, "name" -> "Test Column 7")
        )
      )
      val expectedJsonFiltered = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj("id" -> 2, "name" -> "Test Column 2"),
          Json.obj("id" -> 4, "name" -> "Test Column 4"),
          Json.obj("id" -> 7, "name" -> "Test Column 7")
        )
      )

      for {
        _ <- createFullTableWithMultilanguageColumns("Test Table")
        testAll <- sendRequest("GET", "/tables/1/columns")
        testFiltered <- sendRequest("GET", "/tables/1/columns?columnIds=2,4,7")
        // skips non existing ids
        testFilteredWithNonExisting <- sendRequest("GET", "/tables/1/columns?columnIds=2,33,4,55,7")
      } yield {
        assertJSONEquals(expectedJsonAll, testAll)
        assertJSONEquals(expectedJsonFiltered, testFiltered)
        assertJSONEquals(expectedJsonFiltered, testFiltered)
        assertJSONEquals(expectedJsonFiltered, testFilteredWithNonExisting)
      }
    }
  }

  @Test
  def retrieveColumnsFilteredByIdsInvalidNoNegative(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns?columnIds=-1")
      } yield ()
    }
  }

  @Test
  def retrieveColumnsFilteredByIdsValidWithConcat(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns?columnIds=0")
      } yield ()
    }
  }

  @Test
  def retrieveColumnsFilteredInvalidNoCombination(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns?columnIds=1&columnNames=name")
      } yield ()
    }
  }

  @Test
  def retrieveColumnsFilteredByNames(implicit c: TestContext): Unit = {
    okTest {
      val expectedJsonAll = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj("id" -> 1, "name" -> "Test Column 1"),
          Json.obj("id" -> 2, "name" -> "Test Column 2"),
          Json.obj("id" -> 3, "name" -> "Test Column 3"),
          Json.obj("id" -> 4, "name" -> "Test Column 4"),
          Json.obj("id" -> 5, "name" -> "Test Column 5"),
          Json.obj("id" -> 6, "name" -> "Test Column 6"),
          Json.obj("id" -> 7, "name" -> "Test Column 7")
        )
      )
      val expectedJsonFiltered = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj("id" -> 2, "name" -> "Test Column 2"),
          Json.obj("id" -> 4, "name" -> "Test Column 4"),
          Json.obj("id" -> 7, "name" -> "Test Column 7")
        )
      )
      val columnNamesQuery = URLEncoder.encode("Test Column 2,Test Column 4,Test Column 7", "UTF-8");
      val columnNamesWithNonExistingQuery =
        URLEncoder.encode("Test Column 2,name,Test Column 4,image,Test Column 7", "UTF-8");

      for {
        _ <- createFullTableWithMultilanguageColumns("Test Table")
        testAll <- sendRequest("GET", "/tables/1/columns")
        testFiltered <- sendRequest("GET", s"/tables/1/columns?columnNames=$columnNamesQuery")
        testFilteredWithNonExisting <-
          sendRequest("GET", s"/tables/1/columns?columnNames=$columnNamesWithNonExistingQuery")
      } yield {
        assertJSONEquals(expectedJsonAll, testAll)
        assertJSONEquals(expectedJsonFiltered, testFiltered)
        // skips non existing names
        assertJSONEquals(expectedJsonFiltered, testFilteredWithNonExisting)
      }
    }
  }

  @Test
  def retrieveColumnsFilteredByNamesValidWithConcat(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns?columnNames=ID")
      } yield ()
    }
  }

  @Test
  def retrieveStringColumn(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Test Column 1",
        "kind" -> "text",
        "ordering" -> 1,
        "multilanguage" -> false,
        "identifier" -> true,
        "displayName" -> Json.obj(),
        "attributes" -> Json.obj(),
        "description" -> Json.obj()
      )

      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns/1")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveNumberColumn(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 2,
        "name" -> "Test Column 2",
        "kind" -> "numeric",
        "ordering" -> 2,
        "multilanguage" -> false,
        "identifier" -> false,
        "displayName" -> Json.obj(),
        "attributes" -> Json.obj(),
        "description" -> Json.obj()
      )

      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns/2")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveTableStructure(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "tables" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "name" -> "Test Table 1",
            "langtags" -> Json.arr("de-DE", "en-GB"),
            "columns" -> Json.arr(
              Json.obj(
                "id" -> 1,
                "name" -> "Test Column 1",
                "kind" -> "text",
                "identifier" -> true,
                "multilanguage" -> false,
                "attributes" -> Json.obj(),
                "ordering" -> 1
              ),
              Json.obj(
                "id" -> 2,
                "name" -> "Test Column 2",
                "kind" -> "numeric",
                "identifier" -> false,
                "multilanguage" -> false,
                "attributes" -> Json.obj(),
                "ordering" -> 2
              )
            )
          )
        )
      )

      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/structure")
      } yield {
        assertJSONEquals(
          expectedJson,
          test
        )
      }
    }
  }

  @Test
  def retrieveSingleTableStructure(implicit c: TestContext): Unit = okTest {
    val table1Json = Json.obj(
      "id" -> 1,
      "name" -> "Test Table 1",
      "langtags" -> Json.arr("de-DE", "en-GB"),
      "columns" -> Json.arr(
        Json.obj(
          "id" -> 1,
          "name" -> "Test Column 1",
          "kind" -> "text",
          "identifier" -> true,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 1
        ),
        Json.obj(
          "id" -> 2,
          "name" -> "Test Column 2",
          "kind" -> "numeric",
          "identifier" -> false,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 2
        ),
        Json.obj(
          "id" -> 3,
          "name" -> "Link 1->2",
          "kind" -> "link",
          "identifier" -> false,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 3
        )
      )
    )

    val table2Json = Json.obj(
      "id" -> 2,
      "name" -> "Test Table 2",
      "langtags" -> Json.arr("de-DE", "en-GB"),
      "columns" -> Json.arr(
        Json.obj(
          "id" -> 1,
          "name" -> "Test Column 1",
          "kind" -> "text",
          "identifier" -> true,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 1
        ),
        Json.obj(
          "id" -> 2,
          "name" -> "Test Column 2",
          "kind" -> "numeric",
          "identifier" -> false,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 2
        ),
        Json.obj(
          "id" -> 3,
          "name" -> "Link 2->3",
          "kind" -> "link",
          "identifier" -> false,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 3
        )
      )
    )

    val table3Json = Json.obj(
      "id" -> 3,
      "name" -> "Test Table 3",
      "langtags" -> Json.arr("de-DE", "en-GB"),
      "columns" -> Json.arr(
        Json.obj(
          "id" -> 1,
          "name" -> "Test Column 1",
          "kind" -> "text",
          "identifier" -> true,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 1
        ),
        Json.obj(
          "id" -> 2,
          "name" -> "Test Column 2",
          "kind" -> "numeric",
          "identifier" -> false,
          "multilanguage" -> false,
          "attributes" -> Json.obj(),
          "ordering" -> 2
        )
      )
    )

    for {
      _ <- createEmptyDefaultTable("Test Table 1")
      _ <- createEmptyDefaultTable("Test Table 2")
      _ <- createEmptyDefaultTable("Test Table 3")

      _ <- sendRequest(
        "POST",
        s"/tables/1/columns",
        Json.obj("columns" -> Json.arr(
          Json.obj(
            "name" -> "Link 1->2",
            "toTable" -> 2,
            "kind" -> "link",
            "singleDirection" -> true
          )
        ))
      )
      _ <- sendRequest(
        "POST",
        "/tables/2/columns",
        Json.obj("columns" -> Json.arr(
          Json.obj(
            "name" -> "Link 2->3",
            "toTable" -> 3,
            "kind" -> "link",
            "singleDirection" -> true
          )
        ))
      )

      structureTable1 <- sendRequest("GET", "/structure/1")
      structureTable2 <- sendRequest("GET", "/structure/2")
      structureTable3 <- sendRequest("GET", "/structure/3")
    } yield {
      logger.info(s"$structureTable3")
      assertJSONEquals(Json.obj("tables" -> Json.arr(table1Json, table2Json, table3Json)), structureTable1)
      assertJSONEquals(Json.obj("tables" -> Json.arr(table2Json, table3Json)), structureTable2)
      assertJSONEquals(Json.obj("tables" -> Json.arr(table3Json)), structureTable3)
    }
  }

  @Test
  def retrieveSingleTableStructure_unionTable(implicit c: TestContext): Unit = okTest {
    val expectedJsonUnion = Json.obj(
      "tables" -> Json.arr(
        Json.obj(
          "id" -> 1 // nested link table
        ),
        Json.obj(
          "id" -> 2
        ),
        Json.obj(
          "id" -> 3
        ),
        Json.obj(
          "id" -> 4
        ),
        Json.obj(
          "id" -> 5,
          "name" -> "union",
          "hidden" -> false,
          "displayName" -> Json.obj("de" -> "Union Table"),
          "langtags" -> Json.arr("de-DE", "en-GB"),
          "type" -> "union",
          "originTables" -> Json.arr(2, 4, 3)
        )
      )
    )

    val expectedJsonNotUnion = Json.obj(
      "tables" -> Json.arr(
        Json.obj(
          "id" -> 6,
          "name" -> "Test Table Not Union"
        )
      )
    )

    for {
      unionTableId <- createUnionTable(false, true)
      notUnionTableId <- createEmptyDefaultTable("Test Table Not Union")

      structureUnionTable <- sendRequest("GET", s"/structure/$unionTableId")
      structureNotUnionTable <- sendRequest("GET", s"/structure/$notUnionTableId")
    } yield {
      assertJSONEquals(expectedJsonUnion, structureUnionTable)
      assertJSONEquals(expectedJsonNotUnion, structureNotUnionTable)
    }
  }
}

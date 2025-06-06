package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.{TableauxTestBase, TestCustomException}
import com.campudus.tableaux.testtools.RequestCreation._

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

@RunWith(classOf[VertxUnitRunner])
class GroupColumnTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "table1")

  def createTextColumnJson(name: String) = Columns(TextCol(name)).getJson

  def createBooleanColumnJson(name: String) = Columns(BooleanCol(name)).getJson

  def createGroupColumnJson(name: String, groups: Seq[ColumnId], showMemberColumns: Boolean = false) =
    Columns(GroupCol(name, groups, showMemberColumns)).getJson

  def createGroupColumnWithFormatPatternJson(name: String, groups: Seq[ColumnId], formatPattern: String) =
    Columns(FormattedGroupCol(name, groups, formatPattern)).getJson

  def sendCreateColumnRequest(tableId: TableId, columnsJson: DomainObject): Future[Int] =
    sendCreateColumnRequest(tableId, columnsJson.getJson)

  def sendCreateColumnRequest(tableId: TableId, columnsJson: JsonObject): Future[Int] =
    sendRequest("POST", s"/tables/$tableId/columns", columnsJson)
      .map(_.getJsonArray("columns").getJsonObject(0).getInteger("id"))

  @Test
  def createGroupColumnWithTwoColumns(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumn <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))
      } yield {
        assertJSONEquals(
          Json.obj("groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId))),
          groupColumn
        )
      }
    }
  }

  @Test
  def createGroupColumnWithTwoColumnsAndRetrieveIt(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnRetrieved <- sendRequest("GET", s"/tables/1/columns/${groupColumnCreated.getInteger("id")}")
      } yield {
        assertJSONEquals(
          Json.obj("groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId))),
          groupColumnCreated
        )

        assertJSONEquals(
          Json.obj("groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId))),
          groupColumnRetrieved
        )
      }
    }
  }

  @Test
  def createIdentifierGroupColumnWithTwoColumns(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(Identifier(GroupCol("groupcolumn", Seq(textColumnId, booleanColumnId))))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        columns <- sendRequest("GET", "/tables/1/columns")
          .map(_.getJsonArray("columns"))

        _ <- sendRequest("GET", "/tables/1/columns/0")
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "NOT FOUND", _) => Future.successful(())
          })
      } yield {
        // grouped columns will be identifier columns if groupcolumn is
        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(
              Json.obj("id" -> textColumnId),
              Json.obj("id" -> booleanColumnId)
            )
          ),
          groupColumnCreated
        )

        assertEquals(3, columns.size())
      }
    }
  }

  @Test
  def createTwoGroupColumnWithSameColumns(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumn1 <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId)))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumn2 <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(GroupCol("groupcolumn2", Seq(textColumnId, booleanColumnId)))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        columns <- sendRequest("GET", "/tables/1/columns")
          .map(_.getJsonArray("columns"))

      } yield {
        val expected = Json.obj(
          "groups" -> Json.arr(
            Json.obj("id" -> textColumnId),
            Json.obj("id" -> booleanColumnId)
          )
        )

        assertJSONEquals(expected, groupColumn1)
        assertJSONEquals(expected, groupColumn2)

        assertEquals(4, columns.size())
      }
    }
  }

  @Test
  def createGroupColumnWithGroupColumn(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumn1 <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId)))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        _ <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(GroupCol("groupcolumn2", Seq(groupColumn1.getLong("id"), booleanColumnId)))
        )
      } yield ()
    }
  }

  @Test
  def createGroupColumnWithUnknownColumn(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        _ <- sendRequest(
          "POST",
          "/tables/1/columns",
          Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId, 100)))
        )
      } yield ()
    }
  }

  @Test
  def createLinkColumnWhichPointsToGroupColumnWithTwoColumnsAndDoChanges(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- sendRequest("POST", "/tables", Json.obj("name" -> "table1")).map(_.getLong("id"))
        tableId2 <- sendRequest("POST", "/tables", Json.obj("name" -> "table2")).map(_.getLong("id"))

        // columns for table 1
        textColumnId1 <- sendCreateColumnRequest(tableId1, Columns(TextCol("textcolumn11")))
        booleanColumnId1 <- sendCreateColumnRequest(tableId1, Columns(BooleanCol("booleancolumn12")))

        groupColumnCreate = Columns(Identifier(GroupCol("groupcolumn12", Seq(textColumnId1, booleanColumnId1))))

        groupColumnCreated <- sendRequest("POST", s"/tables/$tableId1/columns", groupColumnCreate)
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnId = groupColumnCreated.getInteger("id")

        // columns for table 2
        textColumnId2 <- sendCreateColumnRequest(tableId2, Columns(Identifier(TextCol("textcolumn21"))))
        booleanColumnId2 <- sendCreateColumnRequest(tableId2, Columns(BooleanCol("booleancolumn22")))
        linkColumnId2 <- sendCreateColumnRequest(tableId2, Columns(LinkBiDirectionalCol("linkcolumn23", tableId1)))

        columns1 <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))
        columns2 <- sendRequest("GET", s"/tables/$tableId2/columns").map(_.getJsonArray("columns"))

        _ <- sendRequest("PATCH", s"/tables/1/columns/$textColumnId1", Json.obj("name" -> "textcolumn11changed"))

        columns2AfterChange <- sendRequest("GET", s"/tables/$tableId2/columns").map(_.getJsonArray("columns"))
        linkColumn2AfterChange <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2")
      } yield {
        assertEquals(4, columns1.size())
        assertEquals(3, columns2.size())

        val expectedColumns2 = Json.arr(
          Json.obj("id" -> textColumnId2),
          Json.obj("id" -> booleanColumnId2),
          Json.obj(
            "id" -> linkColumnId2,
            "toColumn" -> Json.obj(
              "id" -> groupColumnId,
              "groups" -> Json.arr(
                Json.obj("id" -> textColumnId1, "name" -> "textcolumn11"),
                Json.obj("id" -> booleanColumnId1)
              )
            )
          )
        )

        assertJSONEquals(expectedColumns2, columns2)

        val expectedColumns2AfterChange = Json.arr(
          Json.obj("id" -> textColumnId2),
          Json.obj("id" -> booleanColumnId2),
          Json.obj(
            "id" -> linkColumnId2,
            "toColumn" -> Json.obj(
              "id" -> groupColumnId,
              "groups" -> Json.arr(
                Json.obj("id" -> textColumnId1, "name" -> "textcolumn11changed"),
                Json.obj("id" -> booleanColumnId1)
              )
            )
          )
        )

        assertJSONEquals(expectedColumns2AfterChange, columns2AfterChange)

        val expectedLinkColumn2AfterChange = Json.obj(
          "id" -> linkColumnId2,
          "toColumn" -> Json.obj(
            "id" -> groupColumnId,
            "groups" -> Json.arr(
              Json.obj("id" -> textColumnId1, "name" -> "textcolumn11changed"),
              Json.obj("id" -> booleanColumnId1)
            )
          )
        )

        assertJSONEquals(expectedLinkColumn2AfterChange, linkColumn2AfterChange)
      }
    }
  }

  @Test
  def createGroupColumnWithTwoColumnsAndDoChanges(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumnCreated <-
          sendRequest("POST", "/tables/1/columns", Columns(GroupCol("groupcolumn", Seq(textColumnId, booleanColumnId))))
            .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnId = groupColumnCreated.getInteger("id")

        columnsBeforeDelete <- sendRequest("GET", s"/tables/1/columns").map(_.getJsonArray("columns"))
        groupColumnBeforeDelete <- sendRequest("GET", s"/tables/1/columns/$groupColumnId")

        _ <- sendRequest("DELETE", s"/tables/1/columns/$booleanColumnId")

        columnsAfterDelete <- sendRequest("GET", s"/tables/1/columns").map(_.getJsonArray("columns"))
        groupColumnAfterDelete <- sendRequest("GET", s"/tables/1/columns/$groupColumnId")

        _ <- sendRequest("PATCH", s"/tables/1/columns/$textColumnId", Json.obj("name" -> "textcolumn2"))

        columnsAfterChange <- sendRequest("GET", s"/tables/1/columns").map(_.getJsonArray("columns"))
        groupColumnAfterChange <- sendRequest("GET", s"/tables/1/columns/$groupColumnId")
      } yield {
        // before delete
        assertEquals(3, columnsBeforeDelete.size())

        assertJSONEquals(
          Json.arr(
            Json.obj("id" -> textColumnId),
            Json.obj("id" -> booleanColumnId),
            Json.obj(
              "id" -> groupColumnId,
              "groups" -> Json.arr(
                Json.obj("id" -> textColumnId),
                Json.obj("id" -> booleanColumnId)
              )
            )
          ),
          columnsBeforeDelete
        )

        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(
              Json.obj("id" -> textColumnId),
              Json.obj("id" -> booleanColumnId)
            )
          ),
          groupColumnBeforeDelete
        )

        // after delete
        assertEquals(2, columnsAfterDelete.size())

        assertJSONEquals(
          Json.arr(
            Json.obj("id" -> textColumnId),
            Json.obj(
              "id" -> groupColumnId,
              "groups" -> Json.arr(
                Json.obj("id" -> textColumnId, "name" -> "textcolumn")
              )
            )
          ),
          columnsAfterDelete
        )

        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(
              Json.obj("id" -> textColumnId, "name" -> "textcolumn")
            )
          ),
          groupColumnAfterDelete
        )

        // after change
        assertEquals(2, columnsAfterChange.size())

        assertJSONEquals(
          Json.arr(
            Json.obj("id" -> textColumnId),
            Json.obj(
              "id" -> groupColumnId,
              "groups" -> Json.arr(
                Json.obj("id" -> textColumnId, "name" -> "textcolumn2")
              )
            )
          ),
          columnsAfterChange
        )

        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(
              Json.obj("id" -> textColumnId, "name" -> "textcolumn2")
            )
          ),
          groupColumnAfterChange
        )
      }
    }
  }

  @Test
  def createSingleGroupColumnWithValidFormat(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))

        groupColumn <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnWithFormatPatternJson(
            "groupcolumn",
            Seq(textColumnId),
            "The value '{{1}}' with fancy format"
          )
        )
          .map(_.getJsonArray("columns").getJsonObject(0))
      } yield {
        val expected = """{
                         |  "groups": [{"id": 1}],
                         |  "formatPattern": "The value '{{1}}' with fancy format"
                         |}""".stripMargin

        assertJSONEquals(expected, groupColumn.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def createTripleGroupColumn(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textCol1 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn1"))
        textCol2 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn2"))
        textCol3 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn3"))

        groupColumn <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnJson("groupcolumn", Seq(textCol1, textCol2, textCol3))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))
      } yield {
        val expected = """{
                         |  "groups": [
                         |    {"id": 1},
                         |    {"id": 2},
                         |    {"id": 3}
                         |  ]
                         |}""".stripMargin

        assertJSONEquals(expected, groupColumn.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def createTripleGroupColumnWithValidFormat(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textCol1 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn1"))
        textCol2 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn2"))
        textCol3 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn3"))

        groupColumn <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnWithFormatPatternJson(
            "groupcolumn",
            Seq(textCol1, textCol2, textCol3),
            "{{1}} × {{2}} × {{3}} mm (B × H × T)"
          )
        ).map(_.getJsonArray("columns").getJsonObject(0))
      } yield {
        val expected = """{
                         |  "groups": [
                         |    {"id": 1},
                         |    {"id": 2},
                         |    {"id": 3}
                         |  ],
                         |  "formatPattern": "{{1}} × {{2}} × {{3}} mm (B × H × T)"
                         |}""".stripMargin

        assertJSONEquals(expected, groupColumn.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def createAndChangeTripleGroupColumnWithValidFormat(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textCol1 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn1"))
        textCol2 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn2"))
        textCol3 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn3"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnWithFormatPatternJson(
            "groupcolumn",
            Seq(textCol1, textCol2, textCol3),
            "{{1}} × {{2}} × {{3}} mm (B × H × T)"
          )
        ).map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnChanged <- sendRequest(
          "POST",
          s"/tables/1/columns/${groupColumnCreated.getInteger("id")}",
          Json.obj(
            "formatPattern" -> "{{1}} × {{2}} mm (B × H)"
          )
        )
      } yield {
        val expected = """{
                         |  "groups": [
                         |    {"id": 1},
                         |    {"id": 2},
                         |    {"id": 3}
                         |  ],
                         |  "formatPattern": "{{1}} × {{2}} mm (B × H)"
                         |}""".stripMargin

        assertJSONEquals(expected, groupColumnChanged.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def createAndChangeTripleGroupColumnWithInvalidFormat(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textCol1 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn1"))
        textCol2 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn2"))
        textCol3 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn3"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnWithFormatPatternJson(
            "groupcolumn",
            Seq(textCol1, textCol2, textCol3),
            "{{1}} × {{2}} × {{3}} mm (B × H × T)"
          )
        ).map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnChanged <- sendRequest(
          "POST",
          s"/tables/1/columns/${groupColumnCreated.getInteger("id")}",
          Json.obj(
            "formatPattern" -> "{{1}} × {{33}} mm (B × H)"
          )
        )
      } yield ()
    }
  }

  @Test
  def createSingleGroupColumnWithInvalidFormat(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textCol1 <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn1"))

        groupColumn <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnWithFormatPatternJson("groupcolumn", Seq(textCol1), "{{1}} × {{2}} mm")
        )
      } yield ()
    }
  }

  @Test
  def createGroupColumnWithTwoColumnsAndWithShowMemberColumnsTrue(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId), true)
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnRetrieved <- sendRequest("GET", s"/tables/1/columns/${groupColumnCreated.getInteger("id")}")
      } yield {
        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId)),
            "showMemberColumns" -> true
          ),
          groupColumnCreated
        )

        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId)),
            "showMemberColumns" -> true
          ),
          groupColumnRetrieved
        )
      }
    }
  }

  @Test
  def changeGroupColumn_setShowMemberColumnsToTrue(implicit c: TestContext): Unit = {
    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
        booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

        groupColumnCreated <- sendRequest(
          "POST",
          "/tables/1/columns",
          createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId))
        )
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnChanged <- sendRequest(
          "POST",
          s"/tables/1/columns/${groupColumnCreated.getInteger("id")}",
          Json.obj(
            "showMemberColumns" -> true
          )
        )
      } yield {
        assertNull(groupColumnCreated.getBoolean("showMemberColumns"))

        assertJSONEquals(
          Json.obj(
            "groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId)),
            "showMemberColumns" -> true
          ),
          groupColumnChanged
        )
      }
    }
  }
}

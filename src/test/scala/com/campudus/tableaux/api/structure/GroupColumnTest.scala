package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.RequestCreation.{Columns, GroupCol, Identifier}
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase, TestCustomException}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class GroupColumnTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "table1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createBooleanColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.BooleanCol(name)).getJson

  def createGroupColumnJson(name: String, groups: Seq[ColumnId]) =
    RequestCreation.Columns().add(RequestCreation.GroupCol(name, groups)).getJson

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

        groupColumn <- sendRequest("POST",
                                   "/tables/1/columns",
                                   createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId)))
          .map(_.getJsonArray("columns").getJsonObject(0))
      } yield {
        assertContainsDeep(
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

        groupColumnCreated <- sendRequest("POST",
                                          "/tables/1/columns",
                                          createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId)))
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumnRetrieved <- sendRequest("GET", s"/tables/1/columns/${groupColumnCreated.getInteger("id")}")
      } yield {
        assertContainsDeep(
          Json.obj("groups" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> booleanColumnId))),
          groupColumnCreated
        )

        assertContainsDeep(
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
          Columns(Identifier(GroupCol("groupcolumn", Seq(textColumnId, booleanColumnId)))))
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
        assertContainsDeep(
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

        groupColumn1 <- sendRequest("POST",
                                    "/tables/1/columns",
                                    Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId))))
          .map(_.getJsonArray("columns").getJsonObject(0))

        groupColumn2 <- sendRequest("POST",
                                    "/tables/1/columns",
                                    Columns(GroupCol("groupcolumn2", Seq(textColumnId, booleanColumnId))))
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

        assertContainsDeep(expected, groupColumn1)
        assertContainsDeep(expected, groupColumn2)

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

        groupColumn1 <- sendRequest("POST",
                                    "/tables/1/columns",
                                    Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId))))
          .map(_.getJsonArray("columns").getJsonObject(0))

        _ <- sendRequest("POST",
                         "/tables/1/columns",
                         Columns(GroupCol("groupcolumn2", Seq(groupColumn1.getLong("id"), booleanColumnId))))
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

        _ <- sendRequest("POST",
                         "/tables/1/columns",
                         Columns(GroupCol("groupcolumn1", Seq(textColumnId, booleanColumnId, 100))))
      } yield ()
    }
  }
}

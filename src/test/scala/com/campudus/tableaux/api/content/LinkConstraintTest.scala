package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{Cardinality, Constraint, DefaultCardinality}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.JsonUtils.asCastedList
import com.campudus.tableaux.testtools.RequestCreation.{Columns, LinkBiDirectionalCol, Rows}
import com.campudus.tableaux.testtools.TestCustomException

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future

import org.junit.{Ignore, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

sealed trait Helper extends LinkTestBase {

  def findByNameInColumnsArray(columnName: String)(jsonArray: JsonArray): JsonObject = {
    import scala.collection.JavaConverters._

    jsonArray.asScala
      .collect({
        case obj: JsonObject => obj
      })
      .toSeq
      .find(_.getString("name") == columnName)
      .orNull
  }

  def toRowsArray(obj: JsonObject): JsonArray = {
    obj.getJsonArray("rows")
  }

  def createCardinalityLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String,
      from: Int,
      to: Int
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(Cardinality(from, to), deleteCascade = false))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }

  def createArchiveCascadeLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(DefaultCardinality, archiveCascade = true))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }

  def filterArchivedRows(jsonArray: JsonArray): Seq[JsonObject] = {
    import scala.collection.JavaConverters._

    jsonArray.asScala
      .collect({
        case obj: JsonObject => obj
      }).toSeq
      .filter(_.getBoolean("archived"))
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkDeleteCascadeTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithDeleteCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("deleteCascade", tableId2, Constraint(DefaultCardinality, deleteCascade = true))
        )

        // create bi-directional link column
        createdDeleteCascadeLinkColumnTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveDeleteCascadeLinkColumnTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("deleteCascade"))

        // retrieve bi-directional backlink from table 2
        retrieveDeleteCascadeLinkColumnTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertJSONEquals(
          Constraint(DefaultCardinality, deleteCascade = true).getJson,
          createdDeleteCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertJSONEquals(
          Constraint(DefaultCardinality, deleteCascade = true).getJson,
          retrieveDeleteCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertNull(retrieveDeleteCascadeLinkColumnTable2.getJsonObject("constraint"))
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(2, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteForeignRowsWithDeleteCascadeShouldNotDeleteParentRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))

        // delete foreign rows
        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1")
        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/2")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        // parent rows should still be there
        assertEquals(3, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithoutDeleteCascadeShouldNotDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId1/columns",
          Columns(
            LinkBiDirectionalCol("deleteCascade", tableId2, Constraint(DefaultCardinality, deleteCascade = false))
          )
        )

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(2, rowsTable1.size())
        assertEquals(2, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldNotDeleteForeignRowsIfStillInUse(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(2))))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(1, rowsTable2.size())

        // row 2 was still in use so only row was deleted
        assertEquals(2, rowsTable2.getJsonObject(0).getLong("id").longValue())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldNotDeleteForeignRowsIfNotLinked(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(1, rowsTable1.size())
        assertEquals(2, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeWhichTriggersDeleteCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        table2LinkColumnId <- createDeleteCascadeLinkColumn(tableId2, tableId1, "deleteCascade2")

        _ <- sendRequest("POST", s"/tables/$tableId1/rows")

        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 2))
        )

        _ <-
          sendRequest("PUT", s"/tables/$tableId2/columns/$table2LinkColumnId/rows/1", Json.obj("value" -> Json.arr(2)))

        _ <-
          sendRequest("PUT", s"/tables/$tableId2/columns/$table2LinkColumnId/rows/2", Json.obj("value" -> Json.arr(3)))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(0, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def putLinkWithDeleteCascadeShouldNotDeleteForeignRowsIfTheyAreAlsoInNewLinkList_simple(
      implicit c: TestContext
  ): Unit = {
    okTest {
      for {
        // Given current links: [1, 2] -> after test: [2]
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 2))
        )

        // When
        _ <-
          sendRequest("PUT", s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1", Json.obj("value" -> Json.arr(2)))

        // Then
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(1, rowsTable2.size())
      }
    }
  }

  @Test
  def putLinkWithDeleteCascadeShouldNotDeleteForeignRowsIfTheyAreAlsoInNewLinkList_onlyDelete(
      implicit c: TestContext
  ): Unit = {
    okTest {
      for {
        // Given current links: [1, 2, 3] -> after test: [1, 3]
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        _ <- sendRequest("POST", s"/tables/$tableId2/rows")
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/2/rows/3", Json.obj("value" -> 3))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/1/rows/3", Json.obj("value" -> s"table${tableId2}row3"))

        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 2, 3))
        )

        // When
        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 3))
        )

        // Then
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
        linkCellTable1 <- sendRequest("GET", s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1")
          .map(_.getJsonArray("value"))
      } yield {
        assertEquals(2, rowsTable2.size())
        val foreignRowIds = asCastedList[JsonObject](linkCellTable1).get.map(_.getLong("id"))
        assertEquals(List(1, 3), foreignRowIds)
      }
    }
  }

  @Test
  def putLinkWithDeleteCascadeShouldNotDeleteForeignRowsIfTheyAreAlsoInNewLinkList_reverseOrder(
      implicit c: TestContext
  ): Unit = {
    okTest {
      for {
        // Given: current links: [1, 2, 3] --> after test: [3, 2, 1]
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        _ <- sendRequest("POST", s"/tables/$tableId2/rows")
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/2/rows/3", Json.obj("value" -> 3))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/1/rows/3", Json.obj("value" -> s"table${tableId2}row3"))

        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 2, 3))
        )

        // When
        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(3, 2, 1))
        )

        // Then
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
        linkedCellTable1 <- sendRequest("GET", s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1")
          .map(_.getJsonArray("value"))
      } yield {
        assertEquals(3, rowsTable2.size())
        val foreignRowIds = asCastedList[JsonObject](linkedCellTable1).get.map(_.getLong("id"))
        assertEquals(List(3, 2, 1), foreignRowIds)
      }
    }
  }

  @Test
  def putLinkWithDeleteCascadeShouldNotDeleteForeignRowsIfTheyAreAlsoInNewLinkList_deleteOneRowAddAnother(
      implicit c: TestContext
  ): Unit = {
    okTest {
      for {
        // Given: current links: [1, 2, 3] ->  after test: [2, 4]
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        _ <- sendRequest("POST", s"/tables/$tableId2/rows")
        _ <- sendRequest("POST", s"/tables/$tableId2/rows")
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/2/rows/3", Json.obj("value" -> 3))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/1/rows/3", Json.obj("value" -> s"table${tableId2}row3"))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/2/rows/4", Json.obj("value" -> 4))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/1/rows/4", Json.obj("value" -> s"table${tableId2}row4"))

        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(1, 2, 3))
        )

        // When
        _ <- sendRequest(
          "PUT",
          s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
          Json.obj("value" -> Json.arr(2, 4))
        )

        // Then
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
        linkedCellTable1 <- sendRequest("GET", s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1")
          .map(_.getJsonArray("value"))
      } yield {
        assertEquals(2, rowsTable2.size())
        val foreignRowIds = asCastedList[JsonObject](linkedCellTable1).get.map(_.getLong("id"))
        assertEquals(List(2, 4), foreignRowIds)
      }
    }
  }

  @Test
  def clearingLinkCellWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        // clear cell
        _ <- sendRequest("DELETE", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteLinkWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        _ <- sendRequest(
          "PUT",
          s"/tables/1/columns/$linkColumnId/rows/1",
          Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))
        )

        // delete link from link cell with delete cascade
        _ <- sendRequest("DELETE", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId/link/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(1, rowsTable2.size())

        // row 2 should still be there
        assertEquals(2, rowsTable2.getJsonObject(0).getLong("id").longValue())
      }
    }
  }

  @Test
  @Ignore
  def deleteRowWithDeleteCascadeCycle(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        table2LinkColumnId <- createDeleteCascadeLinkColumn(tableId2, tableId1, "deleteCascade2")

        _ <-
          sendRequest("PUT", s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1", Json.obj("value" -> Json.arr(1)))

        _ <-
          sendRequest("PUT", s"/tables/$tableId2/columns/$table2LinkColumnId/rows/1", Json.obj("value" -> Json.arr(1)))

        // This will currently end up in a endless loop
        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)

      } yield {
        assertEquals(1, rowsTable1.size())
        assertEquals(1, rowsTable2.size())
      }
    }
  }

  private def createDeleteCascadeLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(DefaultCardinality, deleteCascade = true))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkCardinalityTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithCardinality(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("cardinality", tableId2, Constraint(Cardinality(5, 8), deleteCascade = false))
        )

        // create bi-directional link column
        createdLinkColumnWithCardinalityTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveLinkColumnWithCardinalityTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("cardinality"))

        // retrieve bi-directional backlink from table 2
        retrieveLinkColumnWithCardinalityTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertJSONEquals(
          Constraint(Cardinality(5, 8), deleteCascade = false).getJson,
          createdLinkColumnWithCardinalityTable1.getJsonObject("constraint")
        )

        assertJSONEquals(
          Constraint(Cardinality(5, 8), deleteCascade = false).getJson,
          retrieveLinkColumnWithCardinalityTable1.getJsonObject("constraint")
        )

        assertJSONEquals(
          Constraint(Cardinality(8, 5), deleteCascade = false).getJson,
          retrieveLinkColumnWithCardinalityTable2.getJsonObject("constraint")
        )
      }
    }
  }

  @Test
  def insertTwoRowsWhichPointToSameForeignRowsShouldFail(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        result <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId")
      } yield {
        assertJSONEquals(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), result.getJsonArray("value"))
      }
    }
  }

  @Test
  def duplicateRowWhichShouldFailBecauseOfCardinality(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows/$rowId/duplicate", Json.emptyObj())
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        rows <- sendRequest("GET", s"/tables/$tableId1/rows")

        result <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId")
      } yield {
        assertEquals(3, rows.getJsonObject("page").getInteger("totalSize"))
        assertJSONEquals(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), result.getJsonArray("value"))
      }
    }
  }

  @Test
  def patchTwoRowsToPointToSameForeignRowsShouldFail(implicit c: TestContext): Unit = {
    import scala.collection.JavaConverters._

    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId1 <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        linkColumnId2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(_.asScala.map(_.asInstanceOf[JsonObject]))
          .map(_.find(_.getString("name") == "table1").get)
          .map(_.getLong("id"))

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))
        rowId2 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId1",
          Json.obj("value" -> Json.arr(1, 2))
        )

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId2",
          Json.obj("value" -> Json.arr(1, 2))
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        rowId3 <- sendRequest("POST", s"/tables/$tableId2/rows")
          .map(_.getLong("id"))

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId3",
          Json.obj("value" -> Json.arr(rowId1, rowId2))
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        result <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId1")
      } yield {
        assertJSONEquals(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), result.getJsonArray("value"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCell(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 1)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertEquals(0, resultCell.getJsonArray("value").size())

        assertEquals(2, resultForeignRows.getJsonObject("page").getLong("totalSize").longValue())
        assertJSONEquals(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), resultForeignRows.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCellWhichDidNotYetHitTheLimit(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1",
          Json.obj("value" -> Json.arr(2))
        )

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertJSONEquals(Json.arr(Json.obj("id" -> 2)), resultCell.getJsonArray("value"))

        assertEquals(1, resultForeignRows.getJsonObject("page").getLong("totalSize").longValue())
        assertJSONEquals(Json.arr(Json.obj("id" -> 1)), resultForeignRows.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCellWhichAlreadyHitTheLimit(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1",
          Json.obj("value" -> Json.arr(1, 2))
        )

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertJSONEquals(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), resultCell.getJsonArray("value"))

        assertEquals(0, resultForeignRows.getJsonObject("page").getLong("totalSize").longValue())
        assertEquals(0, resultForeignRows.getJsonArray("rows").size())
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCellWhichAlreadyHitTheLimitButAddedAnotherLinkColumnBefore(implicit
  c: TestContext): Unit = {
    okTest {
      for {
        // create additional table and links so that system_link_table -> link_id and link_table -> ids are not identical
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)
        tableId3 <- createDefaultTable(name = "table3", tableNum = 3)

        linkColumnId1 <- createCardinalityLinkColumn(tableId2, tableId1, "cardinality1", 0, 0)
        linkColumnId2 <- createCardinalityLinkColumn(tableId3, tableId2, "cardinality2", 1, 1)

        rowId1 <- sendRequest("POST", s"/tables/$tableId2/rows").map(_.getLong("id"))
        rowId2 <- sendRequest("POST", s"/tables/$tableId3/rows").map(_.getLong("id"))

        // create additional links before
        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId2/columns/$linkColumnId1/rows/$rowId1",
          Json.obj("value" -> Json.arr(1, 2))
        )
        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId3/columns/$linkColumnId2/rows/$rowId2",
          Json.obj("value" -> Json.arr(1))
        )

        resultForeignRows1 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId1/rows/$rowId1/foreignRows")
        resultForeignRowsTotalSize1 = resultForeignRows1.getJsonObject("page").getLong("totalSize").longValue()

        resultForeignRows2 <- sendRequest("GET", s"/tables/$tableId3/columns/$linkColumnId2/rows/$rowId2/foreignRows")
        resultForeignRowsTotalSize2 = resultForeignRows2.getJsonObject("page").getLong("totalSize").longValue()
      } yield {
        assertEquals("Size must be 2 because of no limit", 2, resultForeignRowsTotalSize1)
        assertEquals("Size must be 2 because of no limit", 2, resultForeignRows1.getJsonArray("rows").size())

        assertEquals("Size must be 0 because the limit is hit", 0, resultForeignRowsTotalSize2)
        assertEquals("Size must be 0 because the limit is hit", 0, resultForeignRows2.getJsonArray("rows").size())
      }
    }
  }

  @Test
  def patchAndRetrieveWithHigherCardinality(implicit c: TestContext): Unit = {
    import scala.collection.JavaConverters._

    okTest {
      for {
        tableId1 <- createEmptyDefaultTable(name = "table1")
        tableId2 <- createEmptyDefaultTable(name = "table2", tableNum = 2)

        linkColumnId1 <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 2, 3)

        columns1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
        columns2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))

        linkColumnId2 = columns2.asScala
          .map(_.asInstanceOf[JsonObject])
          .find(_.getString("name") == "table1")
          .get
          .getLong("id")
          .longValue()

        rowId11 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row11")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId12 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row12")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        rowId21 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row21")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId22 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row22")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId23 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row23")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId24 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row24")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        // let's do some real tests
        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11",
          Json.obj("value" -> Json.arr(rowId22, rowId21, rowId23))
        )

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23",
          Json.obj("value" -> Json.arr(rowId12))
        )

        resultCell11 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11")

        resultForeignRows11 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11/foreignRows")

        resultCell21 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId21")
        resultCell22 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId22")
        resultCell23 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23")

        resultForeignRows21 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId21/foreignRows")
        resultForeignRows23 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23/foreignRows")
        resultForeignRows12 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId12/foreignRows")

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11",
          Json.obj("value" -> Json.arr(rowId24))
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        rowId13 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row13")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23",
          Json.obj("value" -> Json.arr(rowId13))
        )
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })
      } yield {
        assertJSONEquals(
          Json.arr(
            Json.obj("id" -> rowId22),
            Json.obj("id" -> rowId21),
            Json.obj("id" -> rowId23)
          ),
          resultCell11.getJsonArray("value")
        )

        // cell 11 is already at it's limit
        assertEquals(0, resultForeignRows11.getJsonObject("page").getLong("totalSize").longValue())
        assertEquals(0, resultForeignRows11.getJsonArray("rows").size())

        assertJSONEquals(Json.arr(Json.obj("id" -> rowId11)), resultCell21.getJsonArray("value"))
        assertJSONEquals(Json.arr(Json.obj("id" -> rowId11)), resultCell22.getJsonArray("value"))
        assertJSONEquals(
          Json.arr(Json.obj("id" -> rowId11), Json.obj("id" -> rowId12)),
          resultCell23.getJsonArray("value")
        )

        assertEquals(1, resultForeignRows21.getJsonObject("page").getLong("totalSize").longValue())
        assertJSONEquals(Json.arr(Json.obj("id" -> rowId12)), resultForeignRows21.getJsonArray("rows"))

        // cell 23 is already at it's limit
        assertEquals(0, resultForeignRows23.getJsonObject("page").getLong("totalSize").longValue())
        assertEquals(0, resultForeignRows23.getJsonArray("rows").size())

        assertEquals(3, resultForeignRows12.getJsonObject("page").getLong("totalSize").longValue())
        assertJSONEquals(
          Json.arr(
            Json.obj("id" -> rowId21),
            Json.obj("id" -> rowId22),
            Json.obj("id" -> rowId24)
          ),
          resultForeignRows12.getJsonArray("rows")
        )
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkDeleteReplaceRowIdTest extends LinkTestBase with Helper {

  @Test
  def deleteRow_withReplacingRowId_updateForeignLinks(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        (tableId1, tableId2) <- createDefaultLinkTables()

        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=3")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(4, rowsTable1.size())
        assertEquals(3, rowsTable2.size())

        val expected = Json.arr(
          Json.obj("id" -> 2, "value" -> "table2row2"),
          Json.obj("id" -> 3, "value" -> "table2row3") // link must be moved from row 1 to 3
        )

        assertJSONEquals(expected, rowsTable1.getJsonObject(2).getJsonArray("values").getJsonArray(2))
        assertJSONEquals(expected, rowsTable1.getJsonObject(3).getJsonArray("values").getJsonArray(2))
      }
    }
  }

  @Test
  def deleteRow_withReplacingRowId_updateForeignLinksWithCardinalityOne(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        (tableId1, tableId2) <- createLinkTablesWithCardinality()

        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=3")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)

        replacedRowIds <-
          dbConnection.query("SELECT replaced_ids FROM user_table_2 WHERE id = 3")
            .map(
              _.getJsonArray("results").getJsonArray(0).getString(0)
            )
      } yield {
        assertEquals(4, rowsTable1.size())
        assertEquals(3, rowsTable2.size())

        val expected = Json.arr(
          Json.obj("id" -> 3, "value" -> "table2row3") // link must be moved from row 1 to 3
        )

        assertJSONEquals(expected, rowsTable1.getJsonObject(2).getJsonArray("values").getJsonArray(2))
        assertJSONEquals(expected, rowsTable1.getJsonObject(3).getJsonArray("values").getJsonArray(2))
        assertEquals("[1]", replacedRowIds)
      }
    }
  }

  @Test
  def deleteRow_withReplacingRowIdAlreadyLinked_shouldSkipDuplicateInsert(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        (tableId1, tableId2) <- createDefaultLinkTables()

        // row 2 is already linked to row 1
        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=2")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)
      } yield {
        assertEquals(4, rowsTable1.size())
        assertEquals(3, rowsTable2.size())

        val expected = Json.arr(Json.obj("id" -> 2, "value" -> "table2row2")) // contains row 2 only once

        assertJSONEquals(expected, rowsTable1.getJsonObject(2).getJsonArray("values").getJsonArray(2))
        assertJSONEquals(expected, rowsTable1.getJsonObject(3).getJsonArray("values").getJsonArray(2))
      }
    }
  }

  @Test
  def deleteRow_withReplacingRowId_mergesAllReplacedRowIds(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
      for {
        (tableId1, tableId2) <- createDefaultLinkTables()

        // pretending that the row ids have been replaced several times
        _ <-
          dbConnection.query(
            "UPDATE user_table_2 set replaced_ids = ?::jsonb WHERE id = ?",
            Json.arr("[11,22]", 3)
          )
        _ <-
          dbConnection.query(
            "UPDATE user_table_2 set replaced_ids = ?::jsonb WHERE id = ?",
            Json.arr("[33,44]", 1)
          )

        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=3")
        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray)

        replacedRowIds <-
          dbConnection.query("SELECT replaced_ids FROM user_table_2 WHERE id = ?", Json.arr(3))
            .map(
              _.getJsonArray("results").getJsonArray(0).getString(0)
            )
      } yield {
        assertEquals(4, rowsTable1.size())
        assertEquals(3, rowsTable2.size())
        assertEquals("[11, 22, 33, 44, 1]", replacedRowIds)
      }
    }
  }

  @Test
  def deleteRow_withUnknownValidReplacingRowId_throwsException(implicit c: TestContext): Unit = {
    exceptionTest("unprocessable.entity") {
      for {
        (tableId1, tableId2) <- createDefaultLinkTables()

        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=9999")
      } yield ()
    }
  }

  @Test
  def deleteRow_withInvalidReplacingRowId_throwsException(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId1, tableId2) <- createDefaultLinkTables()

        deleteResult <- sendRequest("DELETE", s"/tables/$tableId2/rows/1?replacingRowId=invalidId") recover {
          case TestCustomException(message, id, statusCode) =>
            (message, id, statusCode)
        }
      } yield {
        assertEquals(
          (
            "com.campudus.tableaux.UnprocessableEntityException: Invalid replacing row id: invalidId",
            "unprocessable.entity",
            422
          ),
          deleteResult
        )
      }
    }
  }

  // format: off
  /**
    * Creates two tables with four rows each and link two rows two times.
    *
    * |----------------------------|           |----------------------------|
    * |           table1           |           |           table2           |
    * |----------------------------|           |----------------------------|
    * | id | Test Column 1 | links |           | id | Test Column 1 | links |
    * |----|---------------|-------|  * ==> *  |----|---------------|-------|
    * | 1  | table1row1    |       |           | 1  | table2row1    | 3,4   |
    * | 2  | table1row2    |       |           | 2  | table2row2    | 3,4   |
    * | 3  | table1row3    | 1,2   |           | 3  | table2row3    |       |
    * | 4  | table1row4    | 1,2   |           | 4  | table2row4    |       |
    */
  private def createDefaultLinkTables(): Future[(TableId, TableId)] = {
  // format: on
    for {
      tableId1 <- createDefaultTable(name = "table1")
      tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/columns",
        Columns(
          LinkBiDirectionalCol("links", tableId2, Constraint(DefaultCardinality, deleteCascade = false))
        )
      )

      columns1 <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))
      columns2 <- sendRequest("GET", s"/tables/$tableId2/columns").map(_.getJsonArray("columns"))

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId2/rows",
        Rows(columns2, Json.obj("Test Column 1" -> "table2row3"))
      )
      _ <- sendRequest(
        "POST",
        s"/tables/$tableId2/rows",
        Rows(columns2, Json.obj("Test Column 1" -> "table2row4"))
      )

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/rows",
        Rows(
          columns1,
          Json.obj("Test Column 1" -> "table1row3", "links" -> Json.arr(1, 2))
        )
      )
      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/rows",
        Rows(
          columns1,
          Json.obj("Test Column 1" -> "table1row4", "links" -> Json.arr(1, 2))
        )
      )
    } yield (tableId1, tableId2)
  }

  // format: off
  /**
    * Creates two tables with four rows each and link two rows.
    *
    * |----------------------------|           |----------------------------|
    * |           table1           |           |           table2           |
    * |----------------------------|           |----------------------------|
    * | id | Test Column 1 | links |           | id | Test Column 1 | links |
    * |----|---------------|-------|  * ==> 1  |----|---------------|-------|
    * | 1  | table1row1    |       |           | 1  | table2row1    | 3,4   |
    * | 2  | table1row2    |       |           | 2  | table2row2    |       |
    * | 3  | table1row3    | 1     |           | 3  | table2row3    |       |
    * | 4  | table1row4    | 1     |           | 4  | table2row4    |       |
    */
  private def createLinkTablesWithCardinality(): Future[(TableId, TableId)] = {
    // format: on
    for {
      tableId1 <- createDefaultTable(name = "table1")
      tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/columns",
        Columns(
          LinkBiDirectionalCol("links", tableId2, Constraint(Cardinality(0, 1), deleteCascade = false))
        )
      )

      columns1 <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))
      columns2 <- sendRequest("GET", s"/tables/$tableId2/columns").map(_.getJsonArray("columns"))

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId2/rows",
        Rows(columns2, Json.obj("Test Column 1" -> "table2row3"))
      )
      _ <- sendRequest(
        "POST",
        s"/tables/$tableId2/rows",
        Rows(columns2, Json.obj("Test Column 1" -> "table2row4"))
      )

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/rows",
        Rows(
          columns1,
          Json.obj("Test Column 1" -> "table1row3", "links" -> Json.arr(1))
        )
      )

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId1/rows",
        Rows(
          columns1,
          Json.obj("Test Column 1" -> "table1row4", "links" -> Json.arr(1))
        )
      )
    } yield (tableId1, tableId2)
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkArchiveCascadeTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithArchiveCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("archiveCascade", tableId2, Constraint(DefaultCardinality, archiveCascade = true))
        )

        // create bi-directional link column
        createdArchiveCascadeLinkColumnTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveArchiveCascadeLinkColumnTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("archiveCascade"))

        // retrieve bi-directional backlink from table 2
        retrieveArchiveCascadeLinkColumnTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertJSONEquals(
          Constraint(DefaultCardinality, archiveCascade = true).getJson,
          createdArchiveCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertJSONEquals(
          Constraint(DefaultCardinality, archiveCascade = true).getJson,
          retrieveArchiveCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertNull(retrieveArchiveCascadeLinkColumnTable2.getJsonObject("constraint"))
      }
    }
  }

  @Test
  def archiveRowWithArchiveCascadeShouldArchiveForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createArchiveCascadeLinkColumn(tableId1, tableId2, "archiveCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("archiveCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        // archive parent row
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/$rowId/annotations", Json.obj("archived" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterArchivedRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterArchivedRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        assertEquals(1, rowsTable1.size)
        assertEquals(2, rowsTable2.size)
        assertEquals(1, historyRowsTable1.size)
        assertEquals(2, historyRowsTable2.size)
      }
    }
  }

  @Test
  def archiveForeignRowsWithArchiveCascadeShouldNotArchiveParentRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createArchiveCascadeLinkColumn(tableId1, tableId2, "archiveCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("archiveCascade" -> Json.arr(1, 2))))

        // archive foreign rows
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/1/annotations", Json.obj("archived" -> true))
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/2/annotations", Json.obj("archived" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterArchivedRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterArchivedRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        // parent row should still be unarchived
        assertEquals(2, rowsTable1.size)
        assertEquals(0, rowsTable2.size)
        assertEquals(2, historyRowsTable1.size)
        assertEquals(0, historyRowsTable2.size)
      }
    }
  }

  @Test
  def archiveTableWithArchiveCascadeShouldArchiveAllRowsAndForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createArchiveCascadeLinkColumn(tableId1, tableId2, "archiveCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("archiveCascade" -> Json.arr(2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        // archive whole table
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/annotations", Json.obj("archived" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterArchivedRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterArchivedRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        assertEquals(3, rowsTable1.size)
        assertEquals(1, rowsTable2.size)
        assertEquals(3, historyRowsTable1.size)
        assertEquals(1, historyRowsTable2.size)
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkFinalCascadeTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithFinalCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("finalCascade", tableId2, Constraint(DefaultCardinality, finalCascade = true))
        )

        // create bi-directional link column
        createdFinalCascadeLinkColumnTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveFinalCascadeLinkColumnTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("finalCascade"))

        // retrieve bi-directional backlink from table 2
        retrieveFinalCascadeLinkColumnTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertJSONEquals(
          Constraint(DefaultCardinality, finalCascade = true).getJson,
          createdFinalCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertJSONEquals(
          Constraint(DefaultCardinality, finalCascade = true).getJson,
          retrieveFinalCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertNull(retrieveFinalCascadeLinkColumnTable2.getJsonObject("constraint"))
      }
    }
  }

  @Test
  def finalizeRowWithFinalCascadeShouldFinalizeForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createFinalCascadeLinkColumn(tableId1, tableId2, "finalCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("finalCascade" -> Json.arr(1, 2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        // finalize parent row
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/$rowId/annotations", Json.obj("final" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterFinalRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterFinalRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        assertEquals(1, rowsTable1.size)
        assertEquals(2, rowsTable2.size)
        assertEquals(1, historyRowsTable1.size)
        assertEquals(2, historyRowsTable2.size)
      }
    }
  }

  @Test
  def finalizeForeignRowsWithFinalCascadeShouldNotFinalizeParentRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createFinalCascadeLinkColumn(tableId1, tableId2, "finalCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("finalCascade" -> Json.arr(1, 2))))

        // finalize foreign rows
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/1/annotations", Json.obj("final" -> true))
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/2/annotations", Json.obj("final" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterFinalRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterFinalRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        // parent row should still NOT be finalized
        assertEquals(2, rowsTable1.size)
        assertEquals(0, rowsTable2.size)
        assertEquals(2, historyRowsTable1.size)
        assertEquals(0, historyRowsTable2.size)
      }
    }
  }

  @Test
  def finalizeTableWithFinalCascadeShouldFinalizeAllRowsAndForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createFinalCascadeLinkColumn(tableId1, tableId2, "finalCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        rowId <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("finalCascade" -> Json.arr(2))))
            .map(toRowsArray)
            .map(_.getJsonObject(0))
            .map(_.getLong("id"))

        // finalize whole table
        _ <- sendRequest("PATCH", s"/tables/$tableId1/rows/annotations", Json.obj("final" -> true))

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(toRowsArray).map(filterFinalRows)
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(toRowsArray).map(filterFinalRows)

        historyRowsTable1 <- sendRequest("GET", s"/tables/$tableId1/history?historyType=row_flag").map(toRowsArray)
        historyRowsTable2 <- sendRequest("GET", s"/tables/$tableId2/history?historyType=row_flag").map(toRowsArray)
      } yield {
        assertEquals(3, rowsTable1.size)
        assertEquals(1, rowsTable2.size)
        assertEquals(3, historyRowsTable1.size)
        assertEquals(1, historyRowsTable2.size)
      }
    }
  }

  def filterFinalRows(jsonArray: JsonArray): Seq[JsonObject] = {
    import scala.collection.JavaConverters._

    jsonArray.asScala
      .collect({
        case obj: JsonObject => obj
      }).toSeq
      .filter(_.getBoolean("final"))
  }

  private def createFinalCascadeLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(DefaultCardinality, finalCascade = true))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveFinalAndArchivedRows extends LinkTestBase with Helper {

  @Test
  def retrieveMixedFinalAndArchivedRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")

        // | rowid | final | archived |
        // | ----- | ----- | -------- |
        // | 1     | false | false    |
        // | 2     | true  | false    |
        // | 3     | false | true     |
        // | 4     | true  | true     |
        // | 5     | true  | false    |
        // | 6     | true  | false    |
        // | 7     | true  | true     |

        _ <- sendRequest("POST", s"/tables/1/rows")
        _ <- sendRequest("POST", s"/tables/1/rows")
        _ <- sendRequest("POST", s"/tables/1/rows")
        _ <- sendRequest("POST", s"/tables/1/rows")
        _ <- sendRequest("POST", s"/tables/1/rows")
        _ <- sendRequest("PATCH", s"/tables/1/rows/2/annotations", Json.obj("final" -> true))
        _ <- sendRequest("PATCH", s"/tables/1/rows/3/annotations", Json.obj("archived" -> true))
        _ <- sendRequest("PATCH", s"/tables/1/rows/4/annotations", Json.obj("final" -> true, "archived" -> true))
        _ <- sendRequest("PATCH", s"/tables/1/rows/5/annotations", Json.obj("final" -> true, "archived" -> false))
        _ <- sendRequest("PATCH", s"/tables/1/rows/6/annotations", Json.obj("final" -> true, "archived" -> false))
        _ <- sendRequest("PATCH", s"/tables/1/rows/7/annotations", Json.obj("final" -> true, "archived" -> true))

        finalFalse <- sendRequest("GET", s"/tables/1/rows?final=false").map(toRowsArray)
        finalTrue <- sendRequest("GET", s"/tables/1/rows?final=true").map(toRowsArray)
        archivedFalse <- sendRequest("GET", s"/tables/1/rows?archived=false").map(toRowsArray)
        archivedTrue <- sendRequest("GET", s"/tables/1/rows?archived=true").map(toRowsArray)
        allRows <- sendRequest("GET", s"/tables/1/rows").map(toRowsArray)
        finalFalseAndArchivedFalse <- sendRequest("GET", s"/tables/1/rows?final=false&archived=false").map(toRowsArray)
        finalTrueAndArchivedTrue <- sendRequest("GET", s"/tables/1/rows?final=true&archived=true").map(toRowsArray)
        finalFalseAndArchivedTrue <- sendRequest("GET", s"/tables/1/rows?final=false&archived=true").map(toRowsArray)
        finalTrueAndArchivedFalse <- sendRequest("GET", s"/tables/1/rows?final=true&archived=false").map(toRowsArray)
      } yield {
        assertEquals(2, finalFalse.size())
        assertEquals(5, finalTrue.size())
        assertEquals(4, archivedFalse.size())
        assertEquals(3, archivedTrue.size())

        assertEquals(7, allRows.size())
        assertEquals(1, finalFalseAndArchivedFalse.size())
        assertEquals(2, finalTrueAndArchivedTrue.size())
        assertEquals(1, finalFalseAndArchivedTrue.size())
        assertEquals(3, finalTrueAndArchivedFalse.size())
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCell_finalAndArchivedRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinalityLinkColumn(tableId1, tableId2, "cardinality", 1, 1)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows").map(_.getLong("id"))
        _ <- sendRequest("PATCH", s"/tables/$tableId2/rows/1/annotations", Json.obj("final" -> true))
        _ <- sendRequest("PATCH", s"/tables/$tableId2/rows/2/annotations", Json.obj("archived" -> true))

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRowsFinal <-
          sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows?final=true")
        resultForeignRowsArchived <-
          sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows?archived=true")

        rrr <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")
      } yield {
        println(s"rrr: $rrr")

        assertEquals(0, resultCell.getJsonArray("value").size())

        assertEquals(2, resultForeignRowsFinal.getJsonObject("page").getLong("totalSize").longValue())
        assertEquals(2, resultForeignRowsArchived.getJsonObject("page").getLong("totalSize").longValue())
        assertJSONEquals(Json.arr(Json.obj("id" -> 1)), resultForeignRowsFinal.getJsonArray("rows"))
        assertJSONEquals(Json.arr(Json.obj("id" -> 2)), resultForeignRowsArchived.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveSingleArchivedRowAndCell(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        _ <- sendRequest("PATCH", s"/tables/1/rows/1/annotations", Json.obj("archived" -> true))

        row <- sendRequest("GET", s"/tables/1/rows/1").map(_.getBoolean("archived"))
        cell <- sendRequest("GET", s"/tables/1/columns/1/rows/1").map(_.getBoolean("archived"))
      } yield {
        assertEquals(row, true)
        assertEquals(cell, true)
      }
    }
  }

  @Test
  def retrieveSingleArchivedFirstCellsAndLinkCell(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createArchiveCascadeLinkColumn(tableId1, tableId2, "cardinality")
        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        rowId1 <-
          sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
            .map(toRowsArray).map(_.getJsonObject(0)).map(_.getLong("id"))
        _ <- sendRequest("PATCH", s"/tables/$tableId2/rows/1/annotations", Json.obj("final" -> true))
        _ <- sendRequest("PATCH", s"/tables/$tableId2/rows/2/annotations", Json.obj("archived" -> true))

        firstCells <- sendRequest("GET", s"/tables/$tableId2/columns/first/rows").map(_.getJsonArray("rows"))
        linkCell <-
          sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1").map(_.getJsonArray("value"))
        row <- sendRequest("GET", s"/tables/$tableId1/rows/$rowId1")
      } yield {
        assertJSONEquals(
          Json.fromObjectString("""|{
                                   |  "id": 1,
                                   |  "values": [
                                   |    "table2row1"
                                   |  ],
                                   |  "final": true
                                   |}
                                   |""".stripMargin),
          firstCells.getJsonObject(0)
        )

        assertJSONEquals(
          Json.fromArrayString(
            """|[
               |  {
               |    "id": 1,
               |    "value": "table2row1",
               |    "final": true
               |  },
               |  {
               |    "id": 2,
               |    "value": "table2row2",
               |    "archived": true
               |  }
               |]
               |""".stripMargin
          ),
          linkCell
        )
      }
    }
  }
}

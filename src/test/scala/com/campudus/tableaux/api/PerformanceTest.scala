package com.campudus.tableaux.api

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.runner.RunWith
import org.junit.{Ignore, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
@Ignore
class PerformanceTest extends TableauxTestBase {

  private def createTableWithMultilanguageColumnsAndConcatColumn(
      tableName: String
  ): Future[(TableId, Seq[ColumnId])] = {
    val createMultilanguageColumn = Json.obj(
      "columns" ->
        Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1", "languageType" -> "language", "identifier" -> true),
          Json.obj("kind" -> "boolean", "name" -> "Test Column 2", "languageType" -> "language"),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 3", "languageType" -> "language"),
          Json.obj("kind" -> "richtext", "name" -> "Test Column 4", "languageType" -> "language"),
          Json.obj("kind" -> "shorttext", "name" -> "Test Column 5", "languageType" -> "language"),
          Json.obj("kind" -> "date", "name" -> "Test Column 6", "languageType" -> "language"),
          Json.obj("kind" -> "datetime", "name" -> "Test Column 7", "languageType" -> "language")
        )
    )

    import scala.collection.JavaConverters._
    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> tableName)) map (_.getLong("id"))
      columns <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn)
      columnIds = columns.getJsonArray("columns").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
    } yield {
      (tableId.toLong, columnIds)
    }
  }

  private def createPerformanceTestData(rows: Int) = {

    def createLinkColumns(linkTo: TableId) = {
      Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "link",
            "name" -> "Test Column 8",
            "toTable" -> linkTo,
            "toName" -> "Test Column 8"
          ),
          Json.obj(
            "kind" -> "link",
            "name" -> "Test Column 9",
            "toTable" -> linkTo,
            "toName" -> "Test Column 9"
          ),
          Json.obj(
            "kind" -> "link",
            "name" -> "Test Column 10",
            "toTable" -> linkTo,
            "toName" -> "Test Column 10"
          )
        )
      )
    }

    def valuesRow = {
      Json.obj(
        "columns" -> Json.arr(
          Json.obj("id" -> 1),
          Json.obj("id" -> 2),
          Json.obj("id" -> 3),
          Json.obj("id" -> 4),
          Json.obj("id" -> 5),
          Json.obj("id" -> 6),
          Json.obj("id" -> 7)
        ),
        "rows" -> Json.arr(
          Json.obj(
            "values" ->
              Json.arr(
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> true,
                  "en-GB" -> false
                ),
                Json.obj(
                  "de-DE" -> 1.234,
                  "en-GB" -> 4.321
                ),
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> "2015-01-01",
                  "en-GB" -> "2016-01-01"
                ),
                Json.obj(
                  "de-DE" -> "2015-01-01T13:37:47.110Z",
                  "en-GB" -> "2016-01-01T13:37:47.110Z"
                )
              )
          )
        )
      )
    }

    def valuesRowWithLink(linkColumnId1: Long, linkColumnId2: Long, linkColumnId3: Long, linkRowId: RowId) = {
      Json.obj(
        "columns" -> Json.arr(
          Json.obj("id" -> 1),
          Json.obj("id" -> 2),
          Json.obj("id" -> 3),
          Json.obj("id" -> 4),
          Json.obj("id" -> 5),
          Json.obj("id" -> 6),
          Json.obj("id" -> 7),
          Json.obj("id" -> linkColumnId1),
          Json.obj("id" -> linkColumnId2),
          Json.obj("id" -> linkColumnId3)
        ),
        "rows" -> Json.arr(
          Json.obj(
            "values" ->
              Json.arr(
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> true,
                  "en-GB" -> false
                ),
                Json.obj(
                  "de-DE" -> 1.234,
                  "en-GB" -> 4.321
                ),
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> "Hallo, Welt!",
                  "en-GB" -> "Hello, World!"
                ),
                Json.obj(
                  "de-DE" -> "2015-01-01",
                  "en-GB" -> "2016-01-01"
                ),
                Json.obj(
                  "de-DE" -> "2015-01-01T13:37:47.110Z",
                  "en-GB" -> "2016-01-01T13:37:47.110Z"
                ),
                Json.obj(
                  "values" -> Json.arr(linkRowId)
                ),
                Json.obj(
                  "values" -> Json.arr(linkRowId)
                ),
                Json.obj(
                  "values" -> Json.arr(linkRowId)
                )
              )
          )
        )
      )
    }

    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val system = SystemModel(dbConnection)

    for {
      // reset for warmup
      _ <- system.uninstall()
      _ <- system.install()

      table1 <- createTableWithMultilanguageColumnsAndConcatColumn("Table 1")
      table2 <- createTableWithMultilanguageColumnsAndConcatColumn("Table 2")

      linkColumns <- sendRequest("POST", s"/tables/${table1._1}/columns", createLinkColumns(table2._1))
      (linkColumnId1, linkColumnId2, linkColumnId3) = linkColumns
        .getJsonArray("columns")
        .asScala
        .toList
        .map({
          case t: JsonObject =>
            t.getLong("id").toLong
        }) match {
        case List(id1, id2, id3) =>
          (id1, id2, id3)
      }

      _ = logger.info(s"Start writing rows data...")

      // Split range in groups & do as many request in parallel as big each group is

      // Fill table 2
      startFillTable2 = System.currentTimeMillis()
      _ <- Range(1, rows).grouped(100).foldLeft(Future.successful(())) {
        case (future, group) =>
          future.flatMap({ _ =>
            {
              Future
                .sequence(group.map({ _ =>
                  {
                    sendStringRequest("POST", s"/tables/${table2._1}/rows", valuesRow)
                  }
                }))
                .map({ _ =>
                  logger.info(s"Finished table 2 group ${group.head}")
                  ()
                })
            }
          })
      }

      elapsedTimesWritingRowsTable2 = System.currentTimeMillis() - startFillTable2
      _ = logger.info(s"elapsedTimesWritingRowsTable2: $elapsedTimesWritingRowsTable2")

      // Fill table 1
      startFillTable1 = System.currentTimeMillis()
      _ <- Range(1, rows).grouped(100).foldLeft(Future.successful(())) {
        case (future, group) =>
          future.flatMap({ _ =>
            {
              Future
                .sequence(group.map({ rowId: Int =>
                  {
                    sendStringRequest(
                      "POST",
                      s"/tables/${table1._1}/rows",
                      valuesRowWithLink(linkColumnId1, linkColumnId2, linkColumnId3, rowId)
                    )
                  }
                }))
                .map({ _ =>
                  {
                    logger.info(s"Finished table 1 group ${group.head}")
                    ()
                  }
                })
            }
          })
      }
      elapsedTimesWritingRowsTable1 = System.currentTimeMillis() - startFillTable1
      _ = logger.info(s"elapsedTimesWritingRowsTable1: $elapsedTimesWritingRowsTable1")

    } yield (elapsedTimesWritingRowsTable1, elapsedTimesWritingRowsTable2)
  }

  private def sendTimedRequest(method: String, path: String): Future[Long] = {
    val startTime = System.currentTimeMillis()
    sendStringRequest(method, path)
      .map(_ => System.currentTimeMillis - startTime)
  }

  private def doPerformanceTest(times: Int, rows: Int): Future[Seq[Long]] = {
    val warmUp = 10

    for {
      // 3 warmup iterations for writing
      _ <- Range(1, 3).foldLeft(Future.successful(()))({
        case (resultsFuture, _) =>
          resultsFuture.flatMap({ _ =>
            {
              for {
                _ <- createPerformanceTestData(rows)
              } yield ()
            }
          })
      })

      // measure writing times
      (elapsedTimesWritingRowsTable1, elapsedTimesWritingRowsTable2) <- createPerformanceTestData(rows)

      elapsedTimesReadingRows <- Range(1, times + warmUp).foldLeft(Future.successful(Seq.empty[Long]))({
        case (resultsFuture, _) =>
          resultsFuture.flatMap({ results => // Send request and measure round-trip time in ms
            {
              for {
                duration <- sendTimedRequest("GET", "/tables/1/rows")
              } yield results ++ Seq(duration)
            }
          })

      })
    } yield {
      // Drop measurements of warmup phase
      val withoutWarmup = elapsedTimesReadingRows.drop(warmUp)
      logger.info("\n=====")
      logger.info(s"performance tests with writing $rows rows to table 1 took $elapsedTimesWritingRowsTable1 ms")
      logger.info(s"performance tests with writing $rows rows to table 2 took $elapsedTimesWritingRowsTable2 ms")
      val average = elapsedTimesReadingRows.sum / elapsedTimesReadingRows.length
      logger.info(s"$times performance tests with reading $rows rows took $average ms in average\n=====\n")

      withoutWarmup
    }
  }

  @Test
  def testPerformanceWith100Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 100))

  @Test
  def testPerformanceWith500Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 500))

  @Test
  def testPerformanceWith1000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 1000))

  @Test
  def testPerformanceWith2000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 2000))

  @Test
  def testPerformanceWith3000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 3000))
}

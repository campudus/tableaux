package com.campudus.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.{Future, Promise}


@RunWith(classOf[VertxUnitRunner])
class PerformanceTest extends TableauxTestBase {

  private def createTableWithMultilanguageColumnsAndConcatColumn(tableName: String): Future[(TableId, Seq[ColumnId])] = {
    val createMultilanguageColumn = Json.obj(
      "columns" ->
        Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> true, "identifier" -> true),
          Json.obj("kind" -> "boolean", "name" -> "Test Column 2", "multilanguage" -> true, "identifier" -> true),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 3", "multilanguage" -> true),
          Json.obj("kind" -> "richtext", "name" -> "Test Column 4", "multilanguage" -> true),
          Json.obj("kind" -> "shorttext", "name" -> "Test Column 5", "multilanguage" -> true),
          Json.obj("kind" -> "date", "name" -> "Test Column 6", "multilanguage" -> true),
          Json.obj("kind" -> "datetime", "name" -> "Test Column 7", "multilanguage" -> true)
        )
    )

    import scala.collection.JavaConverters._
    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> tableName)) map (_.getLong("id"))
      columns <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn)
      columnIds = columns.getArray("columns").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
    } yield {
      (tableId.toLong, columnIds)
    }
  }

  private def createPerformanceTestData(rows: Int): Future[Unit] = {
    def createLinkColumn(linkTo: TableId) = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "link",
      "name" -> "column 10 (link)",
      "toTable" -> linkTo
    )))

    val valuesRow = Json.obj(
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
        Json.obj("values" ->
          Json.arr(
            Json.obj(
              "de_DE" -> "Hallo, Welt!",
              "en_US" -> "Hello, World!"
            ),
            Json.obj(
              "de_DE" -> true,
              "en_US" -> false
            ),
            Json.obj(
              "de_DE" -> 1.234,
              "en_US" -> 4.321
            ),
            Json.obj(
              "de_DE" -> "Hallo, Welt!",
              "en_US" -> "Hello, World!"
            ),
            Json.obj(
              "de_DE" -> "Hallo, Welt!",
              "en_US" -> "Hello, World!"
            ),
            Json.obj(
              "de_DE" -> "2015-01-01",
              "en_US" -> "2016-01-01"
            ),
            Json.obj(
              "de_DE" -> "2015-01-01T13:37:47.110Z",
              "en_US" -> "2016-01-01T13:37:47.110Z"
            )
          )
        )
      )
    )

    for {
      table1 <- createTableWithMultilanguageColumnsAndConcatColumn("Table 1")
      table2 <- createTableWithMultilanguageColumnsAndConcatColumn("Table 2")

      linkColumn <- sendRequest("POST", s"/tables/${table1._1}/columns", createLinkColumn(table2._1))
      linkColumnId = linkColumn.getArray("columns").getJsonObject(0).getLong("id").toLong

      table1Rows <- Range(1, rows).foldLeft(Future.successful(Seq.empty[JsonObject]))({
        case (results, row) =>
          results.flatMap({
            resultRows =>
              sendRequest("POST", s"/tables/${table1._1}/rows", valuesRow).map({
                json =>
                  resultRows ++ Seq(json)
              })
          })
      })
    } yield ()
  }

  private def doPerformanceTest(times: Int, rows: Int): Future[Seq[Long]] = {
    val warmUp = 10

    for {
      _ <- createPerformanceTestData(rows)

      elapsedTimes <- Range(1, times + warmUp).foldLeft(Future.successful(Seq.empty[Long]))({
        case (resultsFuture, x) =>
          resultsFuture.flatMap({
            results =>
              val startTime = System.currentTimeMillis()

              val p = Promise[Seq[Long]]()
              httpRequest("GET", "/tables/1/rows", {
                response =>
                  val elapsedTime = System.currentTimeMillis - startTime
                  logger.info(s"\n=====\nperformance test $x with $rows rows took $elapsedTime ms\n=====")
                  p.success(results ++ Seq(elapsedTime))
              }, {
                ex =>
                  val elapsedTime = System.currentTimeMillis - startTime
                  logger.info(s"\n=====\nperformance test $x with $rows rows took $elapsedTime ms\n=====")
                  p.failure(ex)
              })
              p.future
          })

      })

    } yield {
      val withoutWarmup = elapsedTimes.drop(warmUp)

      val average = elapsedTimes.sum / elapsedTimes.length
      logger.info(s"\n=====\n$times performance tests with $rows rows took $average ms in average\n=====")

      withoutWarmup
    }
  }

  @Test
  def testPerformanceWith10Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 10))

  @Test
  def testPerformanceWith100Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 100))

  @Test
  def testPerformanceWith500Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 500))

  @Test
  def testPerformanceWith1000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 1000))

  @Test
  def testPerformanceWith10000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 10000))

  @Test
  def testPerformanceWith100000Rows(implicit context: TestContext): Unit = okTest(doPerformanceTest(10, 100000))
}

package com.campudus.tableaux.controller

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.{Starter, TestConfig}
import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.{SQLConnection, VertxExecutionContext}
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerTest extends TestConfig with VertxExecutionContext {

  override val verticle = new Starter
  val vertx: Vertx = Vertx.vertx()

  def createTableauxController(): TableauxController = {
    val sqlConnection = SQLConnection(vertx, databaseConfig)
    val dbConnection = DatabaseConnection(sqlConnection)

    val model = TableauxModel(dbConnection)

    TableauxController(tableauxConfig, model)
  }

  @Test
  def checkCreateRowWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq())))
  }

  @Test
  def checkCreateRowWithNullSeq(implicit c: TestContext): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq()))))
  }

  @Test
  def checkCreateRowWithNullId(implicit c: TestContext): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((0: ColumnId, ""))))))
  }

  @Test
  def checkCreateRowWithNullValue(implicit c: TestContext): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((1: ColumnId, null))))))
  }

  @Test
  def checkFillCellWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.fillCell(0, 0, 0, null))
  }

  private def illegalArgumentTest[A](f: => Future[A])(implicit c: TestContext): Unit = {
    val async = c.async()
    try {
      f map { _: A =>
        c.fail("should get an IllegalArgumentException")
        async.complete()
      }
    } catch {
      case ex: IllegalArgumentException =>
        async.complete()
      case x: Throwable =>
        c.fail(s"should get an IllegalArgumentException, but got exception $x")
        async.complete()
    }
  }
}

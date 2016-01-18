package com.campudus.tableaux.controller

import com.campudus.tableaux.database.domain.CreateSimpleColumn
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.{DatabaseConnection, SingleLanguage, TextType}
import com.campudus.tableaux.{TableauxTestBase, Starter, TestConfig}
import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.{SQLConnection, VertxExecutionContext}
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class StructureControllerTest extends TableauxTestBase {

  def createStructureController(): StructureController = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model)
  }

  @Test
  def checkCreateTableWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn(null, null, null, SingleLanguage, identifier = false))))
  }

  @Test
  def checkCreateColumnWithEmptyParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn(null, None, TextType, SingleLanguage, identifier = false))))
  }

  @Test
  def checkCreateColumnWithNullType(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn("", None, null, SingleLanguage, identifier = false))))
  }

  @Test
  def checkDeleteColumnWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.deleteColumn(0, 0))
  }

  @Test
  def checkDeleteTableWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.deleteTable(0))
  }

  @Test
  def checkGetColumnWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.retrieveColumn(0, 0))
  }

  @Test
  def checkGetTableWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.retrieveTable(0))
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

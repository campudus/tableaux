package com.campudus.tableaux.controller

import com.campudus.tableaux.TestConfig
import com.campudus.tableaux.database.domain.CreateSimpleColumn
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.{DatabaseConnection, SingleLanguage, TextType}
import org.junit.Test
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class StructureControllerTest extends TestVerticle with TestConfig {

  override val verticle = this

  def createStructureController(): StructureController = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model)
  }

  @Test
  def checkCreateTableWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn(null, null, null, SingleLanguage))))
  }

  @Test
  def checkCreateColumnWithEmptyParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn(null, None, TextType, SingleLanguage))))
  }

  @Test
  def checkCreateColumnWithNullType(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq(CreateSimpleColumn("", None, null, SingleLanguage))))
  }

  @Test
  def checkDeleteColumnWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.deleteColumn(0, 0))
  }

  @Test
  def checkDeleteTableWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.deleteTable(0))
  }

  @Test
  def checkGetColumnWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.retrieveColumn(0, 0))
  }

  @Test
  def checkGetTableWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.retrieveTable(0))
  }

  private def illegalArgumentTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should get an IllegalArgumentException") } catch {
      case ex: IllegalArgumentException => testComplete()
      case x: Throwable => fail(s"should get an IllegalArgumentException, but got exception $x")
    }
  }
}

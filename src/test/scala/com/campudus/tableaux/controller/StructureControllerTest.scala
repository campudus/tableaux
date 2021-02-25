package com.campudus.tableaux.controller

import com.campudus.tableaux.database.domain.{CreateSimpleColumn, GenericTable}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.{DatabaseConnection, LanguageNeutral, TextType}
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class StructureControllerTest extends TableauxTestBase {

  def createStructureController(): StructureController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model, roleModel)
  }

  @Test
  def checkCreateTableWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(
      controller.createTable(null,
                             hidden = false,
                             langtags = None,
                             displayInfos = null,
                             tableType = GenericTable,
                             tableGroupId = None)
    )
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(
      controller.createColumns(0,
                               Seq(CreateSimpleColumn(null, null, null, LanguageNeutral, identifier = false, List())))
    )
  }

  @Test
  def checkCreateColumnWithEmptyParameter(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createColumns(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(
      controller
        .createColumns(0, Seq(CreateSimpleColumn(null, None, TextType, LanguageNeutral, identifier = false, List())))
    )
  }

  @Test
  def checkCreateColumnWithNullType(implicit c: TestContext): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(
      controller.createColumns(0, Seq(CreateSimpleColumn("", None, null, LanguageNeutral, identifier = false, List())))
    )
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

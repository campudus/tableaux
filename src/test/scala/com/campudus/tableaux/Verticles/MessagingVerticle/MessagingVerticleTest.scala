package com.campudus.tableaux.Verticles.MessagingVerticle

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.cache.CacheVerticle
import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database.{DatabaseConnection, LanguageNeutral, TextType}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.{ServiceModel, StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.testtools.{TableauxTestBase, TokenHelper}
import com.campudus.tableaux.verticles.MessagingVerticle.MessagingVerticle
import com.campudus.tableaux.verticles.MessagingVerticle.MessagingVerticleClient

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext}
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import org.vertx.scala.core.json.{JsonObject, _}

import scala.collection.mutable.MutableList
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.mockito.Mockito.{doAnswer, spy}
import org.mockito.captor.ArgCaptor
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar

@RunWith(classOf[VertxUnitRunner])
class MessagingVerticleTest extends TableauxTestBase with MockitoSugar {
  val answers: MutableList[JsonObject] = MutableList()
  var messagingClient: MessagingVerticleClient = _
  var tableauxModel: TableauxModel = _
  var structureModel: StructureModel = _

  def addToAnswers(obj: JsonObject): Unit = {
    answers += obj
  }

  @Before
  override def before(context: TestContext): Unit = {
    vertx = Vertx.vertx()

    executionContext = VertxExecutionContext(
      io.vertx.scala.core.Context(vertx.asJava.asInstanceOf[io.vertx.core.Vertx].getOrCreateContext())
    )

    messagingClient = MessagingVerticleClient(vertx)

    val config = Json
      .fromObjectString(fileConfig.encode())
      .put("host", fileConfig.getString("host", "127.0.0.1"))
      .put("port", getFreePort)

    databaseConfig = config.getJsonObject("database", Json.obj())
    authConfig = config.getJsonObject("auth", Json.obj())

    val rolePermissionsPath = config.getString("rolePermissionsPath")
    val rolePermissions = FileUtils(this.vertxAccess()).readJsonFile(rolePermissionsPath, Json.emptyObj())

    host = config.getString("host")
    port = config.getInteger("port").intValue()

    tableauxConfig = new TableauxConfig(
      vertx,
      authConfig,
      databaseConfig,
      config.getString("workingDirectory"),
      config.getString("uploadsDirectory"),
      rolePermissions
    )

    val async = context.async()

    val completionHandler = {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")
        async.complete()

      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit

    val verticleConfig =
      Json.obj(
        "rolePermissions" -> tableauxConfig.rolePermissions,
        "authConfig" -> tableauxConfig.authConfig,
        "databaseConfig" -> tableauxConfig.databaseConfig
      )
    val options = DeploymentOptions()
      .setConfig(verticleConfig)

    val spiedMessagingVerticle = spy(new MessagingVerticle(tableauxConfig))
    val listenersCaptor = ArgCaptor[Seq[Service]]
    val payloadCaptor = ArgCaptor[JsonObject]

    doAnswer(new Answer[Future[Seq[Any]]] {
      override def answer(i: InvocationOnMock): Future[Seq[Any]] = {
        val listeners = i.getArgument[Seq[Service]](0)
        val payload = i.getArgument[JsonObject](1)
        val futures = listeners.map(listener => {
          val name = listener.name
          val res =
            Json.obj(
              "listenerName" -> name,
              "payload" -> payload
            )
          addToAnswers(res)
          Future(
            res
          )
        })
        Future.sequence(futures)

      }
    }).when(spiedMessagingVerticle).sendMessage(
      listenersCaptor,
      payloadCaptor
    )
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)

    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val system = SystemModel(dbConnection)

    for {
      _ <- system.uninstall()
      _ <- system.install()
      _ <- vertx.deployVerticleFuture(new CacheVerticle(tableauxConfig), options)
    } yield {
      vertx.deployVerticleFuture(spiedMessagingVerticle, options)
    }.onComplete(completionHandler)

    val tokenHelper = TokenHelper(this.vertxAccess())

    wildcardAccessToken = tokenHelper.generateToken(
      Json.obj(
        "aud" -> "grud-backend",
        "iss" -> "campudus-test",
        "preferred_username" -> "Test",
        "realm_access" -> Json.obj("roles" -> Json.arr("dev"))
      )
    )

    structureModel = createStructureModel()
    tableauxModel = createTableauxModel()

    user = TableauxUser("", Seq("dev"))
  }

  @After
  override def after(context: TestContext): Unit = {
    answers.drop(answers.size)
    vertx.close(context.asyncAssertSuccess())
  }

  def createStructureModel(): StructureModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)

    StructureModel(dbConnection)
  }

  def createTableauxModel(): TableauxModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)

    val structureModel = createStructureModel()

    TableauxModel(dbConnection, structureModel, tableauxConfig)
  }

  def createSystemController(): SystemController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)
    val structureModel = StructureModel(dbConnection)
    val systemModel = SystemModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel, tableauxConfig)
    val serviceModel = ServiceModel(dbConnection)

    SystemController(tableauxConfig, systemModel, tableauxModel, structureModel, serviceModel, roleModel)
  }

  def createScope(
      tableIncludes: Seq[JsonObject] = Seq(Json.obj("name" -> ".*")),
      columnIncludes: Seq[JsonObject] = Seq(Json.obj("name" -> ".*")),
      tableExcludes: Seq[JsonObject] = Seq(),
      columnExcludes: Seq[JsonObject] = Seq()
  ): JsonObject = {
    Json.obj(
      "scope" -> "global",
      "tables" -> Json.obj(
        "includes" -> tableIncludes,
        "excludes" -> tableExcludes
      ),
      "columns" -> Json.obj(
        "includes" -> columnIncludes,
        "excludes" -> columnExcludes
      )
    )
  }

  def createListenerConfig(
      events: Seq[String],
      host: String = "test_host",
      port: Int = 666,
      route: String = "/",
      headers: JsonObject = Json.obj()
  ): JsonObject = {
    Json.obj(
      "events" -> events,
      "host" -> host,
      "port" -> port,
      "route" -> route,
      "headers" -> headers
    )
  }

  def createListener(name: String, config: JsonObject, scope: JsonObject = createScope()): Future[Service] = {
    val systemController = createSystemController()
    for {
      service <- systemController.createService(
        name,
        ServiceType(Some("listener")),
        None,
        MultiLanguageValue(),
        MultiLanguageValue(),
        active = true,
        Some(config),
        Some(scope)
      ).map(obj => obj.asInstanceOf[Service])
      _ <- messagingClient.servicesChanged()
    } yield service
  }

  def createExpectedJson(
      name: String,
      event: String,
      data: JsonObject,
      tableId: Option[TableId],
      columnId: Option[ColumnId] = None,
      rowId: Option[RowId] = None
  ): JsonObject = {
    val payload = Json.obj(
      "event" -> event,
      "data" -> data
    )
    tableId match {
      case Some(id) => payload.put("tableId", id)
      case _ =>
    }
    columnId match {
      case Some(id) => payload.put("columnId", id)
      case _ =>
    }
    rowId match {
      case Some(id) => payload.put("rowId", id)
      case _ =>
    }
    Json.obj(
      "listenerName" -> name,
      "payload" -> payload
    )
  }

  @Test
  def createTableTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "table_creation_listener"
    val event = MessagingVerticle.EVENT_TYPE_TABLE_CREATED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      listener <- createListener(listenerName, listenerConfig)

      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )

      _ <- messagingClient.tableCreated(table.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, table.getJson, Some(table.id))
      assertJSONEquals(expected, answers.head)
    }

  }

  @Test
  def deleteTableTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "table_deletion_listener"
    val event = MessagingVerticle.EVENT_TYPE_TABLE_DELETED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      _ <- structureModel.tableStruc.delete(table.id)
      _ <- messagingClient.tableDeleted(table.id, table)
    } yield {
      val expected = createExpectedJson(listenerName, event, table.getJson, Some(table.id))
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def changeTableTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "table_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_TABLE_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      _ <- structureModel.tableStruc.change(
        table.id,
        Some("test_table_1_updated"),
        None,
        None,
        None,
        None,
        None
      )
      updatedTable <- structureModel.tableStruc.retrieve(table.id)
      _ <- messagingClient.tableChanged(updatedTable.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, updatedTable.getJson, Some(updatedTable.id))
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def createColumnTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "column_creation_listener"
    val event = MessagingVerticle.EVENT_TYPE_COLUMN_CREATED
    val listenerConfig = createListenerConfig(Seq(event))
    val columnToCreate = CreateSimpleColumn(
      "test_column_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = false,
      Seq(),
      separator = false,
      None
    )

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      column <- structureModel.columnStruc.createColumn(table, columnToCreate)
      _ <- messagingClient.columnCreated(table.id, column.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, column.getJson, Some(table.id), Some(column.id))
      assertJSONEquals(expected, answers.head)
    }

  }

  @Test
  def deleteColumnTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "column_deletion_listener"
    val event = MessagingVerticle.EVENT_TYPE_COLUMN_DELETED
    val listenerConfig = createListenerConfig(Seq(event))
    val columnToCreate = CreateSimpleColumn(
      "test_column_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = false,
      Seq(),
      separator = false,
      None
    )
    val columnToCreate2 = CreateSimpleColumn(
      "test_column_2",
      None,
      TextType,
      LanguageNeutral,
      identifier = false,
      Seq(),
      separator = false,
      None
    )

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      column <- structureModel.columnStruc.createColumn(table, columnToCreate)
      _ <- structureModel.columnStruc.createColumn(table, columnToCreate2)
      _ <- structureModel.columnStruc.delete(table, column.id)
      _ <- messagingClient.columnDeleted(table.id, column.id, column)
    } yield {
      val expected = createExpectedJson(listenerName, event, column.getJson, Some(table.id), Some(column.id))
      assertJSONEquals(expected, answers.head)
    }

  }

  @Test
  def changeColumnTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "column_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_COLUMN_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))
    val columnToCreate = CreateSimpleColumn(
      "test_column_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = false,
      Seq(),
      separator = false,
      None
    )

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      column <- structureModel.columnStruc.createColumn(table, columnToCreate)
      updatedColumn <-
        structureModel.columnStruc.change(
          table,
          column.id,
          Some("test_column_1_changed_name"),
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None
        )
      _ <- messagingClient.columnChanged(table.id, updatedColumn.id)
    } yield {
      val expected =
        createExpectedJson(listenerName, event, updatedColumn.getJson, Some(table.id), Some(updatedColumn.id))
      assertJSONEquals(expected, answers.head)
    }
  }

  def createDefaultTableWithColumn(): Future[(Table, ColumnType[_])] = {
    val defaultColumnToCreate = CreateSimpleColumn(
      "test_column_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = true,
      Seq(),
      separator = false,
      None
    )
    for {
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      column <- structureModel.columnStruc.createColumn(table, defaultColumnToCreate)
    } yield (table, column)

  }

  @Test
  def addRowTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "row_creation_listener"
    val event = MessagingVerticle.EVENT_TYPE_ROW_CREATED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, _) <- createDefaultTableWithColumn()
      createdRow <- tableauxModel.createRow(table, None)
      _ <- messagingClient.rowCreated(table.id, createdRow.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, createdRow.getJson, Some(table.id))
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def deleteRowTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "row_deletion_listener"
    val event = MessagingVerticle.EVENT_TYPE_ROW_DELETED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, _) <- createDefaultTableWithColumn()
      createdRow <- tableauxModel.createRow(table, None)
      _ <- tableauxModel.deleteRow(table, createdRow.id)
      _ <- messagingClient.rowDeleted(table.id, createdRow.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, Json.obj(), Some(table.id), None, Some(createdRow.id))
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def changeRowAnnotationTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "row_annotation_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_ROW_ANNOTATION_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, _) <- createDefaultTableWithColumn()
      createdRow <- tableauxModel.createRow(table, None)
      updatedRowWithAnnotation <- tableauxModel.updateRowAnnotations(table, createdRow.id, Some(true), None)
      _ <- messagingClient.rowAnnotationChanged(table.id, createdRow.id)
    } yield {
      val expected = createExpectedJson(
        listenerName,
        event,
        updatedRowWithAnnotation.getJson,
        Some(table.id),
        None,
        Some(createdRow.id)
      )
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def changeCellTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "cell_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_CELL_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, column) <- createDefaultTableWithColumn()
      createdRow <- tableauxModel.createRow(table, None)
      updatedCell <- tableauxModel.updateCellValue(table, column.id, createdRow.id, "new_test_value")
      _ <- messagingClient.cellChanged(table.id, column.id, createdRow.id)
    } yield {
      val payloadJson = Json.obj("cell" -> updatedCell.getJson, "dependentCells" -> Seq())

      val expected = createExpectedJson(
        listenerName,
        event,
        payloadJson,
        Some(table.id),
        Some(column.id),
        Some(createdRow.id)
      )
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def changeCellWithDependentCellsTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "cell_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_CELL_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))
    val linkColumnToCreate = CreateLinkColumn(
      "link_column_1",
      None,
      1,
      None,
      None,
      singleDirection = true,
      identifier = true,
      Seq(),
      DefaultConstraint,
      None
    )

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, column) <- createDefaultTableWithColumn()
      createdRowTable1 <- tableauxModel.createRow(table, None)
      table2 <- structureModel.tableStruc.create(
        "test_table_2",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      linkColumn <- structureModel.columnStruc.createColumn(table2, linkColumnToCreate)
      createdRowTable2 <- tableauxModel.createRow(table2, None)
      updatedCell <- tableauxModel.updateCellValue(table, column.id, createdRowTable1.id, "new_test_value")
      _ <- tableauxModel.updateCellValue(
        table2,
        linkColumn.id,
        createdRowTable2.id,
        Seq(createdRowTable1.id)
      )
      dependentCells <- tableauxModel.retrieveDependentCells(table, createdRowTable1.id)
      dependentCellValues <- Future.sequence(dependentCells.flatMap {
        case (table, linkColumn, rowIds) =>
          rowIds.map(rowId => {
            tableauxModel.retrieveCell(table, linkColumn.id, rowId).map(data => {
              Json.obj("table" -> table.id, "column" -> linkColumn.id, "row" -> rowId).mergeIn(data.getJson)
            })
          })
      })
      _ <- messagingClient.cellChanged(table.id, column.id, createdRowTable1.id)
    } yield {
      val payloadJson = Json.obj("cell" -> updatedCell.getJson, "dependentCells" -> dependentCellValues)

      val expected = createExpectedJson(
        listenerName,
        event,
        payloadJson,
        Some(table.id),
        Some(column.id),
        Some(createdRowTable1.id)
      )
      assertJSONEquals(expected, answers.head)
    }
  }

  @Test
  def changeCellAnnotationTest(implicit c: TestContext): Unit = okTest {
    val listenerName = "cell_annotation_change_listener"
    val event = MessagingVerticle.EVENT_TYPE_CELL_ANNOTATION_CHANGED
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      (table, column) <- createDefaultTableWithColumn()
      createdRow <- tableauxModel.createRow(table, None)
      addedCellAnnotation <-
        tableauxModel.addCellAnnotation(column, createdRow.id, Seq(), FlagAnnotationType, "important")
      _ <- messagingClient.cellAnnotationChanged(table.id, column.id, createdRow.id)
      cellAnnotationsAfterAdd <- tableauxModel.retrieveCellAnnotations(table, column.id, createdRow.id)
      _ <- tableauxModel.deleteCellAnnotation(column, createdRow.id, addedCellAnnotation.uuid)
      _ <- messagingClient.cellAnnotationChanged(table.id, column.id, createdRow.id)
      cellAnnotationsAfterDelete <- tableauxModel.retrieveCellAnnotations(table, column.id, createdRow.id)
    } yield {
      val expectedAfterAdd = createExpectedJson(
        listenerName,
        event,
        cellAnnotationsAfterAdd.getJson,
        Some(table.id),
        Some(column.id),
        Some(createdRow.id)
      )
      val expectedAfterDelete = createExpectedJson(
        listenerName,
        event,
        cellAnnotationsAfterDelete.getJson,
        Some(table.id),
        Some(column.id),
        Some(createdRow.id)
      )

      assertJSONEquals(expectedAfterAdd, answers.head)
      assertJSONEquals(expectedAfterDelete, answers.drop(1).head)
    }
  }

  @Test
  def listenerScopeTest(implicit c: TestContext): Unit = okTest {
    // val listenerName = "cell_annotation_change_listener"
    // val event = MessagingVerticle.EVENT_TYPE_CELL_ANNOTATION_CHANGED
    // val listenerConfig = createListenerConfig(Seq(event))
    val events = Seq(
      MessagingVerticle.EVENT_TYPE_COLUMN_CREATED
    )
    val excludedTableName = "exclude_this_table_settings"
    val scope = createScope(
      tableIncludes = Seq(
        Json.obj("name" -> ".*settings", "hidden" -> false)
      ),
      tableExcludes = Seq(
        Json.obj("name" -> excludedTableName)
      ),
      columnIncludes = Seq(
        Json.obj("name" -> ".*")
      ),
      columnExcludes = Seq(
        Json.obj("identifier" -> true)
      )
    )
    val listenerName =
      "column_settings_table_listener"
    val listenerConfig = createListenerConfig(events)

    val nonIdentifierColumnToCreate = CreateSimpleColumn(
      "test_column_non_identifier_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = false,
      Seq(),
      separator = false,
      None,
      hidden = false
    )

    val identifierColumnToCreate = CreateSimpleColumn(
      "test_column_identifier_1",
      None,
      TextType,
      LanguageNeutral,
      identifier = true,
      Seq(),
      separator = false,
      None,
      hidden = false
    )

    for {
      _ <- createListener(listenerName, listenerConfig, scope)
      // listener should not be selected
      _ <- createDefaultTableWithColumn()

      settingsTableNonHidden <- structureModel.tableStruc.create(
        "test_table_1_settings",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      // listener should be selected
      nonIdentifierNonHiddenTableColumn <-
        structureModel.columnStruc.createColumn(settingsTableNonHidden, nonIdentifierColumnToCreate)
      _ <- messagingClient.columnCreated(settingsTableNonHidden.id, nonIdentifierNonHiddenTableColumn.id)
      // listener should NOT be selected
      identifierNonHiddenTableColumn <-
        structureModel.columnStruc.createColumn(settingsTableNonHidden, identifierColumnToCreate)
      _ <- messagingClient.columnCreated(settingsTableNonHidden.id, identifierNonHiddenTableColumn.id)

      // table is hidden, listener should NOT be selected for ANY column
      settingsTableHidden <- structureModel.tableStruc.create(
        "test_table_1_hidden_settings",
        hidden = true,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      // listener should NOT be selected
      nonIdentifierHiddenTableColumn <-
        structureModel.columnStruc.createColumn(settingsTableHidden, nonIdentifierColumnToCreate)
      _ <- messagingClient.columnCreated(settingsTableHidden.id, nonIdentifierHiddenTableColumn.id)
      // listener should NOT be selected
      identifierHiddenTableColumn <-
        structureModel.columnStruc.createColumn(settingsTableHidden, identifierColumnToCreate)
      _ <- messagingClient.columnCreated(settingsTableHidden.id, identifierHiddenTableColumn.id)
    } yield {
      val expected = createExpectedJson(
        listenerName,
        events.head,
        nonIdentifierNonHiddenTableColumn.getJson,
        Some(settingsTableNonHidden.id),
        Some(nonIdentifierNonHiddenTableColumn.id),
        None
      )
      // expect only one answer, as listener only applies one time
      assertEquals(1, answers.size)
      assertJSONEquals(expected, answers.head)
    }
  }

}

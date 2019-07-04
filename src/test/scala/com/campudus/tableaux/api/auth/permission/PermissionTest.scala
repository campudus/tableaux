package com.campudus.tableaux.api.auth.permission

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database._
import com.campudus.tableaux.router.auth.permission.{ComparisonObjects, Permission}
import org.junit.{Assert, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

class PermissionTest {

  private def createTable(
      id: Long = 1,
      name: String = "Test",
      hidden: Boolean = false,
      tableType: TableType = GenericTable,
      tableGroupOpt: Option[TableGroup] = None
  ): Table = Table(id, name, hidden, null, null, tableType, tableGroupOpt)

  private def createSimpleColumn(
      id: Long = 1,
      name: String = "TestColumn",
      kind: TableauxDbType = TextType,
      languageType: LanguageType = LanguageNeutral,
      identifier: Boolean = false,
      table: Table = createTable()
  ): ColumnType[_] = {
    val createColumn: CreateColumn =
      CreateSimpleColumn(name, null, kind, languageType, identifier, frontendReadOnly = false, Nil)

    val columnInfo: BasicColumnInformation = BasicColumnInformation(table, id, 1, null, createColumn)

    SimpleValueColumn(kind, languageType, columnInfo)
  }

  val defaultPermissionJson: JsonObject = Json.fromObjectString(
    """
      |{
      |  "type": "grant",
      |  "action": ["delete"],
      |  "scope": "table",
      |  "condition": {
      |    "table": {
      |      "id": ".*"
      |    }
      |  }
      |}
      |""".stripMargin
  )

  @Test
  def isMatching_callWithoutTable_returnsFalse(): Unit = {
    val permission: Permission = Permission(defaultPermissionJson)
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects()))
  }

  @Test
  def isMatching_tablePermissionRegexAll_returnsTrue(): Unit = {

    val table = Table(1, "table", hidden = false, null, null, null, null)

    val permission: Permission = Permission(defaultPermissionJson)
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table)))
  }

  @Test
  def isMatching_tableId(): Unit = {

    val table1 = createTable(1)
    val table2 = createTable(2)
    val table3 = createTable(3)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "table",
        |  "condition": {
        |    "table": {
        |      "id": "[2|3]"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table2)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table3)))
  }

  @Test
  def isMatching_tableName(): Unit = {

    val table1 = createTable(name = "product_model")
    val table2 = createTable(name = "product_model_foo")

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "table",
        |  "condition": {
        |    "table": {
        |      "name": ".*_model$"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table1)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(table2)))
  }

  @Test
  def isMatching_tableHidden(): Unit = {

    val table1 = createTable(hidden = false)
    val table2 = createTable(hidden = true)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "table",
        |  "condition": {
        |    "table": {
        |      "hidden": "true"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table2)))
  }

  @Test
  def isMatching_tableType(): Unit = {

    val genericTable = createTable(tableType = GenericTable)
    val settingsTable = createTable(tableType = SettingsTable)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "table",
        |  "condition": {
        |    "table": {
        |      "tableType": "settings"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(genericTable)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(settingsTable)))
  }

  @Test
  def isMatching_tableGroup(): Unit = {
    val table1 = createTable(tableGroupOpt = Some(TableGroup(1, Seq.empty)))
    val table2 = createTable(tableGroupOpt = Some(TableGroup(2, Seq.empty)))
    val table3 = createTable(tableGroupOpt = Some(TableGroup(3, Seq.empty)))

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "table",
        |  "condition": {
        |    "table": {
        |      "tableGroup": "[1-2]"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(table2)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(table3)))
  }

  @Test
  def isMatching_columnId(): Unit = {
    val column = createSimpleColumn(1)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "column",
        |  "condition": {
        |    "column": {
        |      "id": "1"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(column)))
  }

  @Test
  def isMatching_columnTypes(): Unit = {

    val textColumn = createSimpleColumn(kind = TextType)
    val numericColumn = createSimpleColumn(kind = NumericType)
    val booleanColumn = createSimpleColumn(kind = BooleanType)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "column",
        |  "condition": {
        |    "column": {
        |      "kind": "numeric"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(textColumn)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(numericColumn)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(booleanColumn)))
  }

  @Test
  def isMatching_identifier(): Unit = {

    val column1 = createSimpleColumn(identifier = true)
    val column2 = createSimpleColumn(identifier = true)
    val column3 = createSimpleColumn(identifier = false)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "column",
        |  "condition": {
        |    "column": {
        |      "identifier": "false"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(column1)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(column2)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(column3)))
  }

  @Test
  def isMatching_name(): Unit = {

    val column1 = createSimpleColumn(name = "confidential_data_x")
    val column2 = createSimpleColumn(name = "non_confidential_data_x")
    val column3 = createSimpleColumn(name = "confidential_data_y")
    val column4 = createSimpleColumn(name = "non_confidential_data_y")

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "column",
        |  "condition": {
        |    "column": {
        |      "name": "^confidential.*"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(column1)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(column2)))
    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(column3)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(column4)))
  }

  @Test
  def isMatching_tableAndColumnMixed(): Unit = {

    val modelTable = createTable(name = "bike_model")
    val variantTable = createTable(name = "bike_variant")

    val modelTable_priority_low = createSimpleColumn(name = "priority_low", table = modelTable)
    val modelTable_priority_high = createSimpleColumn(name = "priority_high", table = modelTable)

    val variantTable_priority_low = createSimpleColumn(name = "priority_low", table = variantTable)
    val variantTable_priority_high = createSimpleColumn(name = "priority_high", table = variantTable)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["delete"],
        |  "scope": "column",
        |  "condition": {
        |    "table": {
        |      "name": ".*_model"
        |    },
        |    "column": {
        |      "name": ".*_low"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ComparisonObjects(modelTable, modelTable_priority_low)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(modelTable, modelTable_priority_high)))

    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(variantTable, variantTable_priority_low)))
    Assert.assertEquals(false, permission.isMatching(ComparisonObjects(variantTable, variantTable_priority_high)))
  }
}

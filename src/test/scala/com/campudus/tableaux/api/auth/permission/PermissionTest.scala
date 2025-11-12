package com.campudus.tableaux.api.auth.permission

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json.{Json, JsonObject}

import org.junit.{Assert, Test}

class PermissionTest {

  implicit val user = TableauxUser("Test", Seq.empty[String])

  private def createTable(
      id: Long = 1,
      name: String = "Test",
      hidden: Boolean = false,
      tableType: TableType = GenericTable,
      tableGroupOpt: Option[TableGroup] = None
  ): Table = Table(id, name, hidden, null, null, tableType, tableGroupOpt, None, None, None)

  private def createSimpleColumn(
      id: Long = 1,
      name: String = "TestColumn",
      kind: TableauxDbType = TextType,
      languageType: LanguageType = LanguageNeutral,
      identifier: Boolean = false,
      table: Table = createTable()
  ): ColumnType[_] = {
    val displayInfos = Seq(DisplayInfos.fromString("en", "name", "desc"))
    val createColumn: CreateColumn =
      CreateSimpleColumn(name, null, kind, languageType, identifier, displayInfos, false, None)

    val columnInfo: BasicColumnInformation = BasicColumnInformation(table, id, 1, displayInfos, createColumn)

    SimpleValueColumn(kind, languageType, columnInfo)
  }

  val defaultPermissionJson: JsonObject = Json.fromObjectString(
    """
      |{
      |  "type": "grant",
      |  "action": ["viewTable"],
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
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects()))
  }

  @Test
  def isMatching_tablePermissionRegexAll_returnsTrue(): Unit = {

    val table = Table(1, "table", hidden = false, null, null, null, null, None, None, None)

    val permission: Permission = Permission(defaultPermissionJson)
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table)))
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
        |  "action": ["viewTable"],
        |  "condition": {
        |    "table": {
        |      "id": "[2|3]"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table2)))
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table3)))
  }

  @Test
  def isMatching_tableName(): Unit = {

    val table1 = createTable(name = "product_model")
    val table2 = createTable(name = "product_model_foo")

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "table": {
        |      "name": ".*_model$"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table1)))
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects(table2)))
  }

  @Test
  def isMatching_tableHidden(): Unit = {

    val table1 = createTable(hidden = false)
    val table2 = createTable(hidden = true)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "table": {
        |      "hidden": "true"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table2)))
  }

  @Test
  def isMatching_tableType(): Unit = {

    val genericTable = createTable(tableType = GenericTable)
    val settingsTable = createTable(tableType = SettingsTable)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "table": {
        |      "tableType": "settings"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects(genericTable)))
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(settingsTable)))
  }

  @Test
  def isMatching_table_tableGroup(): Unit = {
    val table1 = createTable(tableGroupOpt = Some(TableGroup(1, Seq.empty)))
    val table2 = createTable(tableGroupOpt = Some(TableGroup(2, Seq.empty)))
    val table3 = createTable(tableGroupOpt = Some(TableGroup(3, Seq.empty)))

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "table": {
        |      "tableGroup": "[1-2]"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table1)))
    Assert.assertEquals(true, permission.isMatching(ViewTable, ComparisonObjects(table2)))
    Assert.assertEquals(false, permission.isMatching(ViewTable, ComparisonObjects(table3)))
  }

  @Test
  def isMatching_table_columnId(): Unit = {
    val column = createSimpleColumn(1)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "column": {
        |      "id": "1"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(column)))
  }

  @Test
  def isMatching_table_columnTypes(): Unit = {

    val textColumn = createSimpleColumn(kind = TextType)
    val numericColumn = createSimpleColumn(kind = NumericType)
    val booleanColumn = createSimpleColumn(kind = BooleanType)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewTable"],
        |  "condition": {
        |    "column": {
        |      "kind": "numeric"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(textColumn)))
    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(numericColumn)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(booleanColumn)))
  }

  @Test
  def isMatching_column_identifier(): Unit = {

    val column1 = createSimpleColumn(identifier = true)
    val column2 = createSimpleColumn(identifier = true)
    val column3 = createSimpleColumn(identifier = false)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewColumn"],
        |  "condition": {
        |    "column": {
        |      "identifier": "false"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(column1)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(column2)))
    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(column3)))
  }

  @Test
  def isMatching_column_name(): Unit = {

    val column1 = createSimpleColumn(name = "confidential_data_x")
    val column2 = createSimpleColumn(name = "non_confidential_data_x")
    val column3 = createSimpleColumn(name = "confidential_data_y")
    val column4 = createSimpleColumn(name = "non_confidential_data_y")

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewColumn"],
        |  "condition": {
        |    "column": {
        |      "name": "^confidential.*"
        |    }
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(column1)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(column2)))
    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(column3)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(column4)))
  }

  @Test
  def isMatching_tableAndColumnMixed(): Unit = {

    val modelTable = createTable(name = "bike_model")
    val variantTable = createTable(name = "bike_variant")

    val model_column_low = createSimpleColumn(name = "priority_low", table = modelTable)
    val model_column_high = createSimpleColumn(name = "priority_high", table = modelTable)

    val variant_column_low = createSimpleColumn(name = "priority_low", table = variantTable)
    val variant_column_high = createSimpleColumn(name = "priority_high", table = variantTable)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["viewColumn"],
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

    Assert.assertEquals(true, permission.isMatching(ViewColumn, ComparisonObjects(modelTable, model_column_low)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(modelTable, model_column_high)))

    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(variantTable, variant_column_low)))
    Assert.assertEquals(false, permission.isMatching(ViewColumn, ComparisonObjects(variantTable, variant_column_high)))
  }

  @Test
  def isMatching_singleLanguageColumn_withLangtagCondition(): Unit = {
    val column = createSimpleColumn(1)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["editCellValue"],
        |  "condition": {
        |    "langtag": "de"
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column)))
    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column, "any value")))
  }

  @Test
  def isMatching_multiLanguageColumn_withLangtagCondition(): Unit = {
    val column = createSimpleColumn(1, languageType = MultiLanguage)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["editCellValue"],
        |  "condition": {
        |    "langtag": "de"
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(
      true,
      permission.isMatching(EditCellValue, ComparisonObjects(column, "wrong value for columnType"))
    )

    val deValue = Json.fromObjectString("""{ "de": "value-de" }""")
    val enValue = Json.fromObjectString("""{ "en": "value-en" }""")
    val deEnValues = Json.fromObjectString("""{ "de": "value-de", "en": "value-en"} """)

    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column, deValue)))
    Assert.assertEquals(false, permission.isMatching(EditCellValue, ComparisonObjects(column, enValue)))
    Assert.assertEquals(false, permission.isMatching(EditCellValue, ComparisonObjects(column, deEnValues)))
  }

  @Test
  def isMatching_multiLanguageColumn_withLangtagConditionMultipleLangtags(): Unit = {
    val column = createSimpleColumn(1, languageType = MultiLanguage)

    val json = Json.fromObjectString(
      """
        |{
        |  "type": "grant",
        |  "action": ["editCellValue"],
        |  "condition": {
        |    "langtag": "de|en|es"
        |  }
        |}
        |""".stripMargin
    )

    val permission: Permission = Permission(json)

    Assert.assertEquals(
      true,
      permission.isMatching(EditCellValue, ComparisonObjects(column, "wrong value for columnType"))
    )

    val deValue = Json.fromObjectString("""{ "de": "value-de" }""")
    val enValue = Json.fromObjectString("""{ "en": "value-en" }""")
    val frValue = Json.fromObjectString("""{ "fr": "value-fr" }""")
    val deEnValues = Json.fromObjectString("""{ "de": "value-de", "en": "value-en"} """)

    val deEnFrValues = Json.fromObjectString("""{ "de": "value-de", "en": "value-en", "fr": "value-fr"} """)

    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column, deValue)))
    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column, enValue)))
    Assert.assertEquals(true, permission.isMatching(EditCellValue, ComparisonObjects(column, deEnValues)))

    Assert.assertEquals(false, permission.isMatching(EditCellValue, ComparisonObjects(column, frValue)))
    Assert.assertEquals(false, permission.isMatching(EditCellValue, ComparisonObjects(column, frValue)))
    Assert.assertEquals(false, permission.isMatching(EditCellValue, ComparisonObjects(column, deEnFrValues)))
  }
}

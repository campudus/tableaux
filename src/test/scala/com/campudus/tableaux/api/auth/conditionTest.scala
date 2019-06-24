package com.campudus.tableaux.api.auth

import com.campudus.tableaux.database.domain.{GenericTable, SettingsTable, Table, TableGroup}
import com.campudus.tableaux.router.auth._
import org.junit.{Assert, Test}
import org.vertx.scala.core.json.Json

class conditionTest {

  val defaultPermissionJson = Json.fromObjectString(
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
    Assert.assertEquals(false, permission.isMatching())
  }

  @Test
  def isMatching_tablePermissionRegexAll_returnsTrue(): Unit = {

    val table = Table(1, "table", hidden = false, null, null, null, null)

    val permission: Permission = Permission(defaultPermissionJson)
    Assert.assertEquals(true, permission.isMatching(Some(table)))
  }

  @Test
  def isMatching_tableId(): Unit = {

    val table1 = Table(1, "table", hidden = false, null, null, null, null)
    val table2 = Table(2, "table", hidden = false, null, null, null, null)
    val table3 = Table(3, "table", hidden = false, null, null, null, null)

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
    Assert.assertEquals(false, permission.isMatching(Some(table1)))
    Assert.assertEquals(true, permission.isMatching(Some(table2)))
    Assert.assertEquals(true, permission.isMatching(Some(table3)))
  }

  @Test
  def isMatching_tableName(): Unit = {

    val table1 = Table(1, "product_model", hidden = false, null, null, null, null)
    val table2 = Table(2, "product_model_foo", hidden = false, null, null, null, null)

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
    Assert.assertEquals(true, permission.isMatching(Some(table1)))
    Assert.assertEquals(false, permission.isMatching(Some(table2)))
  }

  @Test
  def isMatching_tableHidden(): Unit = {

    val table1 = Table(1, "table", hidden = false, null, null, null, null)
    val table2 = Table(2, "table", hidden = true, null, null, null, null)

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
    Assert.assertEquals(false, permission.isMatching(Some(table1)))
    Assert.assertEquals(true, permission.isMatching(Some(table2)))
  }

  @Test
  def isMatching_tableType(): Unit = {

    val genericTable = Table(1, "table", hidden = false, null, null, GenericTable, null)
    val settingsTable = Table(2, "table", hidden = false, null, null, SettingsTable, null)

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
    Assert.assertEquals(false, permission.isMatching(Some(genericTable)))
    Assert.assertEquals(true, permission.isMatching(Some(settingsTable)))
  }

  @Test
  def isMatching_tableGroup(): Unit = {
    val group1 = TableGroup(1, Seq.empty)
    val group2 = TableGroup(2, Seq.empty)
    val group3 = TableGroup(3, Seq.empty)

    val table1 = Table(1, "table", hidden = false, null, null, null, Some(group1))
    val table2 = Table(2, "table", hidden = false, null, null, null, Some(group2))
    val table3 = Table(3, "table", hidden = false, null, null, null, Some(group3))

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

    Assert.assertEquals(true, permission.isMatching(Some(table1)))
    Assert.assertEquals(true, permission.isMatching(Some(table2)))
    Assert.assertEquals(false, permission.isMatching(Some(table3)))
  }
}

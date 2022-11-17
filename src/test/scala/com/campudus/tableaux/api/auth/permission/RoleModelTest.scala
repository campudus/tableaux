package com.campudus.tableaux.api.auth.permission

import com.campudus.tableaux.router.auth.permission._

import org.vertx.scala.core.json.{Json, JsonObject}

import org.junit.{Assert, Test}
import org.scalatest.Assertions._

class RoleModelTest {

  @Test
  def parse_validRolePermissions_onePermissionParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"],
                                                   |      "condition": {
                                                   |        "table": {
                                                   |          "id": ".*",
                                                   |          "hidden": "false"
                                                   |        }
                                                   |      }
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-tables"))
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def parse_twoValidPermissionsInRole_twoPermissionsParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"],
                                                   |      "condition": {
                                                   |        "table": {
                                                   |          "id": ".*",
                                                   |          "hidden": "false"
                                                   |        }
                                                   |      }
                                                   |    }, {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"],
                                                   |      "condition": {
                                                   |        "table": {
                                                   |          "id": ".*",
                                                   |          "hidden": "false"
                                                   |        }
                                                   |      }
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-tables"))
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def parse_validPermissionWithoutCondition_onePermissionParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewMedia"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)
    val roleModel: RoleModel = RoleModel(json)

    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-media"))
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def parse_invalidPermissionType_throwsException(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "invalidType",
                                                   |      "action": ["viewMedia"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    import org.scalatest.Assertions._

    assertThrows[IllegalArgumentException] {
      RoleModel(json)
    }
  }

  @Test
  def parse_invalidAction_throwsException(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["invalidAction"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    import org.scalatest.Assertions._

    assertThrows[IllegalArgumentException] {
      RoleModel(json)
    }
  }

  @Test
  def parse_validPermissionWithLangtagCondition_onePermissionParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-column": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewColumn"],
                                                   |      "condition": {
                                                   |        "langtag": "de|en"
                                                   |      }
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)
    val roleModel: RoleModel = RoleModel(json)

    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-column"))
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def getPermissionsFor_twoRolesWithOneValidPermissionEach_returnsTwoPermissions(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ],
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewMedia"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-tables", "view-media"))
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionsAreMatching_returnsTwoPermissions(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ],
                                                   |  "view-tables3": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, ViewTable)
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionNotMatching_returnsOnePermission(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["editTable"]
                                                   |    }
                                                   |  ],
                                                   |  "view-tables3": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, ViewTable)
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def filterPermissions_threePermissionsOneWithTypeDeny_returnsOnePermission(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    },
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["viewTable"]
                                                   |    },
                                                   |    {
                                                   |      "type": "deny",
                                                   |      "action": ["viewTable"]
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1"), Deny, ViewTable)
    Assert.assertEquals(1, permissions.size)
  }
}

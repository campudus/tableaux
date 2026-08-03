package com.campudus.tableaux.api.auth.permission

import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.router.auth.permission._

import io.vertx.lang.scala.json.JsonObject

import org.junit.{Assert, Test}

class RoleModelTest {

  @Test
  def parse_validRolePermissions_onePermissionParsed(): Unit = {

    val json: JsonObject = new JsonObject("""
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

    val json: JsonObject = new JsonObject("""
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

    val json: JsonObject = new JsonObject("""
                                            |{
                                            |  "view-table": [
                                            |    {
                                            |      "type": "grant",
                                            |      "action": ["viewTable"]
                                            |    }
                                            |  ]
                                            |}""".stripMargin)
    val roleModel: RoleModel = RoleModel(json)

    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-table"))
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def parse_invalidPermissionType_throwsException(): Unit = {

    val json: JsonObject = new JsonObject("""
                                            |{
                                            |  "view-media": [
                                            |    {
                                            |      "type": "invalidType",
                                            |      "action": ["viewMedia"]
                                            |    }
                                            |  ]
                                            |}""".stripMargin)

    Assert.assertThrows(classOf[IllegalArgumentException], () => RoleModel(json))
  }

  @Test
  def parse_invalidAction_throwsException(): Unit = {

    val json: JsonObject = new JsonObject("""
                                            |{
                                            |  "view-media": [
                                            |    {
                                            |      "type": "grant",
                                            |      "action": ["invalidAction"]
                                            |    }
                                            |  ]
                                            |}""".stripMargin)

    Assert.assertThrows(classOf[IllegalArgumentException], () => RoleModel(json))
  }

  @Test
  def parse_validPermissionWithLangtagCondition_onePermissionParsed(): Unit = {

    val json: JsonObject = new JsonObject("""
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

    val json: JsonObject = new JsonObject("""
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
                                            |      "action": ["deleteTable"]
                                            |    }
                                            |  ]
                                            |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-tables", "view-media"))
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionsAreMatching_returnsTwoPermissions(): Unit = {

    val json: JsonObject = new JsonObject("""
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
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, ViewTable, false)
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionNotMatching_returnsOnePermission(): Unit = {

    val json: JsonObject = new JsonObject("""
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
                                            |      "action": ["deleteTable"]
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
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, ViewTable, false)
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def filterPermissions_threePermissionsOneWithTypeDeny_returnsOnePermission(): Unit = {

    val json: JsonObject = new JsonObject("""
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
      roleModel.filterPermissions(Seq("view-tables1"), Deny, ViewTable, false)
    Assert.assertEquals(1, permissions.size)
  }
}

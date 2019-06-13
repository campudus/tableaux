package com.campudus.tableaux.api.auth

import com.campudus.tableaux.router.auth.{Permission, RoleModel}
import org.junit.{Assert, Test}
import org.scalatest.Assertions._
import org.vertx.scala.core.json.{Json, JsonObject}

class permissionTest {

  @Test
  def parse_validRolePermissions_onePermissionParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table",
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
    val permissions: Seq[Permission] = roleModel.getPermissionsFor("view-tables")
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def parse_twoValidPermissionsInRole_twoPermissionsParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table",
                                                   |      "condition": {
                                                   |        "table": {
                                                   |          "id": ".*",
                                                   |          "hidden": "false"
                                                   |        }
                                                   |      }
                                                   |    }, {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "media",
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
    val permissions: Seq[Permission] = roleModel.getPermissionsFor("view-tables")
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def parse_validPermissionWithoutCondition_onePermissionParsed(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "media"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)
    val roleModel: RoleModel = RoleModel(json)

    val permissions: Seq[Permission] = roleModel.getPermissionsFor("view-media")
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def parse_invalidPermissionType_throwsException(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "invalidType",
                                                   |      "action": ["view"],
                                                   |      "scope": "media"
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
                                                   |      "action": ["invalidAction"],
                                                   |      "scope": "media"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    import org.scalatest.Assertions._

    assertThrows[IllegalArgumentException] {
      RoleModel(json)
    }
  }

  @Test
  def parse_invalidScope_throwsException(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "invalidScope"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    assertThrows[IllegalArgumentException] {
      RoleModel(json)
    }
  }
}

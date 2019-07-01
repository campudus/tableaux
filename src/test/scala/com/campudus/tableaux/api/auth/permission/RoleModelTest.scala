package com.campudus.tableaux.api.auth.permission

import com.campudus.tableaux.router.auth.permission._
import org.junit.{Assert, Test}
import org.scalatest.Assertions._
import org.vertx.scala.core.json.{Json, JsonObject}

class RoleModelTest {

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
                                                   |      "action": ["view"],
                                                   |      "scope": "media"
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

  @Test
  def getPermissionsFor_twoRolesWithOneValidPermissionEach_returnsTwoPermissions(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-media": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "media"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] = roleModel.filterPermissions(Seq("view-tables", "view-media"))
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionAndScopeAreMatching_returnsTwoPermissions(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables3": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, View, ScopeTable)
    Assert.assertEquals(2, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesActionNotMatching_returnsOnePermission(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["edit"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables3": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, View, ScopeTable)
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def filterPermissions_threeRolesScopeNotMatching_returnsOnePermission(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "media"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables3": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, View, ScopeTable)
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def filterPermissions_threePermissionsOneWithTypeDeny_returnsOnePermission(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    },
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    },
                                                   |    {
                                                   |      "type": "deny",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1"), Deny, View, ScopeTable)
    Assert.assertEquals(1, permissions.size)
  }

  @Test
  def filterPermissions_withoutDefinedAction_returnsAllPermissions(): Unit = {

    val json: JsonObject = Json.fromObjectString("""
                                                   |{
                                                   |  "view-tables1": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    },
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["edit"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ],
                                                   |  "view-tables2": [
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["view"],
                                                   |      "scope": "table"
                                                   |    },
                                                   |    {
                                                   |      "type": "grant",
                                                   |      "action": ["delete"],
                                                   |      "scope": "table"
                                                   |    }
                                                   |  ]
                                                   |}""".stripMargin)

    val roleModel: RoleModel = RoleModel(json)
    val permissions: Seq[Permission] =
      roleModel.filterPermissions(Seq("view-tables1", "view-tables2"), Grant, ScopeTable)
    Assert.assertEquals(4, permissions.size)
  }
}

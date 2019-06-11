package com.campudus.tableaux.api.auth

import com.campudus.tableaux.router.auth.{Permission, RoleModel}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.runner.RunWith
import org.junit.{Assert, Test}
import org.scalatest.Assertions._
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class permissionTest extends TableauxTestBase {

  // TODO move away from here (assert and TableauxTestBase)
  @Test
  def testAuthorization_tokenSignedWithDifferentKey_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val token: String = "" +
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjYW1wdWR1cy10ZXN0Iiwic3ViIjoidGVzdEBjYW1wdWR1cy5jb20iLCJhdWQiOiJnc" +
        "nVkLWJhY2tlbmQiLCJuYmYiOjAsImlhdCI6MTU1NzMyODIwNywiZXhwIjoyMjIyMjIyMjIyLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ1bml0LXRlc3R" +
        "lciIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGV2ZWxvcGVyIiwidmlldy10YWJsZXMiLCJkZWxldGUtbWVkaWEiLCJ2aWV3L" +
        "WNlbGxzIl19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJuYW1lIjoiVGVzdCBUZXN0IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiVGVzdCIsImdpdmV" +
        "uX25hbWUiOiJUZXN0IiwiZmFtaWx5X25hbWUiOiJUZXN0In0.YrJ4ikXxjRBITp9B98lc-ygr7Xlc52PKSCnSU1G3YWOxec9DJH0ybkGdwSYqVLejQ" +
        "5PC12CVlh19IAEHON2lXTPmMAMOoOlG5dcvTs6MSYnYwoJnTE91MJ0yUJHRcmSkC6npbYnsYjwzk_UKgXcmKYW6UMrsIcU1bEImXWNoLtU"

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

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

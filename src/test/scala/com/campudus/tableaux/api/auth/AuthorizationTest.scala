package com.campudus.tableaux.api.auth

import com.campudus.tableaux.testtools.{TableauxTestBase, TokenHelper}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

@RunWith(classOf[VertxUnitRunner])
class AuthorizationTest extends TableauxTestBase {

  /*
   * For integration tests we are forced to use only one role-permissions configuration because
   * "TableauxTestBase" class manages the vert.x startup in a @Before TestSetup method and for
   * now it is not possible to inject different setting on runtime.
   *
   * Therefore the mass and detailed tests are performed as unit test on controller layer and
   * additionally some base tests with complete REST workflow are located here.
   */

  val defaultTestClaims: JsonObject = Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test")

  def tokenWithRoles(roles: String*): Option[String] = {
    val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

    val claims: JsonObject = defaultTestClaims.put("realm_access", Json.obj("roles" -> roles))

    Some(tokenHelper.generateToken(claims))
  }

  val expectedOkJson: JsonObject = Json.obj("status" -> "ok")

  def getPermission(json: JsonObject): JsonObject = {
    json.getJsonObject("permission")
  }

  private val simpleDefaultService: String = """{
                                               |  "name": "first service",
                                               |  "type": "action"
                                               |}""".stripMargin

  private def createDefaultService: Future[Long] =
    for {
      serviceId <- sendRequest("POST", "/system/services", simpleDefaultService).map(_.getLong("id"))
    } yield serviceId

  def createTableJson(name: String = "Test Table"): JsonObject = {
    Json.obj("name" -> name)
  }

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
  def testAuthorization_invalidAudience_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "__invalid__"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization_invalidIssuer_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "grud-backend", "iss" -> "__invalid__"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization_expirationTimeLiesInThePast_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      import java.time.Instant
      val timestampOneMinuteAgo: Long = Instant.now.getEpochSecond - 60

      val token: String =
        tokenHelper.generateToken(
          Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test", "exp" -> timestampOneMinuteAgo)
        )

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization__correctSignedToken_validAudience_validIssuer__ok(implicit c: TestContext): Unit = {
    okTest {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def deleteTable_validRole(implicit c: TestContext): Unit = {
    okTest {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1", tokenWithRoles("delete-tables", "view-tables"))
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

  @Test
  def deleteTable_withoutRole(implicit c: TestContext): Unit = {
    exceptionTest("error.request.unauthorized") {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1", tokenWithRoles())
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

  // Structure Auth Tests

  @Test
  def enrichTableSeq_createIsAllowed(implicit c: TestContext): Unit = okTest {

    for {
      permission <- sendRequest("GET", "/tables", tokenWithRoles("create-tables")).map(getPermission)
    } yield {

      val expected = Json.obj(
        "create" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTableSeq_createIsNotAllowed(implicit c: TestContext): Unit = okTest {

    for {
      permission <- sendRequest("GET", "/tables", tokenWithRoles()).map(getPermission)
    } yield {

      val expected = Json.obj(
        "create" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_editPropertiesAreAllowed(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- sendRequest("POST", "/tables", createTableJson())
      permission <- sendRequest("GET", "/tables/1", tokenWithRoles("edit-tables")).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_editStructureProperty_onlyForModelTablesAllowed(implicit c: TestContext): Unit = okTest {

    for {
      tableId1 <- sendRequest("POST", "/tables", createTableJson("test_model"))
      tableId2 <- sendRequest("POST", "/tables", createTableJson("test_variant"))
      modelTablePermissions <- sendRequest("GET", "/tables/1", tokenWithRoles("edit-model-tables")).map(getPermission)
      variantTablePermissions <- sendRequest("GET", "/tables/2", tokenWithRoles("edit-model-tables")).map(getPermission)
    } yield {

      val modelTableExpected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true
      )

      val variantTableExpected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> false
      )

      assertJSONEquals(modelTableExpected, modelTablePermissions, JSONCompareMode.LENIENT)
      assertJSONEquals(variantTableExpected, variantTablePermissions, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_noActionIsAllowed(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- sendRequest("POST", "/tables", createTableJson())
      permission <- sendRequest("GET", "/tables/1", tokenWithRoles("view-tables")).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false,
        "createRow" -> false,
        "deleteRow" -> false,
        "editCellAnnotation" -> false,
        "editRowAnnotation" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {

    for {
      tableId <- sendRequest("POST", "/tables", createTableJson())
      permission <- sendRequest("GET", "/tables/1", tokenWithRoles("edit-tables-all-allowed")).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true,
        "createRow" -> true,
        "deleteRow" -> true,
        "editCellAnnotation" -> true,
        "editRowAnnotation" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumnSeq_createIsAllowed(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- sendRequest("POST", "/tables", createTableJson())
      permission <- sendRequest("GET", "/tables/1/columns", tokenWithRoles("create-columns")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumnSeq_createIsNotAllowed(implicit c: TestContext): Unit = okTest {

    for {
      tableId <- sendRequest("POST", "/tables", createTableJson())
      permission <- sendRequest("GET", "/tables/1/columns", tokenWithRoles("view-tables")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_noActionIsAllowed(implicit c: TestContext): Unit = okTest {

    for {
      tableId <- createDefaultTable()
      permission <-
        sendRequest("GET", "/tables/1/columns/1", tokenWithRoles("view-columns", "view-tables")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false,
        "editCellValue" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createDefaultTable()
      permission <-
        sendRequest("GET", "/tables/1/columns/1", tokenWithRoles("edit-columns-all-allowed")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true,
        "editCellValue" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueIsAllowedForLangtagsDeAndEs(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/system/settings/langtags", Json.obj("value" -> Json.arr("de", "en", "fr", "es")))

      _ <- createFullTableWithMultilanguageColumns("Test Table")
      permission1 <-
        sendRequest("GET", "/tables/1/columns/1", tokenWithRoles("can-edit-de-en-cell-values")).map(getPermission)
      permission2 <-
        sendRequest("GET", "/tables/1/columns/2", tokenWithRoles("can-edit-de-en-cell-values")).map(getPermission)
    } yield {
      val expected = Json.obj("de" -> false, "en" -> true, "fr" -> false, "es" -> true)

      assertJSONEquals(expected, permission1.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
      assertJSONEquals(expected, permission2.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueWithoutLangtagConditionIsAllowedForAllLangtags(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/system/settings/langtags", Json.obj("value" -> Json.arr("de", "en", "fr", "es")))

      _ <- createFullTableWithMultilanguageColumns("Test Table")
      permission <- sendRequest("GET", "/tables/1/columns/1", tokenWithRoles("can-edit-cell-values")).map(getPermission)
    } yield {
      val expected = Json.obj("de" -> true, "en" -> true, "fr" -> true, "es" -> true)

      assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueWithoutLangtagConditionIsAllowedForAllCountryTags(implicit c: TestContext): Unit =
    okTest {

      val country_column = Json.obj(
        "name" -> "country_column",
        "kind" -> "currency",
        "languageType" -> "country",
        "countryCodes" -> Json.arr("DE", "AT", "GB")
      )

      val columns = Json.obj("columns" -> Json.arr(country_column))

      for {
        _ <- createEmptyDefaultTable()
        _ <- sendRequest("POST", s"/tables/1/columns", columns)

        permission <-
          sendRequest("GET", "/tables/1/columns/3", tokenWithRoles("can-edit-cell-values")).map(getPermission)
      } yield {
        val expected = Json.obj("DE" -> true, "AT" -> true, "GB" -> true)
        assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
      }
    }

  @Test
  def enrichColumn_editCellValueIsAllowedForCountryTagsATAndGB(implicit c: TestContext): Unit = okTest {
    val country_column = Json.obj(
      "name" -> "country_column",
      "kind" -> "currency",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
    )

    val columns = Json.obj("columns" -> Json.arr(country_column))

    for {
      _ <- createEmptyDefaultTable()
      _ <- sendRequest("POST", s"/tables/1/columns", columns)

      permission <-
        sendRequest("GET", "/tables/1/columns/3", tokenWithRoles("can-edit-cell-values-AT-GB")).map(getPermission)
    } yield {
      val expected = Json.obj("DE" -> false, "AT" -> true, "GB" -> true)
      assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrich_createGranted_onlyCreateIsAllowed(implicit c: TestContext): Unit = {

    def getPermission(json: JsonObject): JsonObject = {
      json.getJsonObject("permission")
    }
    def createTableJson: JsonObject = {
      Json.obj("name" -> s"Test Table")
    }

    okTest {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        permission <- sendRequest("GET", "/tables", tokenWithRoles("create-tables")).map(getPermission)
      } yield {

        val expected = Json.obj("create" -> true)

        assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def enrich_noPermission_noActionIsAllowed(implicit c: TestContext): Unit =
    okTest {

      for {
        permission <- sendRequest("GET", "/folders?langtag=de", tokenWithRoles()).map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> false,
          "delete" -> false
        )

        assertJSONEquals(expected, permission)
      }
    }

  @Test
  def enrich_editAndDeleteGranted_onlyEditAndDeleteAreAllowed(implicit c: TestContext): Unit = {

    okTest {
      for {
        permission <- sendRequest("GET", "/folders?langtag=de", tokenWithRoles("edit-delete-media")).map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> true,
          "delete" -> true
        )

        assertJSONEquals(expected, permission)
      }
    }
  }

  @Test
  def enrich_editAndDeleteGranted_deleteDenied_onlyEditIsAllowed(implicit c: TestContext): Unit = {

    okTest {
      for {
        permission <- sendRequest(
          "GET",
          "/folders?langtag=de",
          tokenWithRoles("edit-delete-media", "NOT-delete-media")
        ).map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> true,
          "delete" -> false
        )

        assertJSONEquals(expected, permission)
      }
    }
  }

  @Test
  def retrieveService_noActionIsAllowed(implicit c: TestContext): Unit = okTest {

    for {
      serviceId <- createDefaultService
      permission <- sendRequest("GET", "/system/services/1", tokenWithRoles("view-services")).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveServices_createIsAllowed(implicit c: TestContext): Unit = okTest {
    for {
      serviceId <- createDefaultService
      permission <- sendRequest("GET", "/system/services", tokenWithRoles("view-create-services")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveServices_createIsNotAllowed(implicit c: TestContext): Unit = okTest {

    for {
      serviceId <- createDefaultService
      permission <- sendRequest("GET", "/system/services", tokenWithRoles("view-services")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveService_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {

    for {
      serviceId <- createDefaultService
      permission <-
        sendRequest("GET", "/system/services/1", tokenWithRoles("edit-and-delete-services")).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

}

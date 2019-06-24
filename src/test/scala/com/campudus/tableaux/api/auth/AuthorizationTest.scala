package com.campudus.tableaux.api.auth

import com.campudus.tableaux.testtools.{TableauxTestBase, TokenHelper}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.runner.RunWith
import org.junit.{Ignore, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class AuthorizationTest extends TableauxTestBase {

  val defaultTestClaims: JsonObject = Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test")

  def tokenWithRoles(roles: String*): Option[String] = {
    val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

    val claims: JsonObject = defaultTestClaims.put("realm_access", Json.obj("roles" -> roles))
    println(s"XXX: $claims")

    Some(tokenHelper.generateToken(claims))
  }

  val expectedOkJson: JsonObject = Json.obj("status" -> "ok")

  @Ignore
  @Test
  def deleteTable_validRole(implicit c: TestContext): Unit = {
    // TODO ich kann immer nur eine globale role-permissions config mitgeben, weil die config im @before TestSetup schon geladen wird
    // deshalb testen wir die permissions direkt als Unit Tests mit dem Controller und sporadisch mit dem kompletten REST Request hier
    okTest {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1", tokenWithRoles("delete-tables"))
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

}

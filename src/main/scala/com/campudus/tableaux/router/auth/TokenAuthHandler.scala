package com.campudus.tableaux.router.auth

import java.util

import com.campudus.tableaux.helper.VertxAccess
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
//import io.vertx.core.{AsyncResult, Handler}
import io.vertx.scala.core.http.{HttpServerRequest}
//import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.{auth, web}
import io.vertx.scala.ext.web.handler.AuthHandler
import org.vertx.scala.core.json.JsonCompatible

import scala.collection.mutable
import scala.concurrent.Future

class TokenAuthHandler(override val vertx: Vertx, authProvider: TableauxAuthHandler)
    extends AuthHandler
    with VertxAccess
    with JsonCompatible {

  override def addAuthority(authority: String): AuthHandler = ???

  override def handle(context: web.RoutingContext): Unit = {
    val req: HttpServerRequest = context.request()
    val body = context.getBodyAsJson

//    if (req.method() != HttpMethod.POST) {
//      context.fail(405) // Must be a POST
//    } else if (body == null) {
//      logger.warn("No username or password provided in Json - did you forget to include a BodyHandler?")
//      context.fail(400)
//    } else {
//      val username = body.g ("username")
//      val password = body.getString("password")
//
//      if (username == null || password == null) {
//        logger.warn("No username or password provided in Json - did you forget to include a BodyHandler?")
//        context.fail(400)
//      } else {
//        authProvider.authenticate(body) map {
//          case authenticatedUser =>
//            val session = context.session()
//
//            if (session == null) {
//              context.fail(500)
//            } else {
////              context.setUser(authenticatedUser)
//              context.next()
//            }
//        } recover {
//          case ex => context.fail(ex) // Failed login
//        }
//      }
//    }
  }
  override def asJava: AnyRef = ???

  override def addAuthorities(authorities: mutable.Set[String]): AuthHandler = ???

  override def authorizeFuture(user: auth.User): Future[Unit] = ???

  override def parseCredentials(context: web.RoutingContext, handler: Handler[AsyncResult[JsonObject]]): Unit = ???

  override def authorize(user: auth.User, handler: Handler[AsyncResult[Unit]]): Unit = ???

  override def parseCredentialsFuture(context: web.RoutingContext): Future[JsonObject] = ???
}

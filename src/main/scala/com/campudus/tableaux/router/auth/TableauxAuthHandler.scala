package com.campudus.tableaux.router.auth

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.oauth2.KeycloakHelper
import io.vertx.scala.ext.auth.{AuthProvider, User}
import io.vertx.scala.ext.web.RoutingContext
import io.vertx.scala.ext.web.handler.AuthHandler

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

class TableauxAuthHandler(override val vertx: Vertx, authProvider: AuthProvider, tableauxConfig: TableauxConfig)
    extends AuthHandler
    with VertxAccess {

  var jwkIsLoaded = false

//  override def handle(context: RoutingContext): Unit = {
//    for {
//      _ <- loadJwkIfNecessary
//      _ <- Future.successful(oauth2Handler.handle(context))
//    } yield ()
//  }

//  private def loadJwkIfNecessary: Future[Unit] = {
//
//    if (jwkIsLoaded) {
//      return Future.successful(())
//    }
//
//    if (tableauxConfig.authConfig.containsKey("realm-public-key")) {
//      logger.info(s"JWK not loaded because public key is defined in keycloak config")
//      jwkIsLoaded = true
//      return Future.successful(())
//    }
//
//    val jwkFuture = authProvider.loadJWKFuture()
//
//    jwkFuture.onComplete({
//      case Success(_) =>
//        jwkIsLoaded = true
//        logger.info(s"JWK loading successfully")
//      case Failure(e) =>
//        logger.error("JWK loading failed", e)
//    })
//
//    jwkFuture
//  }

//  def handleExtStuff(context: RoutingContext): Unit = {
//
//    val accessTokenString: String = KeycloakHelper.rawAccessToken(context.user().get.principal)
//    val token: JsonObject = KeycloakHelper.parseToken(accessTokenString)
//
//    oauth2Provider.decodeTokenFuture(accessTokenString).onComplete {
//      case Success(u: AccessToken) => {
//        println("decode success: " + u)
//
//        println("expired: " + u.expired())
//        println("tokenType: " + u.tokenType())
//
//        println("accessToken: " + u.accessToken().toString)
//        println("tokenType: " + u)
//
//        val uif = u.userInfoFuture()
//
//        uif.onComplete {
//          case Success(at) => {
//            println("userInfoFuture success: " + at)
//          }
//          case Failure(cause) => {
//            println("userInfoFuture failed: " + cause)
//            println("XXX: token validation failed: " + cause.getMessage)
//          }
//        }
//
//        println("XXX: " + u.accessToken().encodePrettily())
//        println("XXX: " + u.accessToken().getString("iss"))
//      }
//      case Failure(cause) => {
//        println("failed: " + cause)
//        println("decode failed: " + cause.getMessage)
//      }
//    }

  //    val tt: AccessToken = context.user().get.principal().asInstanceOf[AccessToken]
  //    println("XXX: " + tt.expired())

  //    oauth2
  //      .authenticateFuture(token)
  //      .onComplete {
  //        case Success(u) => {
  //          println("authentication success: " + u)
  //        }
  //        case Failure(cause) => {
  //          println("failed: " + cause)
  //          println("authentication failed: " + cause.getMessage)
  //        }
  //      }

  //    oauth2
  //      .introspectTokenFuture(accessTokenString)
  //      .onComplete {
  //        case Success(at) => {
  //          println("success: " + at)
  //        }
  //        case Failure(cause) => {
  //          println("failed: " + cause)
  //          println("XXX: token validation failed: " + cause.getMessage)
  //        }
  //      }
  //      .onComplete(res => {
  //        if (res.isSuccess) {
  //          println("XXX: " + "token is valid!")
  //          val accessTokenString = res.get
  //        } else {
  //          println("XXX: token validation failed: " + res.failed)
  //        }
  //      })

//    println("XXX: " + token.getString("preferred_username"))
//
//    println("yes babe")
//    context.next()
//  }

//  override def handle(context: web.RoutingContext): Unit = {
//
//    val accessTokenString: String = KeycloakHelper.rawAccessToken(context.user().principal())
//    val token: JsonObject = KeycloakHelper.parseToken(accessTokenString)
//
//    for {
//      _ <- loadJwkIfNecessary
//      _ <- oauth2Provider.authenticateFuture(token)
//    } yield ()
//  }

  //  override def handle(context: RoutingContext): Unit = {
//    val accessTokenString: String = KeycloakHelper.rawAccessToken(context.user().principal())
//    val token: JsonObject = KeycloakHelper.parseToken(accessTokenString)
//
//    for {
//      _ <- loadJwkIfNecessary
//      _ <- oauth2Provider.authenticateFuture(token)
//    } yield ()
//  }

  override def handle(context: RoutingContext): Unit = {

    println("XXX: " + context.user())

    val accessTokenString: String = KeycloakHelper.rawAccessToken(context.user().get.principal())
    val token: JsonObject = KeycloakHelper.parseToken(accessTokenString)

    println("XXX: " + token)

//    authProvider
//      .authenticateFuture(token)
//      .onComplete({
//        case Success(at) => {
//          println("success: " + at)
//        }
//        case Failure(cause) => {
//          println("failed: " + cause)
//          println("XXX: token validation failed: " + cause.getMessage)
//        }
//      })

    for {
//      _ <- loadJwkIfNecessary
      user <- authProvider.authenticateFuture(token)
    } yield
      (
        println("XXX: " + user.toString)
      )
  }

  override def addAuthority(authority: String): AuthHandler = ???

  override def asJava: AnyRef = this.asInstanceOf[TableauxAuthHandler]

  override def addAuthorities(authorities: mutable.Set[String]): AuthHandler = ???

  override def parseCredentials(context: RoutingContext, handler: Handler[AsyncResult[JsonObject]]): Unit = ???

  override def authorize(user: User, handler: Handler[AsyncResult[Unit]]): Unit = ???

  override def parseCredentialsFuture(context: RoutingContext): Future[JsonObject] = ???

  override def authorizeFuture(user: User): Future[Unit] = ???
}

object TableauxAuthHandler {

  def apply(vertx: Vertx, authProvider: AuthProvider, config: TableauxConfig): TableauxAuthHandler = {
    val tah = new TableauxAuthHandler(vertx, authProvider: AuthProvider, config)
//    tah.loadJwkIfNecessary
    tah
  }
}

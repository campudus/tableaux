//package com.campudus.tableaux.router.auth
//
//import java.lang.Boolean
//
//import io.vertx.core.{AsyncResult, Handler}
//import io.vertx.scala.ext.auth.User
//import io.vertx.scala.ext.auth.AuthProvider
////import io.vertx.ext.auth.{AbstractUser, AuthProvider}
//import io.vertx.scala.ext.auth.User
//import org.vertx.scala.core.json._
//
//class TableauxUser(val person: User) extends JsonCompatible {
//
//  val rolesWithPermissions: JsonObject = Json.emptyObj()
//
//  val divisions: JsonObject = Json.emptyObj()
//
////  def copy(person: Person = person, divisions: JsonObject = divisions) = {
////    val user = new ShowroomUser(person)
////
////    user.rolesWithPermissions.mergeIn(this.rolesWithPermissions)
////    user.divisions.mergeIn(divisions)
////
////    user
////  }
//
////  @throws(classOf[CurrentDivisionNotAvailableException])
////  def getCurrentDivision: Division = {
////    val currentDivisionId = person.requireCurrentDivisionId()
////    val currentDivisions = divisions.getJsonArray("divisions").toJsonObjects.map { division =>
////      Division.fromJson(division.getJsonObject("division"))
////    }
////
////    currentDivisions.find(_.id == currentDivisionId).getOrElse {
////      throw CurrentDivisionNotAvailableException()
////    }
////  }
//
//  override def principal(): JsonObject = person.principal()
//
//  override def doIsPermitted(permission: String, resultHandler: Handler[AsyncResult[Boolean]]): Unit = ???
//}

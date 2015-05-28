package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, FileController}
import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.vertx.java.core.json.JsonElement
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.routing.{Get, Put, Post}

import scala.concurrent.Future
import scala.util.matching.Regex

object MediaRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => MediaController): MediaRouter = {
    new MediaRouter(config, controllerCurry(config))
  }
}

class MediaRouter(override val config: TableauxConfig, val controller: MediaController) extends BaseRouter {
  /**
   * Regex for a UUID Version 4
   */
  val uuidRegex: String = "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  val FolderId: Regex = "/folders/(\\d+)".r
  val FileId: Regex = s"/files/($uuidRegex)".r
  val FileIdStatic: Regex = s"/files/($uuidRegex)/.*".r

  override def routes(implicit req: HttpServerRequest): Routing =  {
    case Post("/folders") => asyncSetReply({
      getJson(req) flatMap { json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = retrieveNullableField(json)("parent")

        println(parent)

        controller.addNewFolder(name, description, parent)
      }
    })
    case Get(FolderId(id)) => asyncGetReply(controller.retrieveFolder(id.toLong))
    case Put(FolderId(id)) => asyncSetReply({
      getJson(req) flatMap { json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = retrieveNullableField(json)("parent")

        println(parent)

        controller.changeFolder(id.toLong, name, description, parent)
      }
    })

    case Post("/files") => ???
    case Get(FileId(uuid)) => ???
    case Get(FileIdStatic(uuid)) => ???
    case Put(FileId(uuid)) => ???
  }

  def retrieveNullableField[A](json: JsonObject)(field: String): Option[A] = {
    Option(json.getField[A](field))
  }
}
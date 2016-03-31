package com.campudus.tableaux.database.model

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.Folder
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.joda.time.DateTime
import org.vertx.scala.core.json.{Json, JsonArray}

import scala.concurrent.Future

object FolderModel {
  type FolderId = Long

  def apply(connection: DatabaseConnection): FolderModel = {
    new FolderModel(connection)
  }
}

class FolderModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val table: String = "folder"

  def add(name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    val insert =
      s"""INSERT INTO $table (
          |name,
          |description,
          |idparent,
          |created_at,
          |updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP,NULL) RETURNING id, created_at""".stripMargin

    connection.transactional { t =>
      for {
        (t, result) <- t.query(insert, Json.arr(name, description, parent.orNull))
      } yield {
        val inserted = insertNotNull(result).head

        val id = inserted.get[Long](0)
        val createdAt = DateTime.parse(inserted.get[String](1))

        (t, Folder(id, name, description, parent, Some(createdAt), None))
      }
    }
  }

  def convertJsonArrayToFolder(arr: JsonArray): Folder = {
    Folder(
      arr.get[FolderId](0), //id
      arr.get[String](1), //name
      arr.get[String](2), //description
      Option(arr.get[FolderId](3)), //idparent
      convertStringToDateTime(arr.get[String](4)), //created_at
      convertStringToDateTime(arr.get[String](5)) //updated_at
    )
  }

  def update(o: Folder): Future[Folder] = {
    val update =
      s"""UPDATE $table SET
          |name = ?,
          |description = ?,
          |idparent = ?,
          |updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING created_at, updated_at""".stripMargin

    for {
      result <- connection.query(update, Json.arr(o.name, o.description, o.parent.orNull, o.id.toString))
      resultArr <- Future(updateNotNull(result))
    } yield {
      Folder(
        o.id,
        o.name,
        o.description,
        o.parent,
        convertStringToDateTime(resultArr.head.get[String](0)), //created_at
        convertStringToDateTime(resultArr.head.get[String](1)) //updated_at
      )
    }
  }

  def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table"

    connection.selectSingleValue(select)
  }

  def retrieveAll(): Future[Seq[Folder]] = {
    val select =
      s"""SELECT
          |id,
          |name,
          |description,
          |idparent,
          |created_at,
          |updated_at FROM $table ORDER BY name""".stripMargin

    for {
      result <- connection.query(select)
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  def retrieveSubfolders(folder: Folder): Future[Seq[Folder]] = {
    def select(condition: String) =
      s"""SELECT
          |id,
          |name,
          |description,
          |idparent,
          |created_at,
          |updated_at FROM $table WHERE $condition ORDER BY name""".stripMargin

    for {
      result <- folder match {
        case MediaController.ROOT_FOLDER => connection.query(select("idparent IS NULL"))
        case f => connection.query(select("idparent = ?"), Json.arr(f.id.toString))
      }
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  def delete(o: Folder): Future[Unit] = {
    deleteById(o.id)
  }

  def retrieve(id: FolderId): Future[Folder] = {
    val select =
      s"""SELECT
          |id,
          |name,
          |description,
          |idparent,
          |created_at,
          |updated_at FROM $table WHERE id = ?""".stripMargin

    for {
      result <- connection.query(select, Json.arr(id.toString))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToFolder(resultArr.head)
    }
  }

  def deleteById(id: FolderId): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE id = ?"

    for {
      result <- connection.query(delete, Json.arr(id.toString))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }
}
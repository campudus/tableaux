package com.campudus.tableaux.database.model

import com.campudus.tableaux.ShouldBeUniqueException
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.Folder
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
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

    for {
      _ <- checkUniqueName(parent, None, name)

      result <- connection.query(insert, Json.arr(name, description, parent.orNull))
      id = insertNotNull(result).head.get[Long](0)

      folder <- retrieve(id)
    } yield folder
  }

  def update(id: FolderId, name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    val update =
      s"""UPDATE $table SET
         |name = ?,
         |description = ?,
         |idparent = ?,
         |updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING created_at, updated_at""".stripMargin

    for {
      _ <- checkUniqueName(parent, Some(id), name)

      result <- connection.query(update, Json.arr(name, description, parent.orNull, id))
      _ = updateNotNull(result)

      folder <- retrieve(id)
    } yield folder
  }

  def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table"

    connection.selectSingleValue(select)
  }

  private def selectStatement(conditions: Option[String]): String = {
    val where = if (conditions.isDefined) {
      s"WHERE ${conditions.get}"
    } else {
      ""
    }

    s"""SELECT
       |id,
       |name,
       |description,
       |(
       |  WITH RECURSIVE prev AS (
       |    SELECT folder.id, ARRAY[]::bigint[] AS parents, FALSE AS cycle
       |    FROM folder WHERE folder.idparent IS NULL
       |    UNION ALL
       |    SELECT folder.id, parents || folder.idparent AS parents, folder.id = ANY(parents) AS cycle
       |    FROM prev INNER JOIN folder ON (prev.id = idparent AND prev.cycle = FALSE)
       |  )
       |  SELECT ARRAY_TO_JSON(parents) FROM prev WHERE cycle IS FALSE AND id = f.id
       |) AS parents,
       |created_at,
       |updated_at FROM $table f $where ORDER BY name""".stripMargin
  }

  private def convertJsonArrayToFolder(arr: JsonArray): Folder = {
    import scala.collection.JavaConverters._
    val parents = Json
      .fromArrayString(arr.getString(3))
      .asScala
      .toSeq
      .map({
        case f: java.lang.Integer => f.toLong
      })

    Folder(
      arr.get[FolderId](0), //id
      arr.get[String](1), //name
      arr.get[String](2), //description
      parents, //parents
      convertStringToDateTime(arr.get[String](4)), //created_at
      convertStringToDateTime(arr.get[String](5)) //updated_at
    )
  }

  def retrieve(id: FolderId): Future[Folder] = {
    for {
      result <- connection.query(selectStatement(Some("id = ?")), Json.arr(id.toString))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToFolder(resultArr.head)
    }
  }

  def retrieveAll(): Future[Seq[Folder]] = {
    for {
      result <- connection.query(selectStatement(None))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  def retrieveSubfolders(folder: Folder): Future[Seq[Folder]] = {
    for {
      result <- folder match {
        case MediaController.ROOT_FOLDER => connection.query(selectStatement(Some("idparent IS NULL")))
        case f => connection.query(selectStatement(Some("idparent = ?")), Json.arr(f.id.toString))
      }
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  def delete(folder: Folder): Future[Unit] = {
    deleteById(folder.id)
  }

  private def deleteById(id: FolderId): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE id = ?"

    for {
      result <- connection.query(delete, Json.arr(id.toString))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }

  private def checkUniqueName(parent: Option[FolderId], folder: Option[FolderId], name: String): Future[Unit] = {
    val (condition, parameter) = (parent, folder) match {
      case (Some(parentId), Some(folderId)) =>
        ("idparent = ? AND id != ? AND name = ?", Json.arr(parentId, folderId, name))
      case (None, Some(folderId)) =>
        ("idparent IS NULL AND id != ? AND name = ?", Json.arr(folderId, name))
      case (Some(parentId), None) =>
        ("idparent = ? AND name = ?", Json.arr(parentId, name))
      case (None, None) =>
        ("idparent IS NULL AND name = ?", Json.arr(name))
    }

    val sql = s"SELECT COUNT(*) = 0 FROM $table WHERE $condition"
    connection
      .selectSingleValue[Boolean](sql, parameter)
      .flatMap({
        case true => Future.successful(())
        case false =>
          Future.failed(
            new ShouldBeUniqueException(s"Name of folder should be unique ($parent, $folder, $name).", "foldername"))
      })
  }
}

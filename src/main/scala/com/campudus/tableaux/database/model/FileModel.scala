package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseHandler}
import com.campudus.tableaux.helper.ResultChecker._
import org.joda.time.DateTime
import org.vertx.java.core.json.JsonArray
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object FileModel {
  def apply(connection: DatabaseConnection): FileModel = {
    new FileModel(connection)
  }
}

class FileModel(override protected[this] val connection: DatabaseConnection) extends DatabaseHandler[File, UUID] {
  val table: String = "file"

  /**
   * Will add a new entity marked as temporary!
   */
  override def add(o: File): Future[File] = {
    //if a UUID is already defined use this one
    val uuid = o.uuid.getOrElse(UUID.randomUUID())

    val insert =
      s"""INSERT INTO $table (
          |uuid,
          |name,
          |description,
          |mime_type,
          |filename,
          |idfolder,
          |created_at) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP) RETURNING created_at""".stripMargin

    for {
      result <- connection.query(insert, Json.arr(uuid.toString, o.name, o.description, o.mimeType, o.filename, o.folder.orNull))
      resultArr <- Future(insertNotNull(result).head)
    } yield {
      val createdAt = DateTime.parse(resultArr.get[String](0))
      File(Some(uuid), o.name, o.description, o.mimeType, o.filename, o.folder, Some(createdAt), None)
    }
  }

  override def retrieve(id: UUID): Future[File] = {
    val select =
      s"""SELECT
          |uuid,
          |name,
          |description,
          |mime_type,
          |filename,
          |idfolder,
          |created_at,
          |updated_at
          |FROM $table WHERE
          |uuid = ? AND tmp = FALSE""".stripMargin

    for {
      result <- connection.query(select, Json.arr(id.toString))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToFile(resultArr.head)
    }
  }

  def convertJsonArrayToFile(arr: JsonArray): File = {
    File(
      arr.get[String](0), //uuid
      arr.get[String](1), //name
      arr.get[String](2), //description
      arr.get[String](3), //mime_type
      arr.get[String](4), //filename
      arr.get[Long](5), //idfolder
      arr.get[String](6), //created_at
      arr.get[String](7) //updated_at
    )
  }

  implicit def convertStringToUUID(str: String): Option[UUID] = {
    Some(UUID.fromString(str))
  }

  override def update(o: File): Future[File] = {
    val update =
      s"""UPDATE $table SET
          |name = ?,
          |description = ?,
          |idfolder = ?,
          |updated_at = CURRENT_TIMESTAMP,
          |tmp = FALSE
          |WHERE uuid = ? RETURNING mime_type, filename, created_at, updated_at""".stripMargin

    for {
      result <- connection.query(update, Json.arr(o.name, o.description, o.folder.orNull, o.uuid.get.toString))
      resultArr <- Future(updateNotNull(result))
    } yield {
      File(
        o.uuid,
        o.name,
        o.description,
        resultArr.head.get[String](0), //mime_type
        resultArr.head.get[String](1), //filename
        o.folder,
        resultArr.head.get[String](2), //created_at
        resultArr.head.get[String](3) //updated_at
      )
    }
  }

  override def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table WHERE tmp = FALSE"

    for {
      result <- connection.query(select)
      resultArr <- Future(selectNotNull(result))
    } yield {
      resultArr.head.get[Long](0)
    }
  }

  override def retrieveAll(): Future[Seq[File]] = {
    val select =
      s"""SELECT
          |uuid,
          |name,
          |description,
          |mime_type,
          |filename,
          |idfolder,
          |created_at,
          |updated_at FROM $table WHERE tmp = FALSE""".stripMargin

    for {
      result <- connection.query(select)
      resultArr <- Future(selectNotNull(result))
    } yield {
      resultArr.map(convertJsonArrayToFile)
    }
  }

  def retrieveFromFolder(folder: FolderId): Future[Seq[File]] = {
    val select =
      s"""SELECT
          |uuid,
          |name,
          |description,
          |mime_type,
          |filename,
          |idfolder,
          |created_at,
          |updated_at FROM $table WHERE idfolder = ? AND tmp = FALSE""".stripMargin

    for {
      result <- connection.query(select, Json.arr(folder))
      resultArr <- Future(getSeqOfJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFile)
    }
  }

  override def delete(o: File): Future[Unit] = {
    deleteById(o.uuid.get)
  }

  override def deleteById(id: UUID): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE uuid = ?"

    for {
      result <- connection.query(delete, Json.arr(id.toString))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }
}

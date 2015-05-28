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

  override def add(o: File): Future[File] = {
    val uuid = UUID.randomUUID()

    val insert =
      s"""INSERT INTO $table (
                              |uuid,
                              |name,
                              |description,
                              |mime_type,
                              |idfolder,
                              |created_at,
                              |updated_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP,NULL) RETURNING created_at""".stripMargin

    connection.transactional { t =>
      for {
        (t, result) <- t.query(insert, Json.arr(uuid.toString, o.name, o.description, o.mimeType, o.folder.orNull))
      } yield {
        val inserted = insertNotNull(result).head

        val createdAt = DateTime.parse(inserted.get[String](0))

        (t, File(Some(uuid), o.name, o.description, o.mimeType, o.folder, Some(createdAt), None))
      }
    }
  }

  override def retrieve(id: UUID): Future[File] = {
    val select =
      s"""SELECT
         |uuid,
         |name,
         |description,
         |mime_type,
         |idfolder,
         |created_at,
         |updated_at FROM $table WHERE uuid = ?""".stripMargin

    for {
      result <- connection.singleQuery(select, Json.arr(id.toString))
      resultArr <- Future.apply(selectNotNull(result))
    } yield {
      resultArr.head
    }
  }

  implicit def convertJsonArrayToFile(arr: JsonArray): File = {
    File(
      arr.get[String](0), //uuid
      arr.get[String](1), //name
      arr.get[String](2), //description
      arr.get[String](3), //mime_type
      arr.get[Long](4),   //idfolder
      arr.get[String](5), //created_at
      arr.get[String](6) //updated_at
    )
  }

  implicit def convertLongToFolderId(id: Long): Option[FolderId] = {
    //TODO not cool!
    if (id == 0) {
      None
    } else {
      Some(id.toLong)
    }
  }

  implicit def convertStringToUUID(str: String): Option[UUID] = {
    Some(UUID.fromString(str))
  }

  implicit def convertStringToDateTime(str: String): Option[DateTime] = {
    if (str == null) {
      None
    } else {
      Option(DateTime.parse(str))
    }
  }

  override def update(o: File): Future[File] = {
    val update =
      s"""UPDATE $table SET
                         |name = ?,
                         |description = ?,
                         |mime_type = ?,
                         |idfolder = ?,
                         |updated_at = CURRENT_TIMESTAMP WHERE uuid = ? RETURNING created_at, updated_at""".stripMargin

    for {
      result <- connection.singleQuery(update, Json.arr(o.name, o.description, o.mimeType, o.folder.orNull, o.uuid.get.toString))
      resultArr <- Future.apply(updateNotNull(result))
    } yield {
      File(
        o.uuid, //uuid
        o.name, //name
        o.description, //description
        o.mimeType, //mime_type
        o.folder,
        resultArr.head.get[String](0), //created_at
        resultArr.head.get[String](1) //updated_at
      )
    }
  }

  override def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table"

    for {
      result <- connection.singleQuery(select, Json.emptyArr())
      resultArr <- Future.apply(selectNotNull(result))
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
         |idfolder,
         |created_at,
         |updated_at FROM $table""".stripMargin

    for {
      result <- connection.singleQuery(select, Json.emptyArr())
      resultArr <- Future.apply(selectNotNull(result))
    } yield {
      resultArr.map { row =>
        convertJsonArrayToFile(row)
      }
    }
  }

  def retrieveFromFolder(folder: FolderId): Future[Seq[File]] = {
    val select =
      s"""SELECT
         |uuid,
         |name,
         |description,
         |mime_type,
         |idfolder,
         |created_at,
         |updated_at FROM $table WHERE idfolder = ?""".stripMargin

    for {
      result <- connection.singleQuery(select, Json.arr(folder))
      resultArr <- Future.apply(getSeqOfJsonArray(result))
    } yield {
      resultArr.map { row =>
        convertJsonArrayToFile(row)
      }
    }
  }

  override def delete(o: File): Future[Unit] = {
    deleteById(o.uuid.get)
  }

  override def deleteById(id: UUID): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE uuid = ?"

    for {
      result <- connection.singleQuery(delete, Json.arr(id.toString))
      resultArr <- Future.apply(deleteNotNull(result))
    } yield ()
  }
}

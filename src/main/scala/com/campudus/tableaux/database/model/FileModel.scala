package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseHandler}
import com.campudus.tableaux.helper.ResultChecker._
import org.joda.time.DateTime
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
                              |mime_type,
                              |file_type,
                              |created_at,
                              |updated_at) VALUES (?,?,?,?,CURRENT_TIMESTAMP,NULL) RETURNING created_at""".stripMargin

    connection.transactional { t =>
      for {
        (t, result) <- t.query(insert, Json.arr(uuid.toString, o.name, o.mimeType, o.fileType))
      } yield {
        val inserted = insertNotNull(result).head

        val createdAt = DateTime.parse(inserted.get[String](0))

        (t, File(Some(uuid), o.name, o.mimeType, o.fileType, Some(createdAt), None))
      }
    }
  }

  override def retrieve(id: UUID): Future[File] = {
    val select =
      s"""SELECT
         |uuid,
         |name,
         |mime_type,
         |file_type,
         |created_at,
         |updated_at FROM $table WHERE uuid = ?""".stripMargin

    for {
      result <- connection.singleQuery(select, Json.arr(id.toString))
      resultArr <- Future.apply(selectNotNull(result))
    } yield {
      File(
        resultArr.head.get[String](0), //uuid
        resultArr.head.get[String](1), //name
        resultArr.head.get[String](2), //mime_type
        resultArr.head.get[String](3), //file_type
        resultArr.head.get[String](4), //created_at
        resultArr.head.get[String](5) //updated_at
      )
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
         |mime_type = ?,
         |file_type = ?,
         |updated_at = CURRENT_TIMESTAMP WHERE uuid = ? RETURNING created_at, updated_at""".stripMargin

    for {
      result <- connection.singleQuery(update, Json.arr(o.name, o.mimeType, o.fileType, o.uuid.get.toString))
      resultArr <- Future.apply(updateNotNull(result))
    } yield {
      File(
        o.uuid, //uuid
        o.name, //name
        o.mimeType, //mime_type
        o.fileType, //file_type
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
         |mime_type,
         |file_type,
         |created_at,
         |updated_at FROM $table""".stripMargin

    for {
      result <- connection.singleQuery(select, Json.emptyArr())
      resultArr <- Future.apply(selectNotNull(result))
    } yield {
      resultArr.map { row =>
        File(
          row.get[String](0), //uuid
          row.get[String](1), //name
          row.get[String](2), //mime_type
          row.get[String](3), //file_type
          row.get[String](4), //created_at
          row.get[String](5) //updated_at
        )
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

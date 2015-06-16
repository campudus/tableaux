package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database.{DatabaseQuery, DatabaseConnection}
import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future


case class Attachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID)

object AttachmentModel {
  def apply(connection: DatabaseConnection) = {
    new AttachmentModel(connection)
  }
}

class AttachmentModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val fileModel: FileModel = FileModel(connection)

  val table = "system_attachment"

  def add(a: Attachment): Future[File] = {
    val insert = s"INSERT INTO $table(table_id, column_id, row_id, attachment_uuid, ordering) VALUES(?, ?, ?, ?, ?)"

    for {
      _ <- connection.query(insert, Json.arr(a.tableId, a.columnId, a.rowId, a.uuid.toString, 1))
      file <- fileModel.retrieve(a.uuid)
    } yield file
  }

  def size(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ?"

    connection.selectSingleLong(select, Json.arr(tableId, columnId, rowId))
  }

  def retrieveAll(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Seq[File]] = {
    val select = s"SELECT attachment_uuid FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ? ORDER BY ordering"

    for {
      result <- connection.query(select, Json.arr(tableId, columnId, rowId))
      attachments <- Future(selectNotNull(result))
      files <- Future.sequence(attachments.map(a => fileModel.retrieve(UUID.fromString(a.get[String](0)))))
    } yield files
  }

  def delete(a: Attachment): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ? AND attachment_uuid = ?"

    for {
      result <- connection.query(delete, Json.arr(a.tableId, a.columnId, a.rowId, a.uuid))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }

  def retrieveFile(a: Attachment): Future[File] = {
    fileModel.retrieve(a.uuid)
  }
}

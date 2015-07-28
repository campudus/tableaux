package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database.domain.{DomainObject, ExtendedFile, File}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering, RowId, TableId}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

case class Attachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID, ordering: Option[Ordering])

case class AttachmentFile(file: ExtendedFile, ordering: Ordering) extends DomainObject {
  override def getJson: JsonObject = Json.obj("ordering" -> ordering).mergeIn(file.getJson)
}

object AttachmentModel {
  def apply(connection: DatabaseConnection) = {
    new AttachmentModel(connection)
  }
}

class AttachmentModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val fileModel: FileModel = FileModel(connection)

  val table = "system_attachment"

  def add(a: Attachment): Future[AttachmentFile] = {
    val insert = s"INSERT INTO $table(table_id, column_id, row_id, attachment_uuid, ordering) VALUES(?, ?, ?, ?, ?)"

    connection.transactional({ t =>
      for {
        (t, ordering) <- retrieveOrdering(t, a)
        (t, _) <- t.query(insert, Json.arr(a.tableId, a.columnId, a.rowId, a.uuid.toString, ordering))
        file <- retrieveFile(a.uuid, ordering)
      } yield (t, file)
    })
  }

  def update(a: Attachment): Future[AttachmentFile] = {
    val update = s"UPDATE $table SET ordering = ? WHERE table_id = ? AND column_id = ? AND row_id = ? AND attachment_uuid = ?"

    connection.transactional({ t =>
      for {
        (t, ordering: Ordering) <- retrieveOrdering(t, a)
        (t, _) <- t.query(update, Json.arr(ordering, a.tableId, a.columnId, a.rowId, a.uuid.toString))
        file <- retrieveFile(a.uuid, ordering)
      } yield (t, file)
    })
  }

  private def retrieveOrdering(t: connection.Transaction, a: Attachment): Future[(connection.Transaction, Ordering)] = {
    for {
      (t, ordering: Ordering) <- a.ordering match {
        case Some(i: Ordering) => Future((t, i: Ordering))
        case None => {
          for {
            (t, result) <- t.query(s"SELECT COALESCE(MAX(ordering),0) + 1 FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ?", Json.arr(a.tableId, a.columnId, a.rowId))
            resultArr <- Future(selectNotNull(result))
          } yield {
            (t, resultArr.head.get[Ordering](0))
          }
        }
      }
    } yield (t, ordering)
  }

  def size(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ?"

    connection.selectSingleValue(select, Json.arr(tableId, columnId, rowId))
  }

  def retrieveAll(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Seq[AttachmentFile]] = {
    val select = s"SELECT attachment_uuid, ordering FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ? ORDER BY ordering"

    for {
      result <- connection.query(select, Json.arr(tableId, columnId, rowId))
      attachments <- Future(getSeqOfJsonArray(result).map(e => (e.get[String](0), e.get[Ordering](1))))
      files <- Future.sequence(attachments.map(attachment => retrieveFile(UUID.fromString(attachment._1), attachment._2)))
    } yield files
  }

  def delete(a: Attachment): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE table_id = ? AND column_id = ? AND row_id = ? AND attachment_uuid = ?"

    for {
      result <- connection.query(delete, Json.arr(a.tableId, a.columnId, a.rowId, a.uuid.toString))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }

  def retrieveFile(file: UUID, ordering: Ordering): Future[AttachmentFile] = {
    fileModel.retrieve(file).map(ExtendedFile).map(f => AttachmentFile(f, ordering))
  }
}

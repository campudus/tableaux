package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.domain.Folder
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseHandler}
import com.campudus.tableaux.helper.ResultChecker._
import org.joda.time.DateTime
import org.vertx.scala.core.json.{JsonArray, Json}

import scala.concurrent.Future

object FolderModel {
  type FolderId = Long

  def apply(connection: DatabaseConnection): FolderModel = {
    new FolderModel(connection)
  }
}

class FolderModel(override protected[this] val connection: DatabaseConnection) extends DatabaseHandler[Folder, FolderId] {
  val table: String = "folder"

  override def add(o: Folder): Future[Folder] = {
    val insert =
      s"""INSERT INTO $table (
                              |name,
                              |description,
                              |idparent,
                              |created_at,
                              |updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP,NULL) RETURNING id, created_at""".stripMargin

    connection.transactional { t =>
      for {
        (t, result) <- t.query(insert, Json.arr(o.name, o.description, o.parent.orNull))
      } yield {
        val inserted = insertNotNull(result).head

        val id = inserted.get[Long](0)
        val createdAt = DateTime.parse(inserted.get[String](1))

        (t, Folder(Some(id), o.name, o.description, o.parent, Some(createdAt), None))
      }
    }
  }

  def convertJsonArrayToFolder(arr: JsonArray): Folder = {
    Folder(
      arr.get[FolderId](0), //id
      arr.get[String](1),   //name
      arr.get[String](2),   //description
      arr.get[FolderId](3), //idparent
      arr.get[String](4),   //created_at
      arr.get[String](5)    //updated_at
    )
  }

  override def update(o: Folder): Future[Folder] = {
    val update =
      s"""UPDATE $table SET
                         |name = ?,
                         |description = ?,
                         |idparent = ?,
                         |updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING created_at, updated_at""".stripMargin

    for {
      result <- connection.query(update, Json.arr(o.name, o.description, o.parent.orNull, o.id.get.toString))
      resultArr <- Future(updateNotNull(result))
    } yield {
      Folder(
        o.id,
        o.name,
        o.description,
        o.parent,
        resultArr.head.get[String](0), //created_at
        resultArr.head.get[String](1)  //updated_at
      )
    }
  }

  override def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table"

    connection.selectSingleValue(select)
  }

  override def retrieveAll(): Future[Seq[Folder]] = {
    val select =
      s"""SELECT
         |id,
         |name,
         |description,
         |idparent,
         |created_at,
         |updated_at FROM $table ORDER BY id""".stripMargin

    for {
      result <- connection.query(select)
      resultArr <- Future(getSeqOfJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  def retrieveSubfolders(folder: Option[FolderId]): Future[Seq[Folder]] = {
    def select(condition: String) =
      s"""SELECT
         |id,
         |name,
         |description,
         |idparent,
         |created_at,
         |updated_at FROM $table WHERE $condition ORDER BY id""".stripMargin

    for {
      result <- folder match {
        case None => connection.query(select("idparent IS NULL"))
        case Some(id) => connection.query(select("idparent = ?"), Json.arr(id.toString))
      }
      resultArr <- Future(getSeqOfJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToFolder)
    }
  }

  override def delete(o: Folder): Future[Unit] = {
    deleteById(o.id.get)
  }

  override def retrieve(id: FolderId): Future[Folder] = {
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

  override def deleteById(id: FolderId): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE id = ?"

    for {
      result <- connection.query(delete, Json.arr(id.toString))
      resultArr <- Future(deleteNotNull(result))
    } yield ()
  }
}
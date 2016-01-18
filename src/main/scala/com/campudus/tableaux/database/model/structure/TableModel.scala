package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database.domain.Table
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class TableModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(name: String, hidden: Boolean): Future[Table] = {
    connection.transactional { t =>
      for {
        (t, result) <- t.query("INSERT INTO system_table (user_table_name, is_hidden) VALUES (?, ?) RETURNING table_id", Json.arr(name, hidden))
        id <- Future(insertNotNull(result).head.get[TableId](0))
        (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, hidden BOOLEAN, PRIMARY KEY (id))")
        t <- createLanguageTable(t, id)
        (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id")
      } yield (t, Table(id, name, hidden))
    }
  }

  private def createLanguageTable(t: connection.Transaction, id: TableId): Future[connection.Transaction] = {
    for {
      (t, _) <- t.query(
        s"""
           | CREATE TABLE user_table_lang_$id (
           |   id BIGSERIAL,
           |   langtag VARCHAR(255),
           |
           |   PRIMARY KEY (id, langtag),
           |
           |   FOREIGN KEY(id)
           |   REFERENCES user_table_$id(id)
           |   ON DELETE CASCADE
           | )
         """.stripMargin)
    } yield t
  }

  def retrieveAll(): Future[Seq[Table]] = {
    for {
      result <- connection.query("SELECT table_id, user_table_name, is_hidden FROM system_table ORDER BY table_id")
    } yield {
      getSeqOfJsonArray(result).map { row =>
        Table(row.get[TableId](0), row.getString(1), row.getBoolean(2))
      }
    }
  }

  def retrieve(tableId: TableId): Future[Table] = {
    for {
      result <- connection.query("SELECT table_id, user_table_name, is_hidden FROM system_table WHERE table_id = ?", Json.arr(tableId))
    } yield {
      val json = selectNotNull(result).head
      Table(json.get[TableId](0), json.getString(1), json.getBoolean(2))
    }
  }

  def delete(tableId: TableId): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_lang_$tableId")
      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_$tableId")

      (t, result) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))

      _ <- Future(deleteNotNull(result)).recoverWith(t.rollbackAndFail())

      (t, _) <- t.query(s"DROP SEQUENCE system_columns_column_id_table_$tableId")

      _ <- t.commit()
    } yield ()
  }

  def change(tableId: TableId, name: String): Future[Unit] = {
    for {
      result <- connection.query(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId))
    } yield ()
  }
}

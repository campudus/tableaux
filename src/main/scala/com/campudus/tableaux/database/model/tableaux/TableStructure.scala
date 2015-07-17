package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class TableStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(name: String): Future[TableId] = {
    connection.transactional { t =>
      for {
        (t, result) <- t.query("INSERT INTO system_table (user_table_name) VALUES (?) RETURNING table_id", Json.arr(name))
        id <- Future(insertNotNull(result).head.get[TableId](0))
        (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))")
        t <- createLanguageTable(t, id)
        (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id")
      } yield (t, id)
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

  def retrieveAll(): Future[Seq[(TableId, String)]] = {
    connection.query("SELECT table_id, user_table_name FROM system_table")
  } map { r => getSeqOfJsonArray(r) map { arr => (arr.get[TableId](0), arr.get[String](1)) } }

  def retrieve(tableId: TableId): Future[(TableId, String)] = {
    connection.query("SELECT table_id, user_table_name FROM system_table WHERE table_id = ?", Json.arr(tableId))
  } map { r =>
    val json = selectNotNull(r).head
    (json.get[TableId](0), json.get[String](1))
  }

  def delete(tableId: TableId): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_$tableId")
    (t, result) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith { t.rollbackAndFail() }
    (t, _) <- t.query(s"DROP SEQUENCE system_columns_column_id_table_$tableId")
    _ <- t.commit()
  } yield ()

  def changeName(tableId: TableId, name: String): Future[Unit] = {
    connection.query(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId))
  } map (_ => ())
}

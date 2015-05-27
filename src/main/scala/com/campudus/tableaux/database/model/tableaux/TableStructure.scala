package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class TableStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(name: String): Future[IdType] = for {
    t <- connection.begin()
    (t, result) <- t.query("INSERT INTO system_table (user_table_name) VALUES (?) RETURNING table_id", Json.arr(name))
    id <- Future.apply(insertNotNull(result).head.get[Long](0))
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def getAll(): Future[Seq[(IdType, String)]] = {
    connection.singleQuery("SELECT table_id, user_table_name FROM system_table", Json.arr())
  } map { r => getSeqOfJsonArray(r) map { arr => (arr.get[IdType](0), arr.get[String](1)) } }

  def get(tableId: IdType): Future[(IdType, String)] = {
    connection.singleQuery("SELECT table_id, user_table_name FROM system_table WHERE table_id = ?", Json.arr(tableId))
  } map { r =>
    val json = selectNotNull(r).head
    (json.get[IdType](0), json.get[String](1))
  }

  def delete(tableId: IdType): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_$tableId", Json.arr())
    (t, result) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith { t.rollbackAndFail() }
    (t, _) <- t.query(s"DROP SEQUENCE system_columns_column_id_table_$tableId", Json.arr())
    _ <- t.commit()
  } yield ()

  def changeName(tableId: IdType, name: String): Future[Unit] = {
    connection.singleQuery(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId))
  } map (_ => ())
}

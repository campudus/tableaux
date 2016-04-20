package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database.domain.Table
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future

class TableModel(val connection: DatabaseConnection) extends DatabaseQuery {

  val systemModel = SystemModel(connection)

  def create(name: String, hidden: Boolean, langtags: Option[Option[Seq[String]]]): Future[Table] = {
    connection.transactional { t =>
      for {
        defaultLangtags <- retrieveGlobalLangtags()

        (t, result) <- t.query(s"INSERT INTO system_table (user_table_name, is_hidden, langtags) VALUES (?, ?, ?) RETURNING table_id", Json.arr(name, hidden, langtags.flatMap(_.map(f => Json.arr(f: _*))).orNull))
        id = insertNotNull(result).head.get[TableId](0)

        (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))")
        t <- createLanguageTable(t, id)
        (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id")
      } yield (t, Table(id, name, hidden, Option(langtags.flatten.getOrElse(defaultLangtags))))
    }
  }

  private def createLanguageTable(t: connection.Transaction, id: TableId): Future[connection.Transaction] = {
    for {
      (t, _) <- t.query(
        s"""
           | CREATE TABLE user_table_lang_$id (
           |   id BIGINT,
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

  private def retrieveGlobalLangtags(): Future[Seq[String]] = {
    // TODO don't really like dependend models
    systemModel.retrieveSetting(SystemController.SETTING_LANGTAGS)
      .map(f => Option(f).map(f => Json.fromArrayString(f).asScala.map(_.toString).toSeq).getOrElse(Seq.empty))
  }

  def retrieveAll(): Future[Seq[Table]] = {
    for {
      defaultLangtags <- retrieveGlobalLangtags()

      result <- connection.query("SELECT table_id, user_table_name, is_hidden, array_to_json(langtags) FROM system_table ORDER BY ordering, table_id")
    } yield {
      resultObjectToJsonArray(result).map(convertRowToTable(_, defaultLangtags))
    }
  }

  def retrieve(tableId: TableId): Future[Table] = {
    for {
      defaultLangtags <- retrieveGlobalLangtags()

      result <- connection.query("SELECT table_id, user_table_name, is_hidden, array_to_json(langtags) FROM system_table WHERE table_id = ?", Json.arr(tableId))
      row = selectNotNull(result).head
    } yield {
      convertRowToTable(row, defaultLangtags)
    }
  }

  private def convertRowToTable(row: JsonArray, defaultLangtags: Seq[String]): Table = {
    Table(
      row.getLong(0),
      row.getString(1),
      row.getBoolean(2),
      Option(Option(row.getString(3)).map(s => convertJsonArrayToSeq(Json.fromArrayString(s), { case f: String => f })).getOrElse(defaultLangtags))
    )
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

  def change(tableId: TableId, tableName: Option[String], hidden: Option[Boolean], langtags: Option[Option[Seq[String]]]): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, result1) <- optionToValidFuture(tableName, t, { name: String => t.query(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId)) })
      (t, result2) <- optionToValidFuture(hidden, t, { hidden: Boolean => t.query(s"UPDATE system_table SET is_hidden = ? WHERE table_id = ?", Json.arr(hidden, tableId)) })
      (t, result3) <- optionToValidFuture(langtags, t, { langtags: Option[Seq[String]] => t.query(s"UPDATE system_table SET langtags = ? WHERE table_id = ?", Json.arr(langtags.map(f => Json.arr(f: _*)).orNull, tableId)) })

      _ <- Future(checkUpdateResults(result1, result2, result3)) recoverWith t.rollbackAndFail()

      _ <- t.commit()
    } yield ()
  }

  def changeOrder(tableId: TableId, location: String, id: Option[Long]): Future[Unit] = {
    val listOfStatements: List[(String, JsonArray)] = location match {
      case "start" => List(
        (s"UPDATE system_table SET ordering = ordering + 1 WHERE ordering >= 1", Json.emptyArr()),
        (s"UPDATE system_table SET ordering = 1 WHERE table_id = ?", Json.arr(tableId))
      )
      case "end" => List(
        (s"UPDATE system_table SET ordering = ordering - 1 WHERE ordering >= (SELECT ordering FROM system_table WHERE table_id = ?)", Json.arr(tableId)),
        (s"UPDATE system_table SET ordering = (SELECT MAX(ordering) + 1 FROM system_table) WHERE table_id = ?", Json.arr(tableId))
      )
      case "before" => List(
        (s"UPDATE system_table SET ordering = (SELECT ordering FROM system_table WHERE table_id = ?) WHERE table_id = ?", Json.arr(id.get, tableId)),
        (s"UPDATE system_table SET ordering = ordering + 1 WHERE (ordering >= (SELECT ordering FROM system_table WHERE table_id = ?) AND table_id != ?)", Json.arr(id.get, tableId))
      )
    }

    for {
      t <- connection.begin()

      (t, results) <- listOfStatements.foldLeft(Future.successful((t, Vector[JsonObject]()))) {
        case (fTuple, (query, bindParams)) =>
          for {
            (latestTransaction, results) <- fTuple
            (lastT, result) <- latestTransaction.query(query, bindParams)
          } yield (lastT, results :+ result)
      }

      _ <- Future(checkUpdateResults(results: _*)) recoverWith t.rollbackAndFail()
      _ <- t.commit()
    } yield ()
  }
}

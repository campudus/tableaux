package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future

class TableModel(val connection: DatabaseConnection) extends DatabaseQuery {

  val systemModel = SystemModel(connection)

  def create(name: String, hidden: Boolean, langtags: Option[Option[Seq[String]]], displayInfos: Seq[DisplayInfo], tableType: TableType): Future[Table] = {
    connection.transactional { t =>
      for {
        (t, result) <- t.query(s"INSERT INTO system_table (user_table_name, is_hidden, langtags, type) VALUES (?, ?, ?, ?) RETURNING table_id", Json.arr(name, hidden, langtags.flatMap(_.map(f => Json.arr(f: _*))).orNull, tableType.NAME))
        id = insertNotNull(result).head.get[TableId](0)

        (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))")
        t <- createLanguageTable(t, id)
        (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id")

        (t, _) <- createTableDisplayInfos(t, DisplayInfos(id, displayInfos))

        defaultLangtags <- retrieveGlobalLangtags()
      } yield (t, Table(id, name, hidden, Option(langtags.flatten.getOrElse(defaultLangtags)), displayInfos, tableType))
    }
  }

  private def createTableDisplayInfos(t: connection.Transaction, displayInfos: TableDisplayInfos): Future[(connection.Transaction, JsonObject)] = {
    if (displayInfos.nonEmpty) {
      val (statement, binds) = displayInfos.createSql
      for {
        (t, result) <- t.query(statement, Json.arr(binds: _*))
      } yield (t, result)
    } else {
      Future.successful((t, Json.obj()))
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
      tables <- getTablesWithDisplayInfos(defaultLangtags)
    } yield tables
  }

  def retrieve(tableId: TableId): Future[Table] = {
    for {
      defaultLangtags <- retrieveGlobalLangtags()
      table <- getTableWithDisplayInfos(tableId, defaultLangtags)
    } yield table
  }

  private def getTableWithDisplayInfos(tableId: TableId, defaultLangtags: Seq[String]): Future[Table] = {
    connection.transactional { t: connection.Transaction =>
      for {
        (t, result) <- t.query("SELECT table_id, user_table_name, is_hidden, array_to_json(langtags), type FROM system_table WHERE table_id = ?", Json.arr(tableId))
        row = selectNotNull(result).head
        table = convertRowToTable(row, defaultLangtags)
        (t, result) <- t.query("SELECT table_id, langtag, name, description FROM system_table_lang")
      } yield {
        val displayInfoTable = resultObjectToJsonArray(result)
          .groupBy(_.getLong(0).toLong)
          .mapValues(
            _.filter(arr => arr.getString(2) != null || arr.getString(3) != null)
              .map(arr => DisplayInfos.fromString(arr.getString(1), arr.getString(2), arr.getString(3)))
          )

        val filledTable = table.copy(displayInfos = displayInfoTable.get(table.id).toList.flatten)
        (t, filledTable)
      }
    }
  }

  private def getTablesWithDisplayInfos(defaultLangtags: Seq[String]): Future[Seq[Table]] = {
    connection.transactional { t =>
      for {
        (t, result) <- t.query("SELECT table_id, user_table_name, is_hidden, array_to_json(langtags), type FROM system_table ORDER BY ordering, table_id")
        tablesInRows = resultObjectToJsonArray(result)
        tableRows = tablesInRows.map(convertRowToTable(_, defaultLangtags))
        (t, result) <- t.query("SELECT table_id, langtag, name, description FROM system_table_lang")
      } yield {
        val displayInfoTable = resultObjectToJsonArray(result)
          .groupBy(_.getLong(0).toLong)
          .mapValues(
            _.filter(arr => arr.getString(2) != null || arr.getString(3) != null)
              .map(arr => DisplayInfos.fromString(arr.getString(1), arr.getString(2), arr.getString(3)))
          )
        val filledTables = tableRows.map(table => table.copy(displayInfos = displayInfoTable.get(table.id).toList.flatten))
        (t, filledTables)
      }
    }
  }

  private def convertRowToTable(row: JsonArray, defaultLangtags: Seq[String]): Table = {
    Table(
      row.getLong(0),
      row.getString(1),
      row.getBoolean(2),
      Option(Option(row.getString(3)).map(s => convertJsonArrayToSeq(Json.fromArrayString(s), { case f: String => f })).getOrElse(defaultLangtags)),
      List(),
      TableType(row.getString(4))
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

  def change(tableId: TableId, tableName: Option[String], hidden: Option[Boolean], langtags: Option[Option[Seq[String]]], displayInfos: Option[Seq[DisplayInfo]]): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, result1) <- optionToValidFuture(tableName, t, { name: String => t.query(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId)) })
      (t, result2) <- optionToValidFuture(hidden, t, { hidden: Boolean => t.query(s"UPDATE system_table SET is_hidden = ? WHERE table_id = ?", Json.arr(hidden, tableId)) })
      (t, result3) <- optionToValidFuture(langtags, t, { langtags: Option[Seq[String]] => t.query(s"UPDATE system_table SET langtags = ? WHERE table_id = ?", Json.arr(langtags.map(f => Json.arr(f: _*)).orNull, tableId)) })
      t <- insertOrUpdateTableDisplayInfo(t, tableId, displayInfos)

      _ <- Future(checkUpdateResults(result1, result2, result3)) recoverWith t.rollbackAndFail()

      _ <- t.commit()
    } yield ()
  }

  private def insertOrUpdateTableDisplayInfo(t: connection.Transaction, tableId: TableId, optDisplayInfos: Option[Seq[DisplayInfo]]): Future[connection.Transaction] = {

    optDisplayInfos match {
      case Some(displayInfos) =>
        val dis = DisplayInfos(tableId, displayInfos)
        dis.entries.foldLeft(Future.successful(t)) {
          case (future, di) =>
            for {
              t <- future
              (t, select) <- t.query("SELECT COUNT(*) FROM system_table_lang WHERE table_id = ? AND langtag = ?", Json.arr(tableId, di.langtag))
              count = select.getJsonArray("results").getJsonArray(0).getLong(0)
              (statement, binds) = if (count > 0) {
                dis.updateSql(di.langtag)
              } else {
                dis.insertSql(di.langtag)
              }
              (t, _) <- t.query(statement, Json.arr(binds: _*))
            } yield t
        }
      case None => Future.successful(t)
    }
  }

  def changeOrder(tableId: TableId, locationType: LocationType): Future[Unit] = {
    val listOfStatements: List[(String, JsonArray)] = locationType match {
      case LocationStart => List(
        (s"UPDATE system_table SET ordering = ordering + 1 WHERE ordering >= 1", Json.emptyArr()),
        (s"UPDATE system_table SET ordering = 1 WHERE table_id = ?", Json.arr(tableId))
      )
      case LocationEnd => List(
        (s"UPDATE system_table SET ordering = ordering - 1 WHERE ordering >= (SELECT ordering FROM system_table WHERE table_id = ?)", Json.arr(tableId)),
        (s"UPDATE system_table SET ordering = (SELECT MAX(ordering) + 1 FROM system_table) WHERE table_id = ?", Json.arr(tableId))
      )
      case LocationBefore(relativeTo) => List(
        (s"UPDATE system_table SET ordering = (SELECT ordering FROM system_table WHERE table_id = ?) WHERE table_id = ?", Json.arr(relativeTo, tableId)),
        (s"UPDATE system_table SET ordering = ordering + 1 WHERE (ordering >= (SELECT ordering FROM system_table WHERE table_id = ?) AND table_id != ?)", Json.arr(relativeTo, tableId))
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

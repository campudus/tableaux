package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{ComparisonObjects, RoleModel, ScopeTable, View}
import com.campudus.tableaux.{RequestContext, ShouldBeUniqueException}
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future

class TableModel(val connection: DatabaseConnection)(
    implicit requestContext: RequestContext,
    roleModel: RoleModel
) extends DatabaseQuery {

  val systemModel = SystemModel(connection)
  val tableGroupModel = TableGroupModel(connection)

  def create(
      name: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableType: TableType,
      tableGroupIdOpt: Option[TableGroupId],
      attributes: Option[JsonObject]
  ): Future[Table] = {
    connection.transactional { t =>
      {
        for {
          t <- t
            .selectSingleValue[Long]("SELECT COUNT(*) FROM system_table WHERE user_table_name = ?", Json.arr(name))
            .flatMap({
              case (t, count) =>
                if (count > 0) {
                  Future.failed(ShouldBeUniqueException("Table name should be unique", "table"))
                } else {
                  Future.successful(t)
                }
            })

          (t, result) <- t
            .query(
              s"INSERT INTO system_table (user_table_name, is_hidden, langtags, type, group_id, attributes) VALUES (?, ?, ?, ?, ?, ?) RETURNING table_id",
              Json
                .arr(
                  name,
                  hidden,
                  langtags.flatMap(_.map(f => Json.arr(f: _*))).orNull,
                  tableType.NAME,
                  tableGroupIdOpt.orNull,
                  attributes match {
                    case Some(obj) => obj.encode()
                    case None => "{}"
                  }
                )
            )
          id = insertNotNull(result).head.get[TableId](0)

          (t, _) <- t.query(
            s"CREATE TABLE user_table_$id (id BIGSERIAL, final BOOLEAN DEFAULT false, replaced_ids jsonb DEFAULT NULL, PRIMARY KEY (id))"
          )
          t <- createLanguageTable(t, id)
          t <- createCellAnnotationsTable(t, id)
          t <- createHistoryTable(t, id)
          (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id")

          (t, _) <- createTableDisplayInfos(t, TableDisplayInfos(id, displayInfos))

          tableGroup <- tableGroupIdOpt match {
            case Some(tableGroupId) =>
              tableGroupModel
                .retrieve(tableGroupId)
                .map(Some(_))
            case None =>
              Future.successful(None)
          }

          defaultLangtags <- retrieveGlobalLangtags()
        } yield (
          t,
          Table(
            id,
            name,
            hidden,
            Option(langtags.flatten.getOrElse(defaultLangtags)),
            displayInfos,
            tableType,
            tableGroup,
            attributes
          )
        )
      }
    }
  }

  private def createTableDisplayInfos(
      t: DbTransaction,
      displayInfos: TableDisplayInfos
  ): Future[(DbTransaction, JsonObject)] = {
    if (displayInfos.nonEmpty) {
      val (statement, binds) = displayInfos.createSql
      for {
        (t, result) <- t.query(statement, Json.arr(binds: _*))
      } yield (t, result)
    } else {
      Future.successful((t, Json.obj()))
    }
  }

  private def createLanguageTable(t: DbTransaction, id: TableId): Future[DbTransaction] = {
    for {
      (t, _) <- t.query(s"""
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

  private def createCellAnnotationsTable(t: DbTransaction, id: TableId): Future[DbTransaction] = {
    for {
      (t, _) <- t.query(s"""
                           | CREATE TABLE user_table_annotations_$id (
                           |   row_id BIGINT NOT NULL,
                           |   column_id BIGINT NOT NULL,
                           |   uuid UUID NOT NULL,
                           |   langtags TEXT[] NOT NULL DEFAULT '{}'::text[],
                           |   type VARCHAR(255) NOT NULL,
                           |   value TEXT NULL,
                           |   created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                           |
                           |   PRIMARY KEY (row_id, column_id, uuid),
                           |   FOREIGN KEY (row_id) REFERENCES user_table_$id (id) ON DELETE CASCADE
                           | )
         """.stripMargin)
    } yield t
  }

  private def createHistoryTable(t: DbTransaction, id: TableId): Future[DbTransaction] = {
    for {
      (t, _) <- t.query(s"""
                           | CREATE TABLE user_table_history_$id (
                           |   revision BIGSERIAL,
                           |   row_id BIGINT NOT NULL,
                           |   column_id BIGINT,
                           |   event VARCHAR(255) NOT NULL DEFAULT 'cell_changed',
                           |   history_type VARCHAR(255),
                           |   value_type VARCHAR(255),
                           |   language_type VARCHAR(255) DEFAULT 'neutral',
                           |   author VARCHAR(255),
                           |   timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                           |   value JSON NULL,
                           |   PRIMARY KEY (revision)
                           | )
           """.stripMargin)
    } yield t
  }

  def retrieveGlobalLangtags(): Future[Seq[String]] = {
    systemModel
      .retrieveSetting(SystemController.SETTING_LANGTAGS)
      .map(valueOpt =>
        valueOpt.map(value => Json.fromArrayString(value).asScala.map(_.toString).toSeq).getOrElse(Seq.empty)
      )
  }

  def retrieveAll(isInternalCall: Boolean): Future[Seq[Table]] = {
    for {
      defaultLangtags <- retrieveGlobalLangtags()
      tables <- getTablesWithDisplayInfos(defaultLangtags)
      filteredTables: Seq[Table] = roleModel
        .filterDomainObjects[Table](ScopeTable, tables, isInternalCall = isInternalCall)
    } yield filteredTables
  }

  def retrieve(tableId: TableId, isInternalCall: Boolean = false): Future[Table] = {
    for {
      defaultLangtags <- retrieveGlobalLangtags()
      table <- getTableWithDisplayInfos(tableId, defaultLangtags)
      _ <- roleModel.checkAuthorization(View, ScopeTable, ComparisonObjects(table), isInternalCall)
    } yield table
  }

  private def getTableWithDisplayInfos(tableId: TableId, defaultLangtags: Seq[String]): Future[Table] = {
    for {
      t <- connection.begin()

      (t, tableResult) <- t.query(
        "SELECT table_id, user_table_name, is_hidden, array_to_json(langtags), type, group_id, attributes FROM system_table WHERE table_id = ?",
        Json.arr(tableId)
      )
      (t, displayInfoResult) <- t.query(
        "SELECT table_id, langtag, name, description FROM system_table_lang WHERE table_id = ?",
        Json.arr(tableId)
      )

      _ <- t.commit()

      row = selectNotNull(tableResult).head

      tableGroups: Map[TableGroupId, TableGroup] <- Option(row.getLong(5)).map(_.longValue()) match {
        case Some(tableGroupId) =>
          tableGroupModel.retrieve(tableGroupId).map(tableGroup => Map(tableGroupId -> tableGroup))
        case None =>
          Future.successful(Map.empty[TableGroupId, TableGroup])
      }
    } yield {
      val table = convertRowToTable(row, defaultLangtags, tableGroups)
      mapDisplayInfosIntoTable(Seq(table), displayInfoResult).head
    }
  }

  private def getTablesWithDisplayInfos(defaultLangtags: Seq[String]): Future[Seq[Table]] = {
    for {
      t <- connection.begin()

      (t, tablesResult) <- t.query(
        "SELECT table_id, user_table_name, is_hidden, array_to_json(langtags), type, group_id, attributes FROM system_table ORDER BY ordering, table_id"
      )
      (t, displayInfosResult) <- t.query("SELECT table_id, langtag, name, description FROM system_table_lang")

      _ <- t.commit()

      tableGroups <- tableGroupModel
        .retrieveAll()
        .map({ tableGroups =>
          {
            tableGroups
              .map({ tableGroup =>
                {
                  (tableGroup.id, tableGroup)
                }
              })
              .toMap
          }
        })
    } yield {
      val tables = resultObjectToJsonArray(tablesResult).map(convertRowToTable(_, defaultLangtags, tableGroups))
      mapDisplayInfosIntoTable(tables, displayInfosResult)
    }
  }

  private def mapDisplayInfosIntoTable(tables: Seq[Table], result: JsonObject): Seq[Table] = {
    val displayInfoTable = resultObjectToJsonArray(result)
      .groupBy(_.getLong(0).longValue())
      .mapValues(
        _.filter(arr => Option(arr.getString(2)).isDefined || Option(arr.getString(3)).isDefined)
          .map(arr => DisplayInfos.fromString(arr.getString(1), arr.getString(2), arr.getString(3)))
      )

    tables.map({ table =>
      {
        table.copy(displayInfos = displayInfoTable.get(table.id).toList.flatten)
      }
    })
  }

  private def convertRowToTable(
      row: JsonArray,
      defaultLangtags: Seq[String],
      tableGroups: Map[TableGroupId, TableGroup]
  ): Table = {
    Table(
      row.getLong(0),
      row.getString(1),
      row.getBoolean(2),
      Option(
        Option(row.getString(3))
          .map(s => convertJsonArrayToSeq(Json.fromArrayString(s), { case f: String => f }))
          .getOrElse(defaultLangtags)
      ),
      List(),
      TableType(row.getString(4)),
      Option(row.getLong(5)).map(_.longValue()).flatMap(tableGroups.get),
      Option(row.getString(6)).map(jsonString => new JsonObject(jsonString))
    )
  }

  def delete(tableId: TableId): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_annotations_$tableId")
      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_lang_$tableId")
      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_$tableId")
      (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_history_$tableId")

      (t, result) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))

      _ <- Future(deleteNotNull(result)).recoverWith(t.rollbackAndFail())

      (t, _) <- t.query(s"DROP SEQUENCE system_columns_column_id_table_$tableId")

      _ <- t.commit()
    } yield ()
  }

  def change(
      tableId: TableId,
      tableName: Option[String],
      hidden: Option[Boolean],
      langtags: Option[Option[Seq[String]]],
      displayInfos: Option[Seq[DisplayInfo]],
      tableGroupId: Option[Option[TableGroupId]],
      attributes: Option[JsonObject]
  ): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, result1) <- optionToValidFuture(
        tableName,
        t,
        { name: String =>
          {
            for {
              t <- t
                .selectSingleValue[Long](
                  "SELECT COUNT(*) FROM system_table WHERE user_table_name = ? AND table_id != ?",
                  Json.arr(name, tableId)
                )
                .flatMap({
                  case (t, count) =>
                    if (count > 0) {
                      Future.failed(ShouldBeUniqueException("Table name should be unique", "table"))
                    } else {
                      Future.successful(t)
                    }
                })
              (t, result) <-
                t.query(s"UPDATE system_table SET user_table_name = ? WHERE table_id = ?", Json.arr(name, tableId))
            } yield (t, result)
          }
        }
      )
      (t, result2) <- optionToValidFuture(
        hidden,
        t,
        { hidden: Boolean =>
          {
            t.query(s"UPDATE system_table SET is_hidden = ? WHERE table_id = ?", Json.arr(hidden, tableId))
          }
        }
      )
      (t, result3) <- optionToValidFuture(
        langtags,
        t,
        { langtags: Option[Seq[String]] =>
          {
            t.query(
              s"UPDATE system_table SET langtags = ? WHERE table_id = ?",
              Json.arr(langtags.map(f => Json.arr(f: _*)).orNull, tableId)
            )
          }
        }
      )
      (t, result4) <- optionToValidFuture(
        tableGroupId,
        t,
        { tableGroupId: Option[TableGroupId] =>
          {
            t.query(s"UPDATE system_table SET group_id = ? WHERE table_id = ?", Json.arr(tableGroupId.orNull, tableId))
          }
        }
      )
      (t, result5) <- optionToValidFuture(
        attributes,
        t,
        { attributes: JsonObject =>
          {
            t.query(
              s"UPDATE system_table SET attributes = ? WHERE table_id = ?",
              Json.arr(attributes.encode(), tableId)
            )
          }
        }
      )

      t <- insertOrUpdateTableDisplayInfo(t, tableId, displayInfos)

      _ <- Future(checkUpdateResults(result1, result2, result3, result4, result5)) recoverWith t.rollbackAndFail()

      _ <- t.commit()
    } yield ()
  }

  private def insertOrUpdateTableDisplayInfo(
      t: DbTransaction,
      tableId: TableId,
      optDisplayInfos: Option[Seq[DisplayInfo]]
  ): Future[DbTransaction] = {
    optDisplayInfos match {
      case Some(displayInfos) =>
        val dis = TableDisplayInfos(tableId, displayInfos)
        dis.entries.foldLeft(Future.successful(t)) {
          case (future, di) =>
            for {
              t <- future
              (t, select) <- t.query(
                "SELECT COUNT(*) FROM system_table_lang WHERE table_id = ? AND langtag = ?",
                Json.arr(tableId, di.langtag)
              )
              count = select.getJsonArray("results").getJsonArray(0).getLong(0)
              (statement, binds) =
                if (count > 0) {
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
      case LocationStart =>
        List(
          (s"UPDATE system_table SET ordering = ordering + 1 WHERE ordering >= 1", Json.emptyArr()),
          (s"UPDATE system_table SET ordering = 1 WHERE table_id = ?", Json.arr(tableId))
        )
      case LocationEnd =>
        List(
          (
            s"UPDATE system_table SET ordering = ordering - 1 WHERE ordering >= (SELECT ordering FROM system_table WHERE table_id = ?)",
            Json.arr(tableId)
          ),
          (
            s"UPDATE system_table SET ordering = (SELECT MAX(ordering) + 1 FROM system_table) WHERE table_id = ?",
            Json.arr(tableId)
          )
        )
      case LocationBefore(relativeTo) =>
        List(
          (
            s"UPDATE system_table SET ordering = (SELECT ordering FROM system_table WHERE table_id = ?) WHERE table_id = ?",
            Json.arr(relativeTo, tableId)
          ),
          (
            s"UPDATE system_table SET ordering = ordering + 1 WHERE (ordering >= (SELECT ordering FROM system_table WHERE table_id = ?) AND table_id != ?)",
            Json.arr(relativeTo, tableId)
          )
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

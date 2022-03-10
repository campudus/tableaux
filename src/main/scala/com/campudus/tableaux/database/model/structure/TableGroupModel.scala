package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

object TableGroupModel {

  def apply(connection: DatabaseConnection): TableGroupModel = {
    new TableGroupModel(connection)
  }
}

class TableGroupModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(displayInfos: Seq[DisplayInfo]): Future[TableGroup] = {
    connection.transactional { t =>
      for {
        (t, result) <- t.query(s"INSERT INTO system_tablegroup(id) VALUES(DEFAULT) RETURNING id")
        id = insertNotNull(result).head.get[TableGroupId](0)

        (t, _) <- createTableDisplayInfos(t, id, displayInfos)
      } yield (t, TableGroup(id, displayInfos))
    }
  }

  private def createTableDisplayInfos(
      t: connection.Transaction,
      tableGroupId: TableGroupId,
      displayInfos: Seq[DisplayInfo]
  ): Future[(connection.Transaction, JsonObject)] = {
    if (displayInfos.nonEmpty) {
      val (statement, binds) = TableGroupDisplayInfos(tableGroupId, displayInfos).createSql

      for {
        (t, result) <- t.query(statement, Json.arr(binds: _*))
      } yield (t, result)
    } else {
      Future.successful((t, Json.obj()))
    }
  }

  def retrieve(id: TableGroupId): Future[TableGroup] = {
    for {
      table <- retrieveWithDisplayInfos(id)
    } yield table
  }

  def retrieveAll(): Future[Seq[TableGroup]] = {
    for {
      table <- retrieveAllWithDisplayInfos()
    } yield table
  }

  private def retrieveWithDisplayInfos(id: TableGroupId): Future[TableGroup] = {
    for {
      displayInfoResult <- connection
        .query("SELECT id, langtag, name, description FROM system_tablegroup_lang WHERE id = ?", Json.arr(id))
      _ = selectNotNull(displayInfoResult)
    } yield {
      mapDisplayInfosIntoTableGroup(displayInfoResult).head
    }
  }

  private def retrieveAllWithDisplayInfos(): Future[Seq[TableGroup]] = {
    for {
      displayInfoResult <- connection.query("SELECT id, langtag, name, description FROM system_tablegroup_lang")
    } yield {
      mapDisplayInfosIntoTableGroup(displayInfoResult)
    }
  }

  private def mapDisplayInfosIntoTableGroup(result: JsonObject): Seq[TableGroup] = {
    val displayInfoTable = resultObjectToJsonArray(result)
      .groupBy(_.getLong(0).longValue())
      .mapValues(
        _.filter(arr => Option(arr.getString(2)).isDefined || Option(arr.getString(3)).isDefined)
          .map(arr => DisplayInfos.fromString(arr.getString(1), arr.getString(2), arr.getString(3)))
      )

    displayInfoTable
      .map({
        case (id, displayInfos) =>
          TableGroup(id, displayInfos)
      })
      .toList
  }

  def delete(tableGroupId: TableGroupId): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, result) <- t.query("DELETE FROM system_tablegroup WHERE id = ?", Json.arr(tableGroupId))
      _ = deleteNotNull(result)

      _ <- t.commit()
    } yield ()
  }

  def change(tableGroupId: TableGroupId, displayInfos: Option[Seq[DisplayInfo]]): Future[Unit] = {
    for {
      t <- connection.begin()

      t <- insertOrUpdateTableDisplayInfo(t, tableGroupId, displayInfos)

      _ <- t.commit()
    } yield ()
  }

  private def insertOrUpdateTableDisplayInfo(
      t: connection.Transaction,
      tableGroupId: TableGroupId,
      optDisplayInfos: Option[Seq[DisplayInfo]]
  ): Future[connection.Transaction] = {
    optDisplayInfos match {
      case Some(displayInfos) =>
        val dis = TableGroupDisplayInfos(tableGroupId, displayInfos)
        dis.entries.foldLeft(Future.successful(t)) {
          case (future, di) =>
            for {
              t <- future
              (t, select) <- t.query(
                "SELECT COUNT(*) FROM system_tablegroup_lang WHERE id = ? AND langtag = ?",
                Json.arr(tableGroupId, di.langtag)
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
}

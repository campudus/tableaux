package com.campudus.tableaux.database.model

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.database.domain.{DependentRowsSeq, Folder, MultiLanguageValue, TableauxFile}
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.helper.ResultChecker._

import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future

import java.util.UUID

object FileModel {

  def apply(connection: DatabaseConnection): FileModel = {
    new FileModel(connection)
  }
}

class FileModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val table: String = "file"

  /**
    * Will add a new entity marked as temporary!
    */
  def add(
      title: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      externalName: MultiLanguageValue[String],
      folder: Option[FolderId]
  ): Future[TableauxFile] = {
    val uuid = UUID.randomUUID()

    val insert =
      s"""INSERT INTO $table (
         |uuid,
         |idfolder,
         |tmp) VALUES (?,?,true) RETURNING created_at""".stripMargin

    val b = MultiLanguageValue.merge(
      Map(
        "title" -> title.values,
        "description" -> description.values,
        "external_name" -> externalName.values
      )
    )

    for {
      resultJson <- connection.query(insert, Json.arr(uuid.toString, folder.orNull))
      resultRow = insertNotNull(resultJson).head

      _ <- addTranslations(uuid, b)

      file <- retrieve(uuid, withTmp = true)
    } yield {
      file
    }
  }

  private def addTranslations(uuid: UUID, map: Map[String, Map[String, _]]): Future[Seq[JsonObject]] = {
    Future.sequence(map.foldLeft(Seq.empty[Future[JsonObject]]) {
      case (result, (langtag, columnsValueMap)) =>
        val columns = columnsValueMap.keySet.toSeq
        val values = columnsValueMap.values.toSeq

        val placeholder = columns.map(_ => "?").mkString(", ")

        val select = s"SELECT COUNT(uuid) FROM file_lang WHERE uuid = ? AND langtag = ?"
        val insert = s"INSERT INTO file_lang(${columns.mkString(",")}, uuid, langtag) VALUES($placeholder, ?, ?)"

        val update =
          s"UPDATE file_lang SET ${columns.map(column => s"$column = ?").mkString(", ")} WHERE uuid = ? AND langtag = ?"

        val binds = Json.arr(values: _*).add(uuid.toString).add(langtag)

        val future = connection.transactional({
          case t =>
            for {
              (t, count) <- t
                .query(select, Json.arr(uuid.toString, langtag))
                .map({
                  case (t, json) =>
                    (t, selectNotNull(json).head.get[Long](0))
                })
              (t, result) <-
                if (count > 0) {
                  t.query(update, binds)
                } else {
                  t.query(insert, binds)
                }
            } yield (t, result)
        })

        result :+ future
    })
  }

  private def jsonObjectAgg(tableAlias: String, column: String, languageColumn: String = "langtag"): String = {
    s"CASE WHEN COUNT(fl.uuid) = 0 THEN NULL ELSE json_object_agg(DISTINCT COALESCE($tableAlias.$languageColumn,'IGNORE'), $tableAlias.$column) FILTER (WHERE $tableAlias.$column IS NOT NULL) END as $column"
  }

  private def select(where: String): String = {
    s"""SELECT
       | f.uuid,
       | f.idfolder,
       | (
       |   WITH RECURSIVE prev AS (
       |     SELECT folder.id, ARRAY[]::bigint[] AS parents, FALSE AS cycle
       |     FROM folder WHERE folder.idparent IS NULL
       |     UNION ALL
       |     SELECT folder.id, parents || folder.idparent AS parents, folder.id = ANY(parents) AS cycle
       |     FROM prev INNER JOIN folder ON (prev.id = idparent AND prev.cycle = FALSE)
       |   )
       |   SELECT ARRAY_TO_JSON(parents || f.idfolder) FROM prev WHERE cycle IS FALSE AND id = f.idfolder
       | ) AS folders,
       | f.created_at,
       | f.updated_at,
       | ${jsonObjectAgg("fl", "title")},
       | ${jsonObjectAgg("fl", "description")},
       | ${jsonObjectAgg("fl", "internal_name")},
       | ${jsonObjectAgg("fl", "external_name")},
       | ${jsonObjectAgg("fl", "mime_type")},
       | COUNT(sa.*) as dependent_count
       |FROM file f
       |LEFT JOIN file_lang fl ON (f.uuid = fl.uuid)
       |LEFT JOIN system_attachment sa ON (f.uuid = sa.attachment_uuid)
       |WHERE $where
       |GROUP BY f.uuid""".stripMargin
  }

  private def selectOrdered(where: String, langtag: String): String = {
    s"""|SELECT
        | s.*
        |FROM (
        | ${select(where)}
        |) s
        |ORDER BY s.external_name->>'$langtag' ASC NULLS FIRST""".stripMargin
  }

  def retrieve(id: UUID): Future[TableauxFile] = retrieve(id, withTmp = false)

  def retrieve(id: UUID, withTmp: Boolean): Future[TableauxFile] = {
    for {
      resultJson <-
        if (withTmp) {
          connection.query(select("f.uuid = ?"), Json.arr(id.toString))
        } else {
          connection.query(select("f.uuid = ? AND tmp = FALSE"), Json.arr(id.toString))
        }
      resultRow = selectNotNull(resultJson).head
    } yield {
      convertRowToFile(resultRow)
    }
  }

  def retrieveAll(): Future[Seq[TableauxFile]] = {
    for {
      resultJson <- connection.query(select("f.tmp = FALSE"))
      resultRows = selectNotNull(resultJson)
    } yield {
      resultRows.map(convertRowToFile)
    }
  }

  def retrieveFromFolder(folder: Folder): Future[Seq[TableauxFile]] = {
    for {
      resultJson <- folder match {
        case MediaController.ROOT_FOLDER =>
          connection.query(select("f.idfolder IS NULL AND f.tmp = FALSE"))
        case f =>
          connection.query(select("f.idfolder = ? AND f.tmp = FALSE"), Json.arr(f.id))
      }

      resultRows = resultObjectToJsonArray(resultJson)
    } yield {
      resultRows.map(convertRowToFile)
    }
  }

  def retrieveFromFolder(folder: Folder, sortByLangtag: String): Future[Seq[TableauxFile]] = {
    for {
      resultJson <- folder match {
        case MediaController.ROOT_FOLDER =>
          connection.query(selectOrdered("f.idfolder IS NULL AND f.tmp = FALSE", sortByLangtag))
        case f =>
          connection.query(selectOrdered("f.idfolder = ? AND f.tmp = FALSE", sortByLangtag), Json.arr(f.id))
      }

      resultRows = resultObjectToJsonArray(resultJson)
    } yield {
      resultRows.map(convertRowToFile)
    }
  }

  private def convertRowToFile(row: JsonArray): TableauxFile = {
    import scala.collection.JavaConverters._

    val folders: Seq[Long] = Option(row.getString(2)) match {
      case None => Seq.empty[Long]
      case Some(_) =>
        Json
          .fromArrayString(row.getString(2))
          .asScala
          .toSeq
          .map({
            case f: java.lang.Integer => f.longValue()
          })
    }

    TableauxFile(
      UUID.fromString(row.get[String](0)), // uuid
      folders, // folders
      MultiLanguageValue.fromString(row.get[String](5)), // title
      MultiLanguageValue.fromString(row.get[String](6)), // description
      MultiLanguageValue.fromString(row.get[String](7)), // internal_name
      MultiLanguageValue.fromString(row.get[String](8)), // external_name
      MultiLanguageValue.fromString(row.get[String](9)), // mime_type
      convertStringToDateTime(row.get[String](3)), // created_at
      convertStringToDateTime(row.get[String](4)), // updated_at
      row.getInteger(10) // dependent_row_count
    )
  }

  def update(
      uuid: UUID,
      title: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      internalName: MultiLanguageValue[String],
      externalName: MultiLanguageValue[String],
      folder: Option[FolderId],
      mimeType: MultiLanguageValue[String]
  ): Future[TableauxFile] = {
    val update =
      s"""UPDATE $table SET
         |idfolder = ?,
         |updated_at = CURRENT_TIMESTAMP,
         |tmp = FALSE
         |WHERE uuid = ?""".stripMargin

    val b = MultiLanguageValue.merge(
      Map(
        "title" -> title.values,
        "description" -> description.values,
        "internal_name" -> internalName.values,
        "external_name" -> externalName.values,
        "mime_type" -> mimeType.values
      )
    )

    for {
      resultJson <- connection.query(update, Json.arr(folder.orNull, uuid.toString))
      _ = updateNotNull(resultJson)

      _ <- addTranslations(uuid, b)

      file <- retrieve(uuid)
    } yield file
  }

  def size(): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM $table WHERE tmp = FALSE"

    connection.selectSingleValue(select)
  }

  def deleteById(id: UUID): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE uuid = ?"

    for {
      resultJson <- connection.query(delete, Json.arr(id.toString))
      _ <- Future(deleteNotNull(resultJson))
    } yield ()
  }

  def deleteByIdAndLangtag(id: UUID, langtag: String): Future[Unit] = {
    val delete = s"DELETE FROM file_lang WHERE uuid = ? AND langtag = ?"

    for {
      resultJson <- connection.query(delete, Json.arr(id.toString, langtag))
      _ <- Future(deleteNotNull(resultJson))
    } yield ()
  }
}

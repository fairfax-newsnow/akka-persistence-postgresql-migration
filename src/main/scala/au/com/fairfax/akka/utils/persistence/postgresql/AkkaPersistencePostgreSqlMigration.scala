package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.postgresql.PostgreSQLSetup._
import au.com.fairfax.akka.utils.persistence.{AsyncDbAccess, Row}
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import com.github.mauricio.async.db.{Connection, QueryResult}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
  * @author Lee, SeongHyun (Kevin)
  * @since 2015-10-22
  */
object AkkaPersistencePostgreSqlMigration extends App {
  import MigrationTool._

  lazy val config = ConfigFactory.load
  lazy val metadataTableName: String = config.getString("akka-persistence-sql-async.metadata-table-name")
  lazy val journalTableName: String = config.getString("akka-persistence-sql-async.journal-table-name")
  lazy val snapshotTableName: String = config.getString("akka-persistence-sql-async.snapshot-table-name")
  lazy val dbName: String = config.getString("postgreSQL.db")

  println(
    s"""====================================================
       |DB INFO
       |----------------------------------------------------
       |           dbName: $dbName
       |metadataTableName: $metadataTableName
       | journalTableName: $journalTableName
       |snapshotTableName: $snapshotTableName
       |====================================================
     """.stripMargin
  )

  val start = System.currentTimeMillis

  private val tableInfoMap = MigrationTool.getAllPrimaryKeys(dbName).groupBy(_.name)
  val journalTableInfo = tableInfoMap(journalTableName)(0)
  val snapshotTableInfo = tableInfoMap(snapshotTableName)(0)

  MigrationTool.createMetadataTable(metadataTableName)
  val result = MigrationTool.migrateForAkka2_4(
    journalTableInfo,
    JournalTableName(journalTableName),
    snapshotTableInfo,
    SnapshotTableName(snapshotTableName),
    MetadataTableName(metadataTableName)
  )

  val end = System.currentTimeMillis - start

  println(
    s"""
       |====================================================
       |result:
       |----------------------------------------------------
       |${result.mkString("\n")}
       |It end ${java.text.NumberFormat.getIntegerInstance.format(end)} milliseconds.
       |====================================================
     """.stripMargin)

}


object MigrationTool {

  trait ToValueString {
    def value: String
    override val toString = value
  }
  case class JournalTableName(value: String) extends ToValueString
  case class SnapshotTableName(value: String) extends ToValueString
  case class MetadataTableName(value: String) extends ToValueString

  case class PrimaryKey(name: String)
  case class TableInfo(name: String, primaryKey: PrimaryKey)

  case class ResultValue[R](name: String, status: String, rowAffected: R) {
    override lazy val toString =
      s"""       name: $name
         |     status: $status
         |rowAffected: $rowAffected
         |----------------------------------------------------
       """.stripMargin
  }

  case class IdAndSeqNr(persistenceId: String, seqNr: Long)


  val db: AsyncDbAccess[Row] = PostgreSqlAsyncDbAccess()

  def performSequentially[T, X](connection: Connection, seq: Seq[T])(extractor: T => (Connection => Future[X])): Future[Seq[X]] = {
    val result = seq
      .drop(1)
      .map(extractor(_))
      .foldLeft(extractor(seq.head)(connection).map(Seq(_))) { case (prev, query) =>
        prev.flatMap { prevResult =>
          query(connection).map(prevResult :+ _)
        }
      }
    result
  }

  def mkResultReport(queryResult: QueryResult): String =
    s"""              queryResult: $queryResult
       |queryResult.statusMessage: ${queryResult.statusMessage}
       | queryResult.rowsAffected: ${queryResult.rowsAffected}
       |         queryResult.rows: ${queryResult.rows}
       |--------------------------------------------------------------------
       """.stripMargin

  def mkResultReport(name: String, statement: String, queryResult: QueryResult): String =
    s"""
       |====================================================================
       |name: $name
       |statement:
       |$statement
       |--------------------------------------------------------------------
       |result:
       |${mkResultReport(queryResult)}
       |====================================================================
     """.stripMargin

  def countTableRows(tableName: String): Long = {
    val future = db.singleQuery(s"SELECT COUNT(*) FROM $tableName")(_("count"))
    Await.result(future, 10 minutes).getOrElse(0)
  }

  def countIdAndMaxSeqNr(tableName: String): Long = {
    val selectStatement = s"SELECT COUNT(*) FROM (SELECT persistence_id, MAX(sequence_nr) as max_sequence_nr FROM $tableName GROUP BY persistence_id) as result;"
    val future = db.singleQuery(selectStatement)(_("count"))
    Await.result(future, 10 minutes).getOrElse(0)
  }

  def getAllPrimaryKeys(dbName: String): Seq[TableInfo] = {
    val fetchingPrimaryKeysStmt =
        s"""SELECT table_catalog, table_name, constraint_type, constraint_name
           |  FROM information_schema.table_constraints
           | WHERE table_catalog = ? AND constraint_type = 'PRIMARY KEY';
         """.stripMargin

    val result = db.query(fetchingPrimaryKeysStmt, dbName) {
      r => {
        for {
          tableName <- r[String]("table_name")
          constraintName <- r[String]("constraint_name")
        } yield (tableName, constraintName)
      }
      .map { case (tableName, constraintName) => TableInfo(tableName, PrimaryKey(constraintName)) }
    }

    Await.result(result, 10 minutes)
  }

  def createMetadataTable(tableName: String): Unit = {

    val createTableStmt =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |  persistence_key BIGSERIAL NOT NULL,
         |  persistence_id VARCHAR(255) NOT NULL,
         |  sequence_nr BIGINT NOT NULL,
         |  PRIMARY KEY (persistence_key),
         |  UNIQUE (persistence_id)
         |);
         |""".stripMargin

    val conn = dbConn

    val future = conn.connect.flatMap { c =>
      c.inTransaction { con =>
        con.sendQuery(createTableStmt)
      }
    }
    .map(queryResult => {
      println(mkResultReport(queryResult))
    })
    .flatMap(_ => conn.disconnect)

    Await.result(future, 10 minutes)
  }


  def migrateForAkka2_4(
    journalTableInfo: TableInfo,
    journalTableName: JournalTableName,
    snapshotTableInfo: TableInfo,
    snapshotTableName: SnapshotTableName,
    metadataTableName: MetadataTableName
  ): Seq[ResultValue[_]] = {

    val analyzeStmt = s"ANALYZE $journalTableName"
    val insertStmt =
      s"""INSERT INTO $metadataTableName (persistence_id, sequence_nr) (SELECT persistence_id, max_sequence_nr as sequence_nr FROM
         |    (SELECT persistence_id, MAX(sequence_nr) as max_sequence_nr FROM $journalTableName GROUP BY persistence_id) as result
         |)
       """.stripMargin

    val addPersistenceKeyStmt = s"ALTER TABLE $journalTableName ADD COLUMN persistence_key BIGINT REFERENCES $metadataTableName(persistence_key);"
    val updatePersistenceKeyStmt =
      s"""UPDATE $journalTableName jt
         |   SET persistence_key = mt.persistence_key
         |  FROM $metadataTableName mt
         | WHERE mt.persistence_id = jt.persistence_id
         |   AND jt.persistence_key IS DISTINCT FROM mt.persistence_key;
       """.stripMargin
    val alterPersistenceKeySetNotNull = s"ALTER TABLE $journalTableName ALTER COLUMN persistence_key SET NOT NULL;"

    val alterDropPrimaryKeyStmt = s"ALTER TABLE $journalTableName DROP CONSTRAINT ${journalTableInfo.primaryKey.name};"

    val alterAddPrimaryKeyStmt = s"ALTER TABLE $journalTableName ADD PRIMARY KEY (persistence_key, sequence_nr)"

    val dropPersistenceIdFromJournalStmt = s"ALTER TABLE $journalTableName DROP COLUMN persistence_id;"

    val dropMarkerFromJournalStmt = s"ALTER TABLE $journalTableName DROP COLUMN marker;"

    val dropCreatedAtFromJournalStmt = s"ALTER TABLE $journalTableName DROP COLUMN created_at;"

    val addPrimaryToSnapshotTableStmt = s"ALTER TABLE $snapshotTableName ADD COLUMN persistence_key BIGINT NOT NULL REFERENCES $metadataTableName(persistence_key);"

    val dropPrimaryKeyOfSnapshotStmt = s"ALTER TABLE $snapshotTableName DROP CONSTRAINT ${snapshotTableInfo.primaryKey.name};"

    val addPrimaryKeyOfSnapshotStmt = s"ALTER TABLE $snapshotTableName ADD PRIMARY KEY (persistence_key, sequence_nr);"

    val dropPersistenceIdFromSnapshotStmt = s"ALTER TABLE $snapshotTableName DROP COLUMN persistence_id;"

    val conn = dbConn

    val future = conn.connect.flatMap {
      con => con.inTransaction {
        con => {
          performSequentially(
            con,
            List(
              (s"ANALYZE $journalTableName", analyzeStmt),
              ("Populate metadata using the Journal data", insertStmt),
              ("Add 'persistence_key' column to the Journal table", addPersistenceKeyStmt),
              ("Set 'perssitence_key' from the Metadata table to the Journal table", updatePersistenceKeyStmt),
              ("Make 'persistence_key' in the Journal table NOT NULL", alterPersistenceKeySetNotNull),
              ("Drop the PRIMARY KEY in the Journal table", alterDropPrimaryKeyStmt),
              ("Add a new PRIMARY KEY to the Journal table", alterAddPrimaryKeyStmt),
              ("Drop 'persistence_id' from the Journal Table", dropPersistenceIdFromJournalStmt),
              ("Drop 'marker' from the Journal Table", dropMarkerFromJournalStmt),
              ("Drop 'created_at' from the Journal Table", dropCreatedAtFromJournalStmt),
              ("Add 'persistence_key' Column to the Snapshot Table", addPrimaryToSnapshotTableStmt),
              ("Drop the PRIMARY KEY from the Snapshot table", dropPrimaryKeyOfSnapshotStmt),
              ("Add the PRIMARY KEY to the Snapshot table", addPrimaryKeyOfSnapshotStmt),
              ("Drop the 'persistence_id' from the Snapshot table", dropPersistenceIdFromSnapshotStmt)
            )
          ) { case (name, statement) => {
              println(s"preparing statement: $statement")
              conn => {
                println(s"about to run this statement: $statement")
                conn.sendQuery(statement)
                    .map { resultSet =>
                        println(mkResultReport(name, statement, resultSet))
                        ResultValue(name, resultSet.statusMessage, resultSet.rowsAffected)
                    }
              }
            }
          }
        }
      }
    }

    future.flatMap {
      _ =>
        println("All DONE!!!!!!!!!!!!!!!!!!!!")
        conn.disconnect
    }

    Await.result(future, 420 minutes)
  }

}

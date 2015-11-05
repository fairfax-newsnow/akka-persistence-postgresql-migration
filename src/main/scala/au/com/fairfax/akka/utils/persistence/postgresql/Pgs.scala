package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.{DataSource, QueryResult, ResultSet, Row}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.{QueryResult => PgQueryResult, ResultSet => PgResultSet, RowData}

import scala.language.postfixOps
import scala.util.Try

/**
  * @author Lee, SeongHyun (Kevin)
  * @since 2015-10-26
  */
object Pgs {

  case object PostgreSqlConnectionDataSource extends DataSource[PostgreSQLConnection]{
    def newDataSource = PostgreSQLSetup.dbConn
  }

  object PostgreSqlQueryResult {
    def apply(pgQueryResult: PgQueryResult): PostgreSqlQueryResult =
      PostgreSqlQueryResult(pgQueryResult.rowsAffected, pgQueryResult.statusMessage, pgQueryResult.rows.map(PostgreSqlResultSet(_)))
  }

  case class PostgreSqlQueryResult(rowsAffected: Long, statusMessage: String, rows: Option[ResultSet]) extends QueryResult

  object PostgreSqlResultSet {
    def apply(resultSet: PgResultSet): PostgreSqlResultSet = {
      val columnNames = resultSet.columnNames
      PostgreSqlResultSet(columnNames, resultSet.length, resultSet.map(PostgreSqlRow(_, columnNames)).toVector)
    }
  }

  case class PostgreSqlResultSet(columnNames: IndexedSeq[String], length: Int, rows: IndexedSeq[PostgreSqlRow]) extends ResultSet {
    override def apply(idx: Int): Row = rows(idx)
  }

  object PostgreSqlRow {
    def apply(rowData: RowData, columnNames: IndexedSeq[String]): PostgreSqlRow = {
      val columnNamesToValues = columnNames.zipWithIndex.map { case (name, index) => name -> rowData(index) }.toMap
      PostgreSqlRow(rowData.rowNumber, rowData.length, columnNamesToValues)
    }
  }

  case class PostgreSqlRow(rowNumber: Int, length: Int, columns: Map[String, Any]) extends Row {
    override def apply[T](columnName: String): Option[T] = columns.get(columnName).flatMap(value => Try(value.asInstanceOf[T]).toOption)

    override def iterator: Iterator[(String, Any)] = columns.iterator
  }

}

package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.postgresql.Pgs.{PostgreSqlConnectionDataSource, PostgreSqlQueryResult}
import au.com.fairfax.akka.utils.persistence.{DataSource, Row, SyncDbAccess}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import com.github.mauricio.async.db.{Connection, QueryResult => PgQueryResult}

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
object PostgreSqlSyncDbAccess {
  def apply(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource): PostgreSqlSyncDbAccess =
    new PostgreSqlSyncDbAccess(dataSource)
}

class PostgreSqlSyncDbAccess(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource) extends SyncDbAccess[Row] {
  override def syncSingleQuery[E](query: String, params: Any*)(mapper: Row => Option[E], wait: Duration = 2 minutes): Option[E] = {

    val runQuery: (Connection) => Future[PgQueryResult] = if (params.isEmpty) _.sendQuery(query) else _.sendPreparedStatement(query, params.toSeq)

    val connection = dataSource.newDataSource
    val future = connection.connect
      .flatMap(runQuery)
      .map(PostgreSqlQueryResult(_))
      .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))

    future.onComplete { case _ => connection.disconnect }

    Await.result(future, wait)
  }

  override def syncQuery[E](query: String, params: Any*)(mapper: Row => Option[E], wait: Duration = 10 minutes): List[E] = {

    val connection = dataSource.newDataSource
    val runQuery: (Connection) => Future[PgQueryResult] = if(params.isEmpty) _.sendQuery(query) else _.sendPreparedStatement(query, params.toSeq)
    val future = connection.connect
                           .flatMap(runQuery)
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))

    future.onComplete { case _ => connection.disconnect }

    Await.result(future, wait)
  }

}

package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.postgresql.Pgs.{PostgreSqlConnectionCloseable, PostgreSqlConnectionDataSource, PostgreSqlQueryResult}
import au.com.fairfax.akka.utils.persistence.{DataSource, Row, SyncDbAccess}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import com.github.mauricio.async.db.{Connection, QueryResult => PgQueryResult}
import org.slf4j.Logger

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
object PostgreSqlSyncDbAccess {
  def apply(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource,
            logger: Option[Logger] = None): PostgreSqlSyncDbAccess =
    new PostgreSqlSyncDbAccess(dataSource)(logger)
}

class PostgreSqlSyncDbAccess(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource)(val logger: Option[Logger])
  extends SyncDbAccess[Row]
     with PostgreSqlConnectionCloseable {

  override def syncSingleQuery[E](query: String, params: Any*)(mapper: Row => Option[E], wait: Duration = 2 minutes): Option[E] = {

    val runQuery: (Connection) => Future[PgQueryResult] = if (params.isEmpty) _.sendQuery(query) else _.sendPreparedStatement(query, params.toSeq)

    implicit val connection = dataSource.newDataSource
    // @formatter:off
    val future = connection.connect
                           .flatMap(runQuery)
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))
                           .recover(closeFailed(_ => None))
    // @formatter:on

    future.onComplete(closeQuietly)

    Await.result(future, wait)
  }

  override def syncQuery[E](query: String, params: Any*)(mapper: Row => Option[E], wait: Duration = 10 minutes): List[E] = {

    implicit val connection = dataSource.newDataSource
    val runQuery: (Connection) => Future[PgQueryResult] = if(params.isEmpty) _.sendQuery(query) else _.sendPreparedStatement(query, params.toSeq)
    // @formatter:off
    val future = connection.connect
                           .flatMap(runQuery)
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))
                           .recover(closeFailed(_ => Nil))
    // @formatter:on

    future.onComplete(closeQuietly)

    Await.result(future, wait)
  }

}

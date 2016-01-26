package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.postgresql.Pgs.{PostgreSqlConnectionCloseable, PostgreSqlConnectionDataSource, PostgreSqlQueryResult}
import au.com.fairfax.akka.utils.persistence.{AsyncDbAccess, DataSource, Row}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import org.slf4j.Logger

import scala.concurrent.Future
import scala.language.postfixOps

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
object PostgreSqlAsyncDbAccess {
  def apply(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource, logger : Option[Logger] = None): PostgreSqlAsyncDbAccess =
    new PostgreSqlAsyncDbAccess(dataSource)(logger)
}

class PostgreSqlAsyncDbAccess(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource)(val logger: Option[Logger])
  extends AsyncDbAccess[Row]
     with PostgreSqlConnectionCloseable {

  override def singleQuery[E](query: String)(mapper: Row => Option[E]): Future[Option[E]] = {

    implicit val connection = dataSource.newDataSource
    // @formatter:off
    val future = connection.connect
                           .flatMap(_.sendQuery(query))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))
                           .recover(closeFailed(_ => None))
    // @formatter:on

    future.onComplete(closeQuietly)
    future
  }
  override def singleQuery[E](query: String, param: Any, restOfParams: Any*)(mapper: Row => Option[E]): Future[Option[E]] = {

    implicit val connection = dataSource.newDataSource
    // @formatter:off
    val future = connection.connect
                           .flatMap(_.sendPreparedStatement(query, param +: restOfParams.toSeq))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))
                           .recover(closeFailed(_ => None))
    // @formatter:on

    future.onComplete(closeQuietly)
    future
  }

  override def query[E](query: String)(mapper: Row => Option[E]): Future[List[E]] = {

    implicit val connection = dataSource.newDataSource
    // @formatter:off
    val future = connection.connect
                           .flatMap(_.sendQuery(query))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))
                           .recover(closeFailed(_ => Nil))
    // @formatter:on

    future.onComplete(closeQuietly)
    future
  }

  override def query[E](query: String, param: Any, restOfParams: Any*)(mapper: Row => Option[E]): Future[List[E]] = {

    implicit val connection = dataSource.newDataSource
    // @formatter:off
    val future = connection.connect
                           .flatMap(_.sendPreparedStatement(query, param +: restOfParams.toSeq))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))
                           .recover(closeFailed(_ => Nil))
    // @formatter:on

    future.onComplete(closeQuietly)
    future
  }

}

package au.com.fairfax.akka.utils.persistence.postgresql

import au.com.fairfax.akka.utils.persistence.postgresql.Pgs.{PostgreSqlConnectionDataSource, PostgreSqlQueryResult}
import au.com.fairfax.akka.utils.persistence.{AsyncDbAccess, DataSource, Row}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext

import scala.concurrent.Future
import scala.language.postfixOps

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
object PostgreSqlAsyncDbAccess {
  def apply(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource): PostgreSqlAsyncDbAccess =
    new PostgreSqlAsyncDbAccess(dataSource)
}

class PostgreSqlAsyncDbAccess(dataSource: DataSource[PostgreSQLConnection] = PostgreSqlConnectionDataSource) extends AsyncDbAccess[Row] {
  override def singleQuery[E](query: String)(mapper: Row => Option[E]): Future[Option[E]] = {

    val connection = dataSource.newDataSource
    val future = connection.connect
      .flatMap(_.sendQuery(query))
      .map(PostgreSqlQueryResult(_))
      .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))

    future.onComplete { _ => connection.disconnect }
    future
  }
  override def singleQuery[E](query: String, param: Any, restOfParams: Any*)(mapper: Row => Option[E]): Future[Option[E]] = {

    val connection = dataSource.newDataSource
    val future = connection.connect
      .flatMap(_.sendPreparedStatement(query, param +: restOfParams.toSeq))
      .map(PostgreSqlQueryResult(_))
      .map(_.rows.flatMap(_.toStream.map(mapper).take(1).flatten.headOption))

    future.onComplete { case _ => connection.disconnect }

    future
  }

  override def query[E](query: String)(mapper: Row => Option[E]): Future[List[E]] = {

    val connection = dataSource.newDataSource
    val future = connection.connect
                           .flatMap(_.sendQuery(query))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))

    future.onComplete { case _ => connection.disconnect }

    future
  }

  override def query[E](query: String, param: Any, restOfParams: Any*)(mapper: Row => Option[E]): Future[List[E]] = {

    val connection = dataSource.newDataSource
    val future = connection.connect
                           .flatMap(_.sendPreparedStatement(query, param +: restOfParams.toSeq))
                           .map(PostgreSqlQueryResult(_))
                           .map(_.rows.map(_.map(mapper).flatten.toList).getOrElse(Nil))

    future.onComplete { case _ => connection.disconnect }

    future
  }

}

package au.com.fairfax.akka.utils.persistence

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
trait SyncDbAccess[R] {
  def syncSingleQuery[E](query: String, params: Any*)(mapper: R => Option[E], wait: Duration): Option[E]
  def syncQuery[E](query: String, params: Any*)(mapper: R => Option[E], wait: Duration): Seq[E]
}

trait AsyncDbAccess[R] {
  def singleQuery[E](query: String)(mapper: R => Option[E]): Future[Option[E]]
  def singleQuery[E](query: String, param: Any, rest: Any*)(mapper: R => Option[E]): Future[Option[E]]
  def query[E](query: String)(mapper: R => Option[E]): Future[Seq[E]]
  def query[E](query: String, param: Any, rest: Any*)(mapper: R => Option[E]): Future[Seq[E]]
}

trait DbAccess[R] extends SyncDbAccess[R] with AsyncDbAccess[R]

trait DataSource[DS] {
  def newDataSource: DS
}

package au.com.fairfax.akka.utils.persistence

import scala.collection.immutable.Iterable

/**
 * @author Lee, Seong Hyun (Kevin)
 * @since 2015-05-05
 */
trait QueryResult {
  def rowsAffected: Long
  def statusMessage: String
  def rows: Option[ResultSet]

  override def toString: String = s"QueryResult(rows=$rowsAffected, status=$statusMessage)"
}

trait ResultSet extends IndexedSeq[Row] {

  def columnNames: IndexedSeq[String]

  override def toString: String = columnNames.mkString("ResultSet of (", ", ", ")") + s" containing ${this.length} rows"
}

trait Row extends Iterable[(String, Any)] {

  def apply[T](columnName: String): Option[T]

  def rowNumber: Int

  def length: Int

  override def toString: String = s"$rowNumber: (${this.mkString(", ")})"
}
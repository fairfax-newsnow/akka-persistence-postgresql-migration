package au.com.fairfax.akka.utils.persistence.postgresql

import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

object PostgreSQLSetup {
  def dbConn: PostgreSQLConnection = {
    val dbConfig = ConfigFactory.load.getConfig("postgreSQL")
    val url = s"${dbConfig.getString("url")}?user=${dbConfig.getString("user")}&password=${dbConfig.getString("pass")}"
//    println(s"DB access URL: $url")
    new PostgreSQLConnection(URLParser parse url)
  }
}

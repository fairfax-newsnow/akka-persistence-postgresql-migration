packSettings

name := "akka-persistence-postgresql-migration"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "com.github.mauricio" %% "postgresql-async" % "0.2.18"
)

packMain := Map("run-migration" -> "au.com.fairfax.akka.utils.persistence.postgresql.AkkaPersistencePostgreSqlMigration")

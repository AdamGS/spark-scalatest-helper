package com.adamgs.spark.testing

import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 *
 */
trait HiveSparkTest extends SparkTest with BeforeAndAfterAll {
  this: Suite =>

  spark.sparkContext.hadoopConfiguration.set("javax.jdo.option.ConnectionURL", connectionString)

  override def hiveSupport: Boolean = {
    true
  }

  /**
   * Connection string for Hive Metastore database, in-memory Derby be default.
   *
   * @return
   */
  def connectionString = {
    "jdbc:derby:memory:db;create=true"
  }

  /**
   * Drops all non-default databases.
   */
  override def afterAll: Unit = {
    spark.sharedState.externalCatalog
      .listDatabases()
      .filter(_ != "default")
      .foreach { db =>
        spark.sql(s"DROP DATABASE ${db} CASCADE")
      }

    super.afterAll()
  }
}

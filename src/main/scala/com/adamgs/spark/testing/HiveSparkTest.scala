package com.adamgs.spark.testing

import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Used to get a `SparkSession` instance available as `spark`, with Hive support enabled. Also allows us to test for equality between
 * `DataFrame` instances using an `equal` MustMatcher.
 *
 * Also exposes some default function the can and should be overridden in order to configure the `SparkSession` instance used for this test suite.
 *
 * '''NOTE:''' The SparkSession instance is created for each TestSuite, and destroyed after all tests are run.
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

    spark.sharedState.externalCatalog.listTables("default").foreach { table =>
      val sparkTable = spark.sharedState.externalCatalog.getTable("default", table)
      if (sparkTable.tableType == CatalogTableType.VIEW) {
        spark.sql(s"DROP VIEW default.${table}")
      } else {
        spark.sql(s"DROP TABLE default.${table}")
      }
    }

    super.afterAll()
  }
}

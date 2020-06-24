package com.adamgs.spark.testing

import com.adamgs.spark.DataFrameTools
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.{Failure, Success, Try}


/**
 * Used to get a `SparkSession` instance available as `spark`. Also allows us to test for equality between
 * `DataFrame` instances using an `equal` MustMatcher.
 *
 * Also exposes some default function the can and should be overridden in order to configure the `SparkSession` instance used for this test suite.
 *
 * '''NOTE:''' The SparkSession instance is created for each TestSuite, and destroyed after all tests are run.
 */
trait SparkTest extends BeforeAndAfterAll with Logging {
  this: Suite =>
  val spark = sparkSession
  implicit val dataframeEq: Equality[DataFrame] = new Equality[DataFrame] {
    override def areEqual(a: DataFrame, b: Any): Boolean = {
      b match {
        case other: DataFrame => DataFrameTools.dataFrameEquals(a, other)
        case _ => false
      }
    }
  }

  /**
   * Creates a `SparkConf` for this test suite, should be overridden.
   *
   * @return
   */
  def sparkConfiguration: SparkConf = {
    new SparkConf
  }

  /**
   * whether the `SparkSession` instance should have hive support enabled
   */
  def hiveSupport: Boolean = {
    false
  }

  private def sparkSession: SparkSession = {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .config(sparkConfiguration)

    if (hiveSupport) {
      builder.enableHiveSupport
    }

    builder.getOrCreate()
  }

  /**
   * Destroys the internal `SparkSession`. '''Must''' be called after every other cleaning up actions.
   */
  override def afterAll() = {
    // I Use the internal Spark logger here, a smarter user than me might want to use a different one
    Try(SparkSession.clearDefaultSession()) match {
      case Success(_) => log.info("Cleared SparkSession successfully")
      case Failure(e) => log.warn(s"Encountered an error while clearing the SparkSession :${e.toString}")
    }

    super.afterAll()
  }

}

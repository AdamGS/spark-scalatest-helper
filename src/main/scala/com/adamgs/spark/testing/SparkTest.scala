package com.adamgs.spark.testing

import com.adamgs.spark.DataFrameTools
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfterAll, Suite}


/**
 * Used to get a `SparkSession` instance available as `spark`. Also allows us to test for equality between
 * `DataFrame` instances using an `equal` MustMatcher.
 *
 * Also exposes some default function the can and should be overridden in order to configure the `SparkSession` instance used for this test suite.
 *
 * '''NOTE:''' The SparkSession instance is created for each TestSuite, and destroyed after all tests are run.
 */
trait SparkTest extends BeforeAndAfterAll {
  this: Suite =>
  @transient val spark = createSession()
  implicit val dataframeEq = new Equality[DataFrame] {
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
  def configureSpark: SparkConf = {
    new SparkConf
  }

  /**
   * whether the `SparkSession` instance should have hive support enabled
   */
  def hiveSupport: Boolean = {
    false
  }

  private def createSession(): SparkSession = {
    val builder = SparkSession.builder()
      .master("local[*]")
      .config(configureSpark)

    if (hiveSupport) {
      builder.enableHiveSupport
    }

    builder.getOrCreate()
  }

  /**
   * Destroys the internal `SparkSession`. '''Must''' be called after every other cleaning up actions.
   */
  override def afterAll() = {
    super.afterAll()
    SparkSession.clearDefaultSession()
  }
}

package com.adamgs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.catalyst.encoders.RowEncoder


object DataFrameTools {
  private def zipWithIndex(df: DataFrame): RDD[(Long, Row)] = {
    val indexed =
      df.withColumn("id", monotonically_increasing_id)

    indexed.rdd.map({ row =>
      (row.getAs[Long]("id"), row)
    })
  }

  def dataFrameEquals(expected: DataFrame, result: DataFrame): Boolean = {
    if (expected.schema != result.schema) {
      return false
    }

    // TODO: Change from try to a more functional Try
    try {
      expected.cache
      result.cache
      if (expected.count != result.count) {
        return false
      }

      val expectedIndexValue = zipWithIndex(expected)
      val resultIndexValue = zipWithIndex(result)

      expectedIndexValue.join(resultIndexValue).filter { case (idx, (r1, r2)) =>
        !(r1.equals(r2))
      }.count() == 0
    } finally {
      expected.unpersist()
      result.unpersist()
    }
  }
}

package com.adamgs.spark

import org.apache.spark.sql.DataFrame

object DataFrameTools {
  def dataFrameEquals(expected: DataFrame, result: DataFrame): Boolean = {
    if (expected.schema != result.schema) {
      return false
    }

    result.except(expected).count == 0 && expected.except(result).count == 0
  }
}

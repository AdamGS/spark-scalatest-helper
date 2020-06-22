package com.adamgs.spark

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import scala.math.abs


object DataFrameTools {
  private def zipWithIndex[U](rdd: RDD[U]) = {
    rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
  }

  def dataFrameEquals(expected: DataFrame, result: DataFrame): Boolean = {
    if (expected.schema != result.schema) {
      return false
    }

    try {
      expected.rdd.cache
      result.rdd.cache
      if (expected.rdd.count != result.rdd.count) {
        return false
      }

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter { case (idx, (r1, r2)) =>
        !(r1.equals(r2) || approxEquals(r1, r2, 0.0))
      }

      unequalRDD.count == 0
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    if (r1.length != r2.length) {
      return false
    } else {
      (0 until r1.length).foreach(idx => {
        if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
          return false
        }

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                return false
              }

            case f1: Float =>
              if (java.lang.Float.isNaN(f1) !=
                java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
                return false
              }
              if (abs(f1 - o2.asInstanceOf[Float]) > tol) {
                return false
              }

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) !=
                java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
                return false
              }
              if (abs(d1 - o2.asInstanceOf[Double]) > tol) {
                return false
              }

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
                return false
              }

            case t1: Timestamp =>
              if (abs(t1.getTime - o2.asInstanceOf[Timestamp].getTime) > tol) {
                return false
              }

            case _ =>
              if (o1 != o2) return false
          }
        }
      })
    }
    true
  }
}

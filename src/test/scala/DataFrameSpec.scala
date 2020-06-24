import com.adamgs.spark.testing.SparkTest
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

class DataFrameSpec extends AnyFunSpec with SparkTest with Matchers {
  describe("DataFrameTools") {
    describe("DataFrames from the same data") {
      it("should equal") {
        val data = Seq(Tuple2("hello", 3))
        val other = spark.createDataFrame(data)
        val df = spark.createDataFrame(data)

        df must equal(other)
      }
    }
    describe("Different DataFrames") {
      it("Should not equal one another") {
        val first = Seq(("hello", 3), ("hello", 5))
        val second = Seq(("hello", 5))
        val oneDF = spark.createDataFrame(first)
        val secondDF = spark.createDataFrame(second)

        oneDF must not equal secondDF
      }
    }
  }
}

import com.adamgs.spark.testing.SparkTest
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

class DataFrameSpec extends AnyFunSpec with SparkTest with Matchers {
  describe("Dataframe") {
    describe("Should equal to itself") {
      it("should work") {
        val data = Seq(Tuple2("hello", 3))
        val other = spark.createDataFrame(data)
        val df = spark.createDataFrame(data)

        df must equal(other)
      }
    }
  }
}

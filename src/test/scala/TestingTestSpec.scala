import com.adamgs.spark.testing.SparkTest
import org.apache.spark.SparkConf
import org.scalatest._
import org.scalatest.matchers.must.Matchers

class TestingTestSpec extends FunSpec with SparkTest with Matchers {
  override def sparkConfiguration: SparkConf = {
    val conf = new SparkConf
    conf.set("some_random", "value")

    conf
  }

  describe("Testing a function") {
    describe("Some cool stuff") {
      it("should be one") {
        val data = Seq(Tuple2("hello", 3))
        val df = spark.createDataFrame(data)

        df.count must equal(data.length)
      }
    }
    describe("Do some other stuff") {
      it("Pass configuration") {
        spark.conf.get("some_random") mustBe "value"
      }
    }
  }
}

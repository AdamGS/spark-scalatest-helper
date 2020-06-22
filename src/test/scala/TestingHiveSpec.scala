import com.adamgs.spark.testing.HiveSparkTest
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

class TestingHiveSpec extends AnyFunSpec with HiveSparkTest with Matchers {
  private val database = "db"

  describe("TestingHiveSparkSession") {
    describe("Should create database") {
      it("Should work") {
        spark.sql(s"CREATE DATABASE $database")
        spark.sql("CREATE TABLE db.stuff (id INT)")
        spark.sharedState.externalCatalog.listDatabases() must contain theSameElementsAs (Seq("default", database))
      }
    }
  }
}

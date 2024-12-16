package dev.bigspark.datasources

import dev.bigspark.security.VaultCredentialsManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class JdbcSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("JdbcSourceTest")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  "JdbcSource" should "correctly identify database type from URL" in {
    val urls = Map(
      "jdbc:postgresql://localhost:5432/db" -> "postgresql",
      "jdbc:oracle:thin:@localhost:1521:db" -> "oracle",
      "jdbc:sqlserver://localhost:1433;database=db" -> "sqlserver",
      "jdbc:teradata://localhost/db" -> "teradata"
    )

    urls.foreach { case (url, expectedType) =>
      val source = InputSources.JdbcSource(
        url = url,
        table = "test_table",
        credentials = Right(InputSources.DirectCredentials("user", "pass"))
      )
      
      val dbType = source.getDatabaseType
      dbType shouldBe expectedType
    }
  }

  it should "throw IllegalArgumentException for unsupported database URL" in {
    val source = InputSources.JdbcSource(
      url = "jdbc:unsupported://localhost/db",
      table = "test_table",
      credentials = Right(InputSources.DirectCredentials("user", "pass"))
    )

    an[IllegalArgumentException] should be thrownBy source.getDatabaseType
  }

  it should "correctly set driver class based on database type" in {
    val expectedDrivers = Map(
      "jdbc:postgresql://localhost:5432/db" -> "org.postgresql.Driver",
      "jdbc:oracle:thin:@localhost:1521:db" -> "oracle.jdbc.driver.OracleDriver",
      "jdbc:sqlserver://localhost:1433;database=db" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbc:teradata://localhost/db" -> "com.teradata.jdbc.TeraDriver"
    )

    expectedDrivers.foreach { case (url, expectedDriver) =>
      val source = InputSources.JdbcSource(
        url = url,
        table = "test_table",
        credentials = Right(InputSources.DirectCredentials("user", "pass"))
      )
      
      val driverClass = source.getDriverClass
      driverClass shouldBe expectedDriver
    }
  }

  it should "handle direct credentials correctly" in {
    val source = InputSources.JdbcSource(
      url = "jdbc:postgresql://localhost:5432/db",
      table = "test_table",
      credentials = Right(InputSources.DirectCredentials("testuser", "testpass"))
    )

    val (username, password) = source.credentials match {
      case Right(creds) => (creds.username, creds.password)
      case _ => fail("Expected DirectCredentials")
    }

    username shouldBe "testuser"
    password shouldBe "testpass"
  }

  it should "apply all optional parameters when provided" in {
    val source = InputSources.JdbcSource(
      url = "jdbc:postgresql://localhost:5432/db",
      table = "test_table",
      credentials = Right(InputSources.DirectCredentials("user", "pass")),
      fetchSize = Some(1000),
      partitionColumn = Some("id"),
      numPartitions = Some(5),
      lowerBound = Some(1L),
      upperBound = Some(1000L),
      filter = Some("column = 'value'")
    )

    // We can't easily test the actual DataFrame creation without a real database,
    // but we can verify that the source object contains all the expected parameters
    source.fetchSize shouldBe Some(1000)
    source.partitionColumn shouldBe Some("id")
    source.numPartitions shouldBe Some(5)
    source.lowerBound shouldBe Some(1L)
    source.upperBound shouldBe Some(1000L)
    source.filter shouldBe Some("column = 'value'")
  }
} 
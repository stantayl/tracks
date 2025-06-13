import org.apache.spark.sql.DataFrame
import java.io.FileInputStream

class PostgresProcessor extends BaseProcessor {
  private def readFromPostgres(tableName: String): DataFrame = {
    val secrets = new java.util.Properties()
    secrets.load(new FileInputStream(s"${System.getProperty("user.home")}/.secrets"))

    val connectionProps = new java.util.Properties()
    Map(
      "user" -> secrets.getProperty("user"),
      "password" -> secrets.getProperty("password"),
      "driver" -> "org.postgresql.Driver"
    ).foreach {
      case (k, v) => connectionProps.setProperty(k, v)
    }

    spark.read.jdbc(
      secrets.getProperty("url"),
      tableName,
      connectionProps
    )
  }

  override def start(): Unit = processSource(readFromPostgres(MainArgs.inputPath))
}

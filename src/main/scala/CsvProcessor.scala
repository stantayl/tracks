import org.apache.spark.sql.DataFrame

class CsvProcessor extends BaseProcessor {
  private def readFromCsv(filename: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("sep", "\\t")
      .load(filename)
  }

  override def start(): Unit = processSource(
    readFromCsv(MainArgs.inputPath)
  )
}


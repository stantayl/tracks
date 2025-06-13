import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, sum, explode}

class JsonProcessor extends BaseProcessor {
  private def readFromJson(filename: String): DataFrame = {
    spark.read.format("json")
      .load(filename)
  }

  import spark.implicits._

  override def start(): Unit = processSource(
    readFromJson(MainArgs.inputPath)
  )

  override def processSource(df: DataFrame): Unit = {
    df.printSchema
    val dfExploded = df
      .withColumn("albums", explode($"albums"))
      .withColumn("track", explode($"albums.tracks"))
      .where($"artist".isNotNull && $"albums.album".isNotNull)
      .groupBy($"artist", $"albums.album", $"albums.year")
      .agg(
        count("*").as("nbrTracks"),
        formattedTime(sum($"track.length")).as("totalTime")
      )

    dfExploded.printSchema
    dfExploded.show(truncate=false)
  }
}


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat, floor, format_string, lit, md5, rank, struct, sum, trim, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object FunctionOption extends Enumeration {
  val Stats = Value("stats")
  val Show = Value("show")
  val ExtendedStats = Value("extended-stats")
  val Validations = Value("validations")
  val WindowedAggregations = Value("windowed-aggs")
  val GenerateJson = Value("generate-json")
}

import FunctionOption._

abstract class BaseProcessor {
  val spark: SparkSession = SparkSession.builder()
    .appName("tracks")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def start(): Unit

  def processSource(df: DataFrame): Unit = {
    if (MainArgs.doOption(Stats))
      reportStats(df)

    val dataframes = collectDatasets(df)
    if (MainArgs.doOption(Show)) {
      Map[String, String](
        "artists" -> "ARTIST",
        "albums" -> "ALBUM",
        "genres" -> "GENRE",
        "tracks" -> "TITLE"
      ).foreach { case (dataframe, sort) =>
        dataframes(dataframe).orderBy(sort).show(5, truncate = false)
      }
    }

    if (MainArgs.doOption(Validations))
      runValidations(dataframes)

    if (MainArgs.doOption(ExtendedStats))
      extendedStats(dataframes)

    if (MainArgs.doOption(WindowedAggregations))
      windowedAggregations(dataframes)

    if (MainArgs.doOption(GenerateJson))
      writeJson(dataframes, MainArgs.outputPath)
  }

  def reportStats(df: DataFrame): Unit = {
    val rawCount = df.count

    val artistCount = df
      .select("ARTIST")
      .where($"ARTIST".isNotNull)
      .distinct
      .count

    val albumCount = df
      .select("ALBUM", "ARTIST")
      .where($"ALBUM".isNotNull && $"ARTIST".isNotNull)
      .distinct
      .count

    println(s"Total raw records: $rawCount")
    println(s"Number of artists: $artistCount")
    println(s"Number of albums: $albumCount")
  }

  /**
   * Wrapper class to provide custom extensions to the DataFrame
   * @param df The DataFrame to wrap
   */
  implicit class MyDataFrameOps(val df: DataFrame) {
    /**
     * Denormalizes a dataframe by joining to an ID table, and replacing the ID
     * with the full descriptive column; e.g., replace df.ALBUM_ID with dfId.ALBUM.
     * @param dfId The ID dataframe (expected to contain SOMEFIELD_ID and SOMEFIELD)
     * @return The df DataFrame, with SOMEFIELD_ID replaced with SOMEFIELD
     */
    def deNormalize(dfId: DataFrame): DataFrame = {
      // Auto-determine field to replace based on column content of ID field
      val idField = dfId.columns.filter(_.endsWith("_ID"))(0)
      val pkIdField = s"pk$idField"

      df
        .join(
          dfId.withColumnRenamed(idField, pkIdField),
          col(idField) === col(pkIdField),
          "left"
        )
        .drop(idField, pkIdField)
    }
  }

  private def trackCountsByAlbum(dataframes: Map[String, DataFrame]): DataFrame = {
    dataframes("tracks")
      .groupBy("ARTIST_ID", "ALBUM_ID")
      .count()
  }

  def albumCountsByTrackCount(trackCountsByAlbum: DataFrame): DataFrame = {
    trackCountsByAlbum
      .withColumnRenamed("count", "trackCount")
      .groupBy("trackCount")
      .count()
      .withColumnRenamed("count", "numberOfAlbums")
  }

  private def extendedStats(dataframes: Map[String, DataFrame]): Unit = {
    val tcba = trackCountsByAlbum(dataframes)
      .deNormalize(dataframes("albums"))
      .deNormalize(dataframes("artists"))
      .select("ARTIST", "ALBUM", "count")
      .orderBy($"count".desc)

    tcba.show(truncate=false)

    albumCountsByTrackCount(tcba)
      .orderBy($"numberOfAlbums".desc, $"trackCount")
      .show(truncate=false)
  }

  /**
   * Converts millisecond track length to "MM:SS"
   * @param colLength the column containing the track length
   * @return a string Column with the formatted time
   */
  protected def formattedTime(colLength: Column): Column = format_string(
      "%02d:%02d",
      (colLength / 60_000).cast("int"),      // minutes
      ((colLength / 1_000) % 60).cast("int") // seconds
    )

  /**
   * Playing with Spark window functions.  What's currently there produces a DataFrame like:
   * +-----------+--------------------------+--------+---------+--------------+
   * |ALBUM_ID   |title                     |fmtTrack|fmtLength|fmtRunningTime|
   * +-----------+--------------------------+--------+---------+--------------+
   * |...        |Tomcat [Muddy Waters]     |1/120   |03:38    |03:38         |
   * |...        |Hard Times [Baby Huey ]   |2/120   |03:21    |06:59         |
   * |...        |Shack Up [Banbarra]       |3/120   |02:59    |09:58         |
   *
   * Where fmtTrack and fmtRunningTime resets for each new ALBUM_ID
   *
   * @param dataframes The collection of DataFrames
   */
  private def windowedAggregations(dataframes: Map[String, DataFrame]): Unit = {
    val windowed = dataframes("tracks")
      .where($"track".isNotNull && $"ALBUM_ID".isNotNull)
      .withColumn("fmtLength", formattedTime($"length"))
      .withColumn("totalTracks", sum("track").over(Window.partitionBy("ALBUM_ID")))
      .withColumn("fmtTrack", concat($"track", lit("/"), $"totalTracks"))
      .withColumn("runningTime", sum("length").over(Window.partitionBy("ALBUM_ID").orderBy("track")))
      .withColumn("fmtRunningTime", formattedTime($"runningTime"))
      .orderBy($"ALBUM_ID", $"track")
      .select($"ALBUM_ID", $"title", $"fmtTrack", $"fmtLength", $"fmtRunningTime")

    windowed.show(truncate=false)
  }

  /**
   * Creates an ID/Description DataFrame from a denormalized DataFrame
   * @param df the denormalized source table
   * @param columnName column name to create an ID dataframe from; e.g., "SOMEFIELD" will
   *                   produce an ID DataFrame containing "SOMEFIELD_ID" and "SOMEFIELD"
   * @param uniqueColumns set of column from which to produce a surrogate key; e.g., to produce
   *                      a proper ALBUM_ID, need to hash ARTIST and ALBUM, since multiple
   *                      artists may have similarly named albums
   * @return new ID DataFrame containing a unique ID and description column
   */
  private def createIdTable(
    df: DataFrame,
    columnName: String,
    uniqueColumns: List[String] = List.empty
  ): DataFrame = {

    // if no unique columns provided, assume that columnName is the sole unique column
    val uniqueCols: List[String] =
      if (uniqueColumns.isEmpty) List(columnName)
      else uniqueColumns

    val colsUnique:List[Column] = uniqueCols.map(s => col(s))
    val tempName = "temp"

    val idName = s"${columnName}_ID"

    /**
     * Column expression that resolves to true if any of the unique columns contains a blank string -
     * we don't want to create an ID row for any source row that has Null in any of the uniqueColumns
     */
    val hasBlankColumn = colsUnique
      .map(c => trim(c) === "")
      .reduce(_ || _)

    val hashingSource = concat(colsUnique: _*)

    df
      .select(colsUnique: _*)
      .withColumn(tempName, when(hasBlankColumn, null).otherwise(hashingSource))
      .where(col(columnName).isNotNull)
      .withColumn(idName, md5(hashingSource))
      .select(idName, columnName)
      .distinct
  }

  /**
   * Takes the given dataframe and replace a value column with an ID column
   * TODO: this method can probably eliminated in preference for replaceValueByHash()
   *
   * @param df - the base dataframe
   * @param columnName - the columnName to replace (e.g., "ARTIST" -> "ARTIST_ID")
   * @param idDf - the data frame containing the ID (e.g., an "ARTIST_ID"/"ARTIST" frame)
   * @return New data frame with value column replaced with ID column
   */
  def replaceValueWithID(
    df: DataFrame,
    columnName: String,
    idDf: DataFrame,
  ): DataFrame = {

    df
      .join(
        idDf.withColumnRenamed(s"$columnName", s"fk$columnName"),
        col(columnName) === col(s"fk$columnName"),
        "left"
      )
      .drop(columnName, s"fk$columnName")
  }

  /**
   * Produces a new DataFrame, replacing a denormalized column in the source DataFrame with the
   * corresponding hashed ID from the ID DataFrame
   * @param df Denormalized source frame
   * @param columnName column name to replace
   * @param dfId ID frame
   * @param hashColumns columns used to produce the hashed ID in the ID frame
   * @return
   */
  def replaceValueByHash(
    df: DataFrame,
    columnName: String,
    dfId: DataFrame,
    hashColumns: List[String] = List.empty
  ): DataFrame = {

    // Default to single column name if no hashColumns provided
    val hashCols = if (hashColumns.isEmpty) List(columnName) else hashColumns
    val colsHash: List[Column] = hashCols.map(s => col(s))

    df
      .withColumn("hash", md5(concat(colsHash: _*)))
      .join(
        dfId.withColumnRenamed(columnName, s"fk$columnName"),
        col("hash") === col(s"${columnName}_ID"),
        "left"
      )
      .drop(columnName, s"fk$columnName", "hash")
  }

  def cleanedTracks(df: DataFrame, idDatasets: Map[String, DataFrame]): DataFrame = {
    val df1 = replaceValueByHash(df, "ALBUM", idDatasets("albums"), List("ARTIST", "ALBUM"))
//    val df2 = replaceValueWithID(df1, "ARTIST", idDatasets("artists"))
    val df2 = replaceValueByHash(df1, "ARTIST", idDatasets("artists"))
//    replaceValueWithID(df2, "GENRE", idDatasets("genres"))
    replaceValueByHash(df2, "GENRE", idDatasets("genres"))
  }

  def collectDatasets(df: DataFrame): Map[String, DataFrame] = {
    val idDatasets = Map(
      "artists" -> createIdTable(df, "ARTIST"),
      "albums" -> createIdTable(df, "ALBUM", List("ARTIST", "ALBUM")),
      "genres" -> createIdTable(df, "GENRE")
    )
    // the tracks dataset is dependent on the other three
    idDatasets + ("tracks" -> cleanedTracks(df, idDatasets))
  }

  // TODO: should probably be consolidated with MyDataFrameOps rather than doing it context manager style.
  class withDf(df: DataFrame) {
    def verifyRange(columnName: String, min: Int, max: Int): withDf = {
      println(s"$columnName out of range $min to $max:")
      val column = col(columnName)
      df
        .select("FILENAME", columnName)
        .where(column < min || column > max)
        .orderBy("FILENAME")
        .show(100_000, truncate = false)

      this
    }

    def verifyNonNull(
      columnName: String,
      conditionalOn: Option[String] = None,
      antiConditionalOn: Option[String] = None
    ): withDf = {

      def description: String =
        if (conditionalOn.isDefined)
          s"$columnName should not be null when ${conditionalOn.get} is non-null"
        else if (antiConditionalOn.isDefined)
          s"$columnName should be null when ${antiConditionalOn.get} is null"
        else
          s"$columnName should not be null"

      println(s"$description:")
      val column = col(columnName)
      val expr: Column =
        if (conditionalOn.isDefined) column.isNull && col(conditionalOn.get).isNotNull
        else if (antiConditionalOn.isDefined) column.isNotNull && col(antiConditionalOn.get).isNull
        else column.isNull

      df
        .select("FILENAME", columnName)
        .where(expr)
        .orderBy("FILENAME")
        .show(100_000, truncate = false)

      this
    }
  }

  def runValidations(dataframes: Map[String, DataFrame]): Unit = {
    //
    // Track validations
    //
    new withDf(dataframes("tracks"))
      .verifyRange("TRACK", 1, 1_000)
      .verifyRange("YEAR", 1920, 10_000)
      .verifyNonNull("GENRE_ID")
      .verifyNonNull("TITLE")
      .verifyNonNull("ARTIST_ID")
      .verifyNonNull("TRACK", conditionalOn=Some("ALBUM_ID"))
      .verifyNonNull("TRACK", antiConditionalOn=Some("ALBUM_ID"))
      .verifyNonNull("YEAR", conditionalOn=Some("ALBUM_ID"))
  }

  def writeJson(dataframes: Map[String, DataFrame], filename: String): Unit = {
    val df_flattened: DataFrame = dataframes("tracks")
      .deNormalize(dataframes("albums"))
      .deNormalize(dataframes("artists"))
      .deNormalize(dataframes("genres"))

    val dfAristAlbum = df_flattened
      .groupBy($"ARTIST".as("artist"), $"ALBUM".as("album"), $"YEAR".as("year"))
      .agg(
        collect_list(
          struct(
            $"TRACK".as("track"),
            $"TITLE".as("title"),
            $"FILENAME".as("filename"),
            $"GENRE".as("genre"),
            $"LENGTH".as("length")
          )
        ).as("tracks")
      )
      .orderBy("artist", "album", "year", "tracks.track")

    val dfArtist = dfAristAlbum
      .groupBy($"artist")
      .agg(
        collect_list(
          struct(
            $"album", $"year", $"tracks"
          )
        ).as("albums")
      )

    dfArtist.repartition(1).write.json(filename)
  }
}

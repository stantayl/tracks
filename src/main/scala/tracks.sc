import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

import java.io.File

val spark = SparkSession.builder()
  .appName("Spark in Worksheet")
  .master("local[*]")  // use local mode with all CPU cores
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

val filename = "/Users/stantayl/repos/personal/java-play/Scala/spark/tracks/src/main/resources/tracks.tsv"

val df = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\\t")
  .load(filename).limit(5_000)

//df.show
//println(s"Number of tracks: ${df.count()}")
//println(s"Number of albums: ${df.select("ALBUM", "ARTIST").distinct().count()}")
//println(s"Number of artists: ${df.select("ARTIST").distinct().count()}")
//
//val df_trackCountsPerAlbum = (
//  df.groupBy("ARTIST", "ALBUM")
//    .count()
//    .withColumnRenamed("count", "trackCount")
//  )
//
//df_trackCountsPerAlbum.show(5, truncate=false)
//
//val df_albumTrackCounts = df_trackCountsPerAlbum
//    .groupBy("trackCount")
//    .agg(
//      F.array_agg(
//        F.concat(
//          F.col("ARTIST"),
//          F.lit("/"),
//          F.col("ALBUM")
//        )
//      ).alias("albums")
//    )
//    .select(
//      F.col("trackCount"),
//      F.size(F.col("albums")).alias("albumCount"),
//      F.col("albums")
//    )
//
//df_albumTrackCounts
//  .orderBy(F.desc("trackCount"))
//  .show(20, truncate=80, vertical=false)
//
//val df_albumsPerArtist = (
//  df
//    .groupBy("ARTIST")
//    .agg(F.count_distinct(F.col("ALBUM")).alias("albumCount"))
//    .orderBy(F.desc("albumCount"))
//  )
//
//df_albumsPerArtist
//  .show(20, truncate=1000, vertical=false)
//
//import spark.implicits._
//
//println("Albums per Artist")
//
//df_albumsPerArtist
//  .where($"albumCount" === 3)
//  .select("ARTIST")
//  .orderBy("ARTIST")
//  .show(20, truncate=1000, vertical=false)
//
//println("Checking years")
//
//val df_years = df
//    .where($"YEAR".isNotNull)
//    .groupBy("ARTIST")
//    .agg(
//      F.min("YEAR").alias("firstYear"),
//      F.max("YEAR").alias("lastYear")
//    )
//    .withColumn(
//      "years",
//      $"lastYear" - $"firstYear" + F.lit(1)
//    )
//    .where($"years" > 1)
//    .orderBy(F.desc("years")
//    )
//
//println("YEARS")
//df_years.show(100)
//
//val rows = df_years.collectAsList()
//rows.forEach {
//  row => println(s"${row(0)}: ${row(1)} to ${row(2)} (${row(3)} years)")
//}

//df.printSchema
//
//val df2 = df
//  .withColumn("mystruct", F.struct("ARTIST", "GENRE"))
//
//df2.printSchema
//df2.show

val df3 = df
  .groupBy("ARTIST", "ALBUM", "YEAR") // must have an agg() before you can show it!
  .agg(F.collect_list($"TRACK").as("my_list"))

df3.printSchema
df3.show

println("hello")

val (groupByCols, nestedCols) = df.columns.partition(
  Seq("ARTIST", "ALBUM", "YEAR").contains(_)
)

val df4 = df
  .groupBy(groupByCols.map(F.col): _*)
  .agg(
    F.collect_list(
      F.struct(
        nestedCols.map(F.col): _*
      )
    ).as("the_tracks")
  )
  .where($"ALBUM".isNotNull)
  .orderBy("ARTIST", "ALBUM")

df4.printSchema
df4.show

val df5 = df4
  .groupBy("ARTIST")
  .agg(
    F.collect_list(
      F.struct(
        $"ARTIST", $"YEAR", $"the_tracks"
      )
    ).as("albums")
  )
  .orderBy("ARTIST")

df5.printSchema
df5.show(truncate=false)
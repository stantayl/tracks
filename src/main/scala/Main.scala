import scala.collection.mutable.ListBuffer
import com.beust.jcommander.{JCommander, Parameter}
import MainArgs.DataSource

import scala.jdk.CollectionConverters.CollectionHasAsScala

object MainArgs {
  val repoBase = "/Users/stantayl/repos/personal/java-play/Scala/spark/tracks" // TO-DO: auto-determine

  object DataSource extends Enumeration {
    val Postgres = Value("postgres")
    val Csv = Value("csv")
    val Json = Value("json")
  }

  @Parameter(names = Array("-h", "--help"), description = "Display this help", required = false, help = true)
  var help: Boolean = false

  @Parameter(names = Array("-s", "--datasource"), description = "Data Source (csv, postgres, or json)", required = true)
  private var _dataSource: String = _

  def dataSource: DataSource.Value = DataSource.withName(_dataSource) // may throw exception

  @Parameter(names = Array("-i", "--input"), description = "Input path", required = false)
  private var _inputPath: String = ""

  // Default input path is dependent on which data source
  def inputPath: String = if (_inputPath.nonEmpty) _inputPath else dataSource match {
    case DataSource.Postgres => "public.raw" // table name
    case DataSource.Csv => s"$repoBase/src/main/resources/tracks.tsv"
    case DataSource.Json => s"$repoBase/src/main/resources/tracks.json"
  }

  // needed when generating fresh JSON from Csv or Postgres source
  @Parameter(names = Array("-o", "--output"), description = "JSON output path", required = false)
  var outputPath: String = s"$repoBase/tracks.json/"

  @Parameter(variableArity = true, description = "Function(s) to run", required = false)
  var _functionOptions: java.util.List[String] = _

  def doOption(opt: FunctionOption.Value): Boolean = {
    val selectedOptions = if (_functionOptions == null) List.empty else _functionOptions.asScala.toList

    // if function options list is empty, then do all except GenerateJson
    if (selectedOptions.isEmpty && (opt != FunctionOption.GenerateJson))
      true
    else
      selectedOptions.contains(opt.toString)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    println(s"ARGS: ${args.mkString(",")}")
    val jCommander = JCommander.newBuilder
      .programName("Tracks")
      .addObject(MainArgs)
      .build()

    jCommander.parse(args: _*)
    if (MainArgs.help) {
      jCommander.usage()
    }
    else {
      MainArgs.dataSource match {
        case DataSource.Csv => new CsvProcessor().start()
        case DataSource.Postgres => new PostgresProcessor().start()
        case DataSource.Json => new JsonProcessor().start()
      }
    }
  }
}

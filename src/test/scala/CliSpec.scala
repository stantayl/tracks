//import org.scalatest.{BeforeAndAfter, FunSuite}

import com.beust.jcommander.JCommander
import org.scalatest.funsuite.AnyFunSuite

class CliSpec extends AnyFunSuite {
  def parseArgs(args: String*): Unit = {
    val jCommander = JCommander.newBuilder
      .programName("TracksTest")
      .addObject(MainArgs)
      .build()

    jCommander.parse(args: _*)
  }

  test("Command-line argument parsing 1") {
    parseArgs("-s", "postgres")

    assert(MainArgs.dataSource == MainArgs.DataSource.Postgres)
    assert(MainArgs.outputPath == s"${MainArgs.repoBase}/tracks.json/")
    assert(MainArgs.inputPath == "public.raw")

    assert(MainArgs.doOption(FunctionOption.Stats))
    assert(MainArgs.doOption(FunctionOption.ExtendedStats))
    assert(MainArgs.doOption(FunctionOption.Validations))
    assert(MainArgs.doOption(FunctionOption.WindowedAggregations))

    assert(!MainArgs.doOption(FunctionOption.GenerateJson))
  }

  test("Command-line argument parsing 2") {
    val inputPath = "my/input/path"
    val outputPath = "my/output/path"
    parseArgs("-s", "csv", "-o", outputPath, "-i", inputPath, "stats", "windowed-aggs", "generate-json")

    assert(MainArgs.dataSource == MainArgs.DataSource.Csv)
    assert(MainArgs.outputPath == outputPath)
    assert(MainArgs.inputPath == inputPath)

    assert(MainArgs.doOption(FunctionOption.Stats))
    assert(MainArgs.doOption(FunctionOption.WindowedAggregations))
    assert(MainArgs.doOption(FunctionOption.GenerateJson))

    assert(!MainArgs.doOption(FunctionOption.ExtendedStats))
    assert(!MainArgs.doOption(FunctionOption.Validations))
  }

  test("Command-line argument parsing 3") {
    parseArgs("-s", "json")

    assert(MainArgs.dataSource == MainArgs.DataSource.Json)
  }

  test("Command-line argument parsing 4") {
    parseArgs("-s", "somethingfunky")
    assertThrows[java.util.NoSuchElementException] {
      MainArgs.dataSource
    }
  }
}
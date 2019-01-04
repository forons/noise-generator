package eu.unitn.disi.noise.parser

import eu.unitn.disi.db.noise.NoiseType
import org.apache.commons.cli._
import eu.unitn.disi.db.spark.io.Format

object CLParser {

  def parse(args: Array[String]): Config = {
    val options = buildOptions()
    val parser = new GnuParser
    val cmd = parser.parse(options, args)
    var input: String = null; var inputFormat = Format.CSV
    var output: String = null; var outputFormat = Format.CSV
    var inferSchema = true; var hasHeader = false
    var nullValues = ""; var quote = "\""
    var noise: NoiseType = null
    var rate: Double = 0.0
    var columns: Array[Int] = Array.emptyIntArray
    var idColumns: Array[Int] = Array.emptyIntArray
    var extraVal: Any = null

    for (option <- cmd.getOptions) {
      option.getLongOpt match {
        case "input" => input = option.getValue.trim
        case "input-format" =>
          inputFormat =
            Format.valueOf(option.getValue.toUpperCase().replace(".", "").trim)
        case "output" => output = option.getValue.trim
        case "output-format" =>
          outputFormat =
            Format.valueOf(option.getValue.toUpperCase.replace(".", "").trim)
        case "header"      => hasHeader = true
        case "inferSchema" => inferSchema = true
        case "null"        => nullValues = option.getValue.trim
        case "quote"       => quote = option.getValue.trim
        case "noise" =>
          noise = NoiseType.valueOf(option.getValue.toUpperCase.trim)
        case "percentage" => rate = option.getValue.trim.toDouble
        case "columns"    => columns = option.getValues.map(_.toInt)
        case "id-columns" => idColumns = option.getValues.map(_.toInt)
        case "extra"      => extraVal = option.getValue.trim
      }
    }
    Config(
      input = input,
      inputFormat = inputFormat,
      output = output,
      outputFormat = outputFormat,
      nullValues = nullValues,
      quote = quote,
      inferSchema = inferSchema,
      hasHeader = hasHeader,
      noise = noise,
      rate = rate,
      columns = columns,
      idColumns = idColumns,
      extra = extraVal
    )
  }

  def buildOptions(): Options = {
    val options: Options = new Options()
    options.addOption(new Option("i", "input", true, "Input dataset"))
    options.addOption(new Option("if", "input-format", true, "Input format"))
    options.addOption(new Option("o", "output", true, "The output path"))
    options.addOption(new Option("of", "output-format", true, "Output format"))
    options.addOption(new Option("header", "header", false, "Has header?"))
    options.addOption(
      new Option("schema", "inferSchema", false, "Infer Schema?"))
    options.addOption(new Option("quote", "quote", true, "Quote character"))
    options.addOption(new Option("null", "null", true, "Null representation"))
    options.addOption(new Option("n", "noise", true, "The noise type"))
    options.addOption(new Option("p", "percentage", true, "Noise percentage"))
    options.addOption(new Option("t", "times", true, "Generation times"))
    options.addOption(new Option("extra", "extra", true, "Extra parameters"))
    options.addOption(new Option("chain", "chain", true, "Noise chain"))
    val columnOption = new Option("col", "Specifies the list of columns")
    columnOption.setLongOpt("columns")
    columnOption.setArgName("columns")
    columnOption.setArgs(-2) // UNLIMITED NUMBER OF ELEMENTS
    columnOption.setValueSeparator(' ')
    columnOption.setRequired(false)
    options.addOption(columnOption)
    val idColumnOption = new Option("id", "Specifies the list of columns")
    idColumnOption.setLongOpt("id-columns")
    idColumnOption.setArgName("columns")
    idColumnOption.setArgs(-2) // UNLIMITED NUMBER OF ELEMENTS
    idColumnOption.setValueSeparator(' ')
    idColumnOption.setRequired(false)
    options.addOption(idColumnOption)
    options
  }
}

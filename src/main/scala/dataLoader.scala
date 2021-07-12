import main.spark
import org.apache.spark.sql.functions._
import utils._
import java.io.File
object dataLoader extends App {
  // load the data files paths
  def loadFilePaths(fileDir: String) = {
    val extension = "csv"
    val fileNames = new File(fileDir).listFiles
      .filter(fileName => fileName.isFile && fileName.getName.endsWith(extension))
    val validFileNames = fileNames.filter(fileName => fileName.getName.matches(f"^.*\\d++.${extension}$$"))
    validFileNames
  }
  // load the raw sensors data files
  def loadRawSignalFiles(filePath: String) = {
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .schema(schema)
      .load(filePath)
    val new_df = df.drop("T1", "H1", "T2", "H2", "T3", "H3", "T4", "H4", "T5", "H5", "Unnamed: 31")
    new_df
  }
  // load the raw labels files
  def loadLabelRaw(filePath: String) = {
    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(labelSchema)
      .load(filePath)
    def pUdf2 = udf((value: String) => {
      "S"+value
      })
    val df2 = df.withColumn("sujIdx", pUdf2($"index")).drop("index")
    df2
  }
}

import main.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lag, udf, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object dataCleaner extends App {

  import spark.implicits._
  //crop the raw data file
  def cropStartEndExperience(df : DataFrame, tagThreshold : Int = 15000) = {
    val endVal = df.filter(s"R10 >= $tagThreshold").select("index")
      .sort(col("index").desc).head().getInt(0)
    val endDf = df.filter(df("index") < endVal)
    val startVal : Int = df.filter(s"R9 >= $tagThreshold").select("index").head().getInt(0)
    val startDf = endDf.filter(df("index") > startVal)
    startDf
  }
  //edit the time stamp column
  def remove_time_offset(df : DataFrame) ={
    val t0 = df.select("# Relative Time").head().getInt(0)
    val newDf = df.withColumn("time", col("# Relative Time")-t0)
    newDf
  }
  //check the sampling frequency
  def checkSamplingFreq(df : DataFrame, verbose : Boolean = false) =
  {
    def pUdf = udf((value: Int) => {
      value / 10 match {
        case 9 => true
        case _ => false
      }
    })
    val w = Window.partitionBy().orderBy(col("index"))
    val newDf = df
      .withColumn("timeLag", lag("time",1).over(w))
      .withColumn("sampleDiff",col("time")-col("timeLag"))
      .withColumn("sampleDiffOk",pUdf($"sampleDiff"))
    if (newDf.filter(newDf("sampleDiffOk") === false).count() >0 && verbose )
    {
      newDf.filter(newDf("sampleDiffOk") === false).show()
      throw new Exception("Sampling Frequency is wrong")
    }
      df
  }
  //convert the tags into one single tag column
    def convertTagRow(df : DataFrame, tagThreshold :Int = 15000 ) = {
      val newDf = df.withColumn("tag", when(col("R3") > tagThreshold, "R3")
        .when(col("R4") > tagThreshold && col("R1") < tagThreshold && col("R2") < tagThreshold && col("R3") < tagThreshold && col("R5") < tagThreshold && col("R6") < tagThreshold && col("R7") < tagThreshold, "R4")
        .when(col("R5") > tagThreshold && col("R1") < tagThreshold && col("R2") < tagThreshold && col("R3") < tagThreshold && col("R4") < tagThreshold && col("R6") < tagThreshold && col("R7") < tagThreshold, "R5")
        .when(col("R6") > tagThreshold && col("R1") < tagThreshold && col("R2") < tagThreshold && col("R3") < tagThreshold && col("R5") < tagThreshold && col("R4") < tagThreshold && col("R7") < tagThreshold, "R6")
        .when(col("R7") > tagThreshold && col("R1") < tagThreshold && col("R2") < tagThreshold && col("R3") < tagThreshold && col("R5") < tagThreshold && col("R6") < tagThreshold && col("R4") < tagThreshold, "R7")
        .when(col("R1") > tagThreshold && col("R2") > tagThreshold && col("R4") < tagThreshold && col("R3") < tagThreshold && col("R5") < tagThreshold && col("R6") < tagThreshold && col("R7") < tagThreshold, "R0")
        .when(col("R1") < tagThreshold && col("R2") < tagThreshold && col("R3") < tagThreshold && col("R4") < tagThreshold && col("R5") < tagThreshold && col("R6") < tagThreshold && col("R7") < tagThreshold, "R0")
        .otherwise("unknown")
      )
      if (newDf.filter(newDf("tag") === "unknown").count() > 0) {
        newDf.filter(newDf("tag") === "unknown").show()
        throw new Exception("Sampling Frequency is wrong")

      }
      newDf
    }
  //convert the tags order
  def checkTagOrder(df : DataFrame) : Unit= {
    val w = Window.partitionBy().orderBy(col("index"))
    val newDf = df.withColumn("tagLag", lag("tag",1).over(w))
      .withColumn("Vtag", col("tag")===col("tagLag"))
      .filter(col("Vtag") === false).select("tag")
      .withColumn("id",monotonically_increasing_id())
    val expectedList =List(
    "R3", "R0", "R4", "R0", "R5", "R0", "R6", "R0", "R7",
    "R0", "R7", "R0", "R6", "R0", "R5", "R0", "R4", "R0", "R3",
    "R0", "R5", "R0", "R7", "R0", "R3", "R0", "R6", "R0", "R4"
    )
    val expectedTags =  expectedList.toDF("tag2")
      .withColumn("id",monotonically_increasing_id())
    val verifDf = newDf.join(expectedTags, "id")
      .withColumn("tagVerif", col("tag")===col("tag2"))
      .filter(col("tagVerif") === false)
    if (verifDf.count() >0){
      throw new Exception("the sampling sequence does not match the expected one")
    }
  }
  //keep the used columns
  def getCleanDf(df : DataFrame) = {
  val resDf = df.select("time", "C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","tag" )
  resDf
  }
  //transform the DataFrame
  def transform (df :DataFrame) ={
    getCleanDf(convertTagRow(checkSamplingFreq(remove_time_offset(cropStartEndExperience(df)))))
  }
}

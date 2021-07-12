import dataCleaner.transform
import main.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

object preprocess {
  // create the DataFrame for average sensors values
  def createAvgDataframe(df:DataFrame) : DataFrame = {
    val df_out = transform(df).filter(col("tag")==="R3")
    val avrg1 = df_out.select(avg($"C1") as "avgC11",avg($"C2") as "avgC12",
      avg($"C3") as "avgC13",avg($"C4") as "avgC14",avg($"C5") as "avgC15").withColumn("index",lit(0))
    val df_in = transform(df).filter(col("tag")==="R4")
    val avrg2 = df_in.select(avg($"C1") as "avgC21" ,avg($"C2") as "avgC22",avg($"C3") as "avgC23"
      ,avg($"C4") as "avgC24",avg($"C5") as "avgC25").withColumn("index",lit(0))

    val avgDf = avrg1.join(avrg2, "index").withColumn("diffC1", col("avgC11")-col("avgC21"))
      .withColumn("diffC2", col("avgC12")-col("avgC22"))
      .withColumn("diffC3", col("avgC13")-col("avgC23"))
      .withColumn("diffC4", col("avgC14")-col("avgC24"))
      .withColumn("diffC5", col("avgC15")-col("avgC25"))
      .drop("index","avgC11","avgC12","avgC13","avgC14","avgC15","avgC21","avgC22","avgC23","avgC24","avgC25")
    avgDf
  }
  // create the features DataFrame
  def createFeaturesDf (averageMap : scala.collection.mutable.Map[String,
    scala.collection.mutable.MutableList[Double]])=
  {
    val valDf = averageMap.toSeq.toDF("index", "score")
    val featuresDf = valDf.select(col("index"),col("score").getItem(0).as("C1"),
      col("score").getItem(1).as("C2"),
      col("score").getItem(2).as("C3"),
      col("score").getItem(3).as("C4"),
      col("score").getItem(4).as("C5")).drop("score")
    featuresDf
  }
}

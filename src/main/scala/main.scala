import dataLoader._
import org.apache.spark.sql.SparkSession
import preprocess._
import scala.collection.mutable
import regression._
object main extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("sensors")
    .getOrCreate()
  val sc = spark.sparkContext
  spark.sparkContext.setLogLevel("ERROR")
  //create A Map that contains the mean values of sensors for two tags
  var avgMap = scala.collection.mutable.Map[String,scala.collection.mutable.MutableList[Double]]()
  loadFilePaths("D:/sensors_data").foreach( file =>
 {
    val subject_id = "S"+file.toString.substring(22, file.toString.length-4)
    val df = loadRawSignalFiles(file.toString)

   val listeAvg = mutable.MutableList[Double]()
   for (i <- 0 to 4)
      {
     val eleme = createAvgDataframe(df).collect().map(_.getDouble(i)).toList
     listeAvg += eleme(0)
      }
     avgMap += subject_id -> listeAvg
  })
  //create features DataFrame
  val featuresDf =createFeaturesDf(avgMap)
  //create label DataFrame
  val labelsRDF = loadLabelRaw("D:/sensors_data/labels.csv")
  //Create the DataSet
  val dataSet = featuresDf.join(labelsRDF,featuresDf("index")===labelsRDF("sujIdx")).drop("index","sujIdx")
  dataSet.show()
  //predict the weight
  Predict(dataSet,"Weight")



}

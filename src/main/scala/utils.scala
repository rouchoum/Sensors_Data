import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object utils {

  val schema = new StructType()
    .add("index", IntegerType, true)
    .add("# Relative Time", IntegerType, true)
    .add("C1", IntegerType, true)
    .add("C2", IntegerType, true)
    .add("C3", IntegerType, true)
    .add("C4", IntegerType, true)
    .add("C5", IntegerType, true)
    .add("C6", IntegerType, true)
    .add("C7", IntegerType, true)
    .add("C8", IntegerType, true)
    .add("C9", IntegerType, true)
    .add("C10", IntegerType, true)
    .add("R1", IntegerType, true)
    .add("R2", IntegerType, true)
    .add("R3", IntegerType, true)
    .add("R4", IntegerType, true)
    .add("R5", IntegerType, true)
    .add("R6", IntegerType, true)
    .add("R7", IntegerType, true)
    .add("R8", IntegerType, true)
    .add("R9", IntegerType, true)
    .add("R10", IntegerType, true)
  val labelSchema = new StructType()
    .add("index", StringType, true)
    .add("Height", DoubleType, true)
    .add("Weight", DoubleType, true)


  val columnsToDrop = ("T1", "H1", "T2", "H2", "T3", "H3", "T4", "T5", "H5", "Unnamed")
  val sensorCols= List("C1","C2","C3","C4","C5","C6","C7","C8","C9","C10")

}

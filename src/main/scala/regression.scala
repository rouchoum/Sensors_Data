import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
object regression  {
  def Predict(data: DataFrame, label: String)={
    val assembler = new VectorAssembler()
      .setInputCols(Array("C1","C2","C3","C4","C5"))
      .setOutputCol("features")
    val df = assembler.transform(data)
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol(label)
      .setFeaturesCol("features")
       .setPredictionCol("prediction")
    // Train model. This also runs the indexer.
    val model = rf.fit(trainingData)
    // Make predictions.
    val predictions = model.transform(testData)
    // Select example rows to display.
    predictions.select("prediction", label, "features").show(5)
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(label)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    predictions
  }
}

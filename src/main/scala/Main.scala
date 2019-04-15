import SGD._
import Utils._
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import Settings._

import scala.util.Random

object Main {
  def main(args: Array[String]) = {

    // Begin the set up
    val t1 = System.nanoTime()
    val spark = SparkSession.builder.appName("hogwild-spark").getOrCreate()
    val sc = spark.sparkContext

    // Load data and split it into train/test
    val data = load_reuters_data(sc, train_path, topics_path, test_paths, "CCAT", true)

    val split = data.randomSplit(Array(train_proportion, 1-train_proportion), seed)
    val train_set = split(0)
    val test_set = split(1)
    val test_collected = test_set.collect()

    // Spread the data to all nodes
    val b_train_set = sc.broadcast(train_set.collect().toList)

    val count_test_set = test_collected.length

    // Initializing weights, training_losses and array containing cumulated durations of epochs
    var weights = Vector.fill(D)(0.0)
    var training_losses = Vector.empty[Double]
    var validation_losses = Vector.empty[Double]
    var epoch_durations_cum = Vector.empty[Double]

    // Creates partition accessor
    val range = sc.parallelize((1 to workers).zip(1 to workers))
    val access = range.partitionBy(new RangePartitioner(workers, test, true))

    // Getting set up time
    val load_duration = (System.nanoTime - t1) / 1e9d
    println("Set up duration: " + load_duration)

    // The start of training epochs
    val t2 = System.nanoTime()
    var validation_loss = 1.0

    while(validation_loss >= 0.3) {

      // Spread weights to all nodes
      val wb = sc.broadcast(weights)

      // Compute gradient at each node using accessor
      val gradients = access.mapPartitions(it => {
        val sample = Random.shuffle(b_train_set.value).take(batch_size).toVector
        val gradients = sgd_subset(sample, wb.value, regParam, D)

        Iterator(gradients)
      }).collect()

      // Merge gradients computed at each partitions
      val gradient = gradients.reduce((x, y) => {
        val list = x.toList ++ y.toList
        val merged = list.groupBy ( _._1) .map { case (k,v) => k -> v.map(_._2).sum }
        merged
      })

      // Update weights
      val grad_keys = gradient.keys.toVector
      weights = ((0 until(D)).zip(weights)).map(x => {
        if (grad_keys.contains(x._1)) x._2- gradient(x._1)*alpha
        else x._2
      }).toVector

      // Compute validation loss
      validation_loss = compute_loss(test_collected.toVector, wb.value, regParam))

      //val train_loss = (lossRDD.sum)/ count_train_set
      validation_loss = (validation_loss)/ count_test_set
      //training_losses :+= train_loss
      validation_losses :+= validation_loss

      val epoch_duration = (System.nanoTime - t2) / 1e9d
      epoch_durations_cum :+= epoch_duration

      //println("Current training loss: " + train_loss)
      println("Current validation loss: " + validation_loss)
      println("Epoch duration: " + epoch_duration)
    }

    // Prints of some key observations
    println("Set up duration: " + load_duration)
    println("Epoch durations: " + epoch_durations_cum)
    println("Training losses: " + training_losses)
    println("Validation losses: " + validation_losses)

  }
}

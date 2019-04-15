import SGD._
import Utils._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
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

    // Partition the train and test sets on the number of workers
    val train_partition = train_set.partitionBy(new HashPartitioner(workers))
    //val test_partition = test_set.partitionBy(new HashPartitioner(workers))
    val count_test_set = test_collected.length
    val count_train_set = train_set.count()

    // Initializing weights, training_losses and array containing cumulated durations of epochs
    var weights = Vector.fill(D)(0.0)
    var training_losses = Vector.empty[Double]
    var validation_losses = Vector.empty[Double]
    var epoch_durations_cum = Vector.empty[Double]

    // Getting set up time
    val load_duration = (System.nanoTime - t1) / 1e9d
    println("Set up duration: " + load_duration)

    // The start of training epochs
    val t2 = System.nanoTime()
    var validation_loss = 1.0
    //

    while(validation_loss >= 0.3) {

      val wb = sc.broadcast(weights)

      val sampledRDD = train_partition.mapPartitions(it => {
        val sample = Random.shuffle(it.toList).take(batch_size)
        sample.iterator
      })

      val gradsRDD = sampledRDD.mapPartitions(partition => {
        Iterator(sgd_subset(partition.toVector, wb.value, regParam, D))
      })

      val grads = gradsRDD.reduce((x, y) => {
        val list = x.toList ++ y.toList
        val merged = list.groupBy ( _._1) .map { case (k,v) => k -> v.map(_._2).sum }
        merged
      })

      val grad_keys = grads.keys.toVector

      //CHECK the ELSE PART IF EVER THERE IS A BUG
      weights = ((0 until(D)).zip(weights)).map(x => {
        if (grad_keys.contains(x._1)) x._2- grads(x._1)*alpha
        else x._2
      }).toVector



      val lossRDD = train_partition.mapPartitions(partition => {
        Iterator(compute_loss(partition.toVector, wb.value, regParam))
      })

      /*val validationLoss = test_collected.map(partition => {
        Iterator(compute_loss(partition, wb.value, regParam))
      })*/
      val validationLoss = compute_loss(test_collected.toVector, wb.value, regParam)


      val train_loss = (lossRDD.sum)/ count_train_set
      validation_loss = (validationLoss)/ count_test_set
      training_losses :+= train_loss
      validation_losses :+= validation_loss

      val epoch_duration = (System.nanoTime - t2) / 1e9d
      epoch_durations_cum :+= epoch_duration

      println("Current training loss: " + train_loss)
      println("Current validation loss: " + validation_loss)
      println("Epoch duration: " + epoch_duration)
    }

    // Prints of wome key observations
    println("Set up duration: " + load_duration)
    println("Epoch durations: " + epoch_durations_cum)
    println("Training losses: " + training_losses)
    println("Validation losses: " + validation_losses)

    /*val map_logs = Map("set_up_duration" -> load_duration,
                       "epoch_durations" -> epoch_durations_cum,
                       "training_losses" -> training_losses,
                       "validation_losses" -> validation_losses) */
  }
}

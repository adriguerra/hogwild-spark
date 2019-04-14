import SGD._
import Utils._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Settings._

import scala.util.Random

object Main {
  def main(args: Array[String]) = {

    // Begin the set up
    val t1 = System.nanoTime()
    val conf = new SparkConf().setMaster("local").setAppName("SGD")
    val sc = new SparkContext(conf)



    // Load data and split it into train/test
    val data = load_reuters_data(sc, train_path, topics_path, test_paths, "CCAT", true)
    val split = data.randomSplit(Array(train_proportion, 1-train_proportion), seed)
    val train_set = split(0)
    val test_set = split(1)

    // Partition the train and test sets on the number of workers
    val train_partition = train_set.partitionBy(new HashPartitioner(workers))
    val test_partition = test_set.partitionBy(new HashPartitioner(workers))

    // Initializing weights, losses and array containing cumulated durations of epochs
    var weights = Vector.fill(D)(0.0)
    var losses = Vector.empty[Double]
    var epoch_durations_cum = Vector.empty[Double]

    // Getting set up time
    val load_duration = (System.nanoTime - t1) / 1e9d
    println("Load duration: " + load_duration)

    // The start of training epochs
    val t2 = System.nanoTime()
    for (i <- 1 to nb_epochs) {

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




      val loss = lossRDD.sum / workers
      losses :+= loss

      val epoch_duration = (System.nanoTime - t2) / 1e9d
      epoch_durations_cum :+= epoch_duration

      println("Current loss: " + loss)
      println("Epoch duration: " + epoch_duration)
    }

    val SGD_duration = (System.nanoTime - t2) / 1e9d
    println("SGD duration: " + SGD_duration)
    println("Epoch durations: " + epoch_durations_cum)
    println("Losses: " + losses)
  }
}

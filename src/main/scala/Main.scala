import SGD._
import Utils._
import Settings._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Random
//import org.apache.spark.sql.randomSplit
//import org.apache.spark.randomSplit

object Main {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("SGD")
    val sc = new SparkContext(conf)

    val topics_path = "/data/datasets/rcv1-v2.topics.qrels"
    val test_paths = List("/data/datasets/lyrl2004_vectors_test_pt0.dat",
      "/data/datasets/lyrl2004_vectors_test_pt1.dat",
      "/data/datasets/lyrl2004_vectors_test_pt2.dat",
      "/data/datasets/lyrl2004_vectors_test_pt3.dat")
    val train_path = "/data/datasets/lyrl2004_vectors_train.dat"

    val t1 = System.nanoTime()
    val data = load_reuters_data(sc, train_path, topics_path, test_paths, "CCAT", true)
    val load_duration = (System.nanoTime - t1) / 1e9d

    println("Load duration: " + load_duration)

    println("TEST HERE")

    val b_batch_size = sc.broadcast(batch_size)

    val split = data.randomSplit(Array(train_proportion, 1-train_proportion), seed)
    val train_set = split(0)
    val test_set = split(1)
    val train_partition = train_set.partitionBy(new HashPartitioner(workers))
    val test_partition = test_set.partitionBy(new HashPartitioner(workers))

    var weights = Vector.fill(D)(0.0)
    var losses = Vector.empty[Double]
    var epoch_durations = Vector.empty[Double]

    val t2 = System.nanoTime()
    for (i <- 1 to nb_epochs) {

      val wb = sc.broadcast(weights)

      // Compute gradient and loss at each partitions
      val gradientsWithLoss = train_partition.mapPartitions(it => {
        val samples = Random.shuffle(it.toList).take(b_batch_size.value).toVector
        val gradient = sgd_subset(samples, wb.value, b_batch_size.value, alpha, regParam)

        val loss = compute_loss(it.toVector, wb.value, regParam)

        Iterator((gradient, loss))
      }).collect()

      // Extract gradients and losses
      val gradients = gradientsWithLoss.map(_._1)
      val losses = gradientsWithLoss.map(_._2)

      // Merge gradients computed at each partitions
      val gradient = gradients.reduce((a,b) => (a,b).zipped.map(_+_))

      // Update weights
      weights = weights.zip(gradient).map(z => z._1 - z._2 * alpha)

      // Compute new loss
      val loss = losses.sum / workers

      val epoch_duration = (System.nanoTime - t2) / 1e9d
      epoch_durations :+= epoch_duration

      println("Current loss: " + loss)
      println("Epoch duration: " + epoch_duration)
    }

    val SGD_duration = (System.nanoTime - t2) / 1e9d
    println("SGD duration: " + SGD_duration)
    println("Epoch durations: " + epoch_durations)
    println("Losses: " + losses)
  }
}

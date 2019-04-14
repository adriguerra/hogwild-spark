import SGD._
import Utils._
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

    val seed = 42
    val train_proportion = 0.9
    val D = 47237
    val N = 23149
    val workers = 10
    val nb_epochs = 10
    val batch_size = sc.broadcast(128)
    val alpha = 0.03 * (100.0 / batch_size.value) / workers
    val regParam = 1e-5

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

      val sampledRDD = train_partition.mapPartitions(it => {
        val sample = Random.shuffle(it.toList).take(batch_size.value)
        sample.iterator
      })

      val gradsRDD = sampledRDD.mapPartitions(partition => {
        Iterator(sgd_subset(partition.toVector, wb.value, batch_size.value, alpha, regParam))
      })
      val lossRDD = sampledRDD.mapPartitions(partition => {
        Iterator(compute_loss(partition.toVector, wb.value, regParam))
      })

      val grads = gradsRDD.reduce((a,b) => (a,b).zipped.map(_+_))
      // weights = weights.flatMap(w => grads.map(g => w - g * alpha))
      weights = weights.zip(grads).map(z => z._1 - z._2 * alpha)

//      println(weights)


      val loss = lossRDD.sum / workers
      losses :+= loss

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

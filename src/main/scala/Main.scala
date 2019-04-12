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

    val topics_path = "/Users/guerra/hogwild-spark/src/main/resources/rcv1-v2.topics.qrels"
    val test_paths = List("/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt0.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt1.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt2.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt3.dat")
    val train_path = "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_train.dat"

    val data = load_reuters_data(sc, train_path, topics_path, test_paths, "CCAT", true)
    val seed = 42
    val train_proportion = 0.9
    val split = data.randomSplit(Array(train_proportion, 1-train_proportion), seed)
    val workers = 10
    val train_set = split(0)
    val test_set = split(1)
    val train_partition = train_set.partitionBy(new HashPartitioner(workers))
    val test_partition = test_set.partitionBy(new HashPartitioner(workers))

    val D = 47236
    val N = 26000
    var weights = Vector.fill(D)(0.0)
    val nb_epochs = 10000
    val batch_size = sc.broadcast(128)
    val alpha = 0.03
    val regParam = 0.1

    for (i <- 1 to nb_epochs) {
      val wb = sc.broadcast(weights)

      val sampledRDD = train_partition.mapPartitions(it => {
        val sample = Random.shuffle(it.toList).take(batch_size.value)
        sample.iterator
      })

      val weightsRDD = sampledRDD.mapPartitions(partition => {
        Iterator(sgd_subset(partition.toVector, wb.value, batch_size.value, alpha, regParam))
      })

      weights = weightsRDD.reduce((a, b)=> (a, b).zipped.map(_+_))
      weights.map(_/workers)
    }
  }
}

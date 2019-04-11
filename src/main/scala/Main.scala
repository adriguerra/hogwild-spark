import Utils._
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SGD")
    val sc = new SparkContext(conf)

    val topics_path = "/Users/guerra/hogwild-spark/src/main/resources/rcv1-v2.topics.qrels"
    val test_path = List("/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt0.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt1.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt2.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt3.dat")
    val train_path = "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_train.dat"
    val data, labels = load_reuters_data(sc, train_path, topics_path, test_path, "CCAT", true)

  }

}

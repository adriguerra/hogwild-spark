import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {

    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SyncSGD")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    val test_data = sc.textFile("src/main/resources/lyrl2004_vectors_test_pt*.dat")
    val train_data = sc.textFile("src/main/resources/lyrl2004_vectors_train.dat")
    val topics = sc.textFile("src/main/resources/rcv1-v2.topics.qrels")

  }
}

import scala.io.Source

object Main {
  def main(args: Array[String]) {

    val topics_path = "/Users/guerra/hogwild-spark/src/main/resources/rcv1-v2.topics.qrels"
    val test_path = List("/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt0.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt1.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt2.dat",
      "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt3.dat")
    val train_path = "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_train.dat"
    val data, labels = load_large_reuters_data(train_path, topics_path, test_path, "CCAT", true)
  }

  def generate_map(datapoint: List[String]) = {
    //    val d = {0 -> 1}
    datapoint.map(x => x.split(":")).map(x => (x.head, x.last))
  }

  def load_large_reuters_data(train_path: String, topics_path: String, test_path: List[String], selected_cat: String, train: Boolean): (List[(String, String)], List[Int]) = {
    var labels_tmp = ""
    var data = List[(String, String)]()
    if (train) {
      val source = Source.fromFile(topics_path)
      val lines = source.getLines().flatMap(line => line.trim().split(" ")).toList
      labels_tmp = lines.head
      data = generate_map(lines.tail.tail)
    }
    else {
      for (path <- test_path) {
        val source = Source.fromFile(path)
        val lines = source.getLines().flatMap(line => line.trim().split(" ")).toList
        val label = lines.head
        val data_i = generate_map(lines.tail.tail)
        labels_tmp += label
        data ++= data_i
      }
    }
    val cat = get_category_dict(topics_path)
    val labels = labels_tmp.map(label => if (cat.get(label.toInt).contains(selected_cat)) 1 else -1)
    (data, labels.toList)
  }

  def get_category_dict(topics_path: String) = {
    val source = Source.fromFile(topics_path)
    val lines = source.getLines().map(line => {
      val s = line.trim().split(" ")
      (s.tail.head.toInt, s.head)
    }).toList
    lines.groupBy(_._1).mapValues(l => l.map(_._2))
  }

}

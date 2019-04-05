import scala.io.Source

object Utils {

  def generate_map(datapoint: List[String]) = {
    //    val d = {0 -> 1}
    datapoint.map(x => x.split(":")).map(x => (x.head, x.last))
  }

  def load_sample_reuters_data(train_path: String, topics_path: String, test_path: List[String], selected_cat: String, train: Boolean): (List[(String, String)], List[Int]) = {
    var labels_tmp = ""
    var data = List[(String, String)]()
    if (train) {
      val source = Source.fromFile(topics_path)
      val lines = source.getLines().take(4).flatMap(line => line.trim().split(" ")).toList
      labels_tmp = lines.head
      data = generate_map(lines.tail.tail)
    }
    else {
      for (path <- test_path) {
        val source = Source.fromFile(path)
        val lines = source.getLines().take(4).flatMap(line => line.trim().split(" ")).toList
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

  def load_reuters_data(train_path: String, topics_path: String, test_path: List[String], selected_cat: String, train: Boolean): (List[(String, String)], List[Int]) = {
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

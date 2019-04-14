import scala.io.Source

val topics_path = "/Users/guerra/hogwild-spark/src/main/resources/rcv1-v2.topics.qrels"
val test_paths = List("/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt0.dat",
  "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt1.dat",
  "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt2.dat",
  "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_test_pt3.dat")
val train_path = "/Users/guerra/hogwild-spark/src/main/resources/lyrl2004_vectors_train.dat"

def generate_mappings(datapoint: Array[String]) = {
  //    val d = {0 -> 1}
  datapoint.map(x => {
    val pairs = x.split(":")
    (pairs.head.toInt, pairs.last.toFloat)
  }).toMap
}

def get_category_dict(topics_path: String) = {
  val source = Source.fromFile(topics_path)
  val lines = source.getLines().map(line => {
    val s = line.trim().split(" ")
    (s.tail.head.toInt, s.head)
  }).toList
  lines.groupBy(_._1).mapValues(l => l.map(_._2))
}

def generate_labelled_data(lines: Iterator[String]) = {
  lines.map(line => {
    val elements = line.trim().split(" ")
    val label = elements.head.toInt
    val mappings = generate_mappings(elements.tail.tail)
    (mappings, label)
  }).toList.unzip
}

def load_sample_reuters_data(train_path: String, topics_path: String, test_path: List[String], selected_cat: String, train: Boolean) = {
  val (data, labels) = {
    if (train) {
      val source = Source.fromFile(train_path)
      val lines = source.getLines().take(4)
      generate_labelled_data(lines)
    }
    else {
      var labels_tmp = List[Int]()
      var data_i = List[Map[Int, Float]]()
      for (path <- test_path) {
        val source = Source.fromFile(path)
        val lines = source.getLines().take(4)
        val labelled_data = generate_labelled_data(lines)
        data_i ++= labelled_data._1
        labels_tmp ++= labelled_data._2
      }
      (data_i, labels_tmp)
    }}
  val categories = get_category_dict(topics_path)
  val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
  (data, cat_labels)
}

def load_reuters_data(train_path: String, topics_path: String, test_path: List[String], selected_cat: String, train: Boolean) = {
  val (data, labels) = {
    if (train) {
      val source = Source.fromFile(train_path)
      val lines = source.getLines()
      generate_labelled_data(lines)
    }
    else {
      var labels_tmp = List[Int]()
      var data_i = List[Map[Int, Float]]()
      for (path <- test_path) {
        val source = Source.fromFile(path)
        val lines = source.getLines()
        val labelled_data = generate_labelled_data(lines)
        data_i ++= labelled_data._1
        labels_tmp ++= labelled_data._2
      }
      (data_i, labels_tmp)
    }}
  val categories = get_category_dict(topics_path)
  val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
  (data, cat_labels)
}

val res = load_sample_reuters_data(train_path, topics_path, test_paths, "CCAT", true)
val ints = res._2
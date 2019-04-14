import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

object Utils {

  def generate_mappings(datapoint: Array[String]) = {
    val d = Map(0 -> 1.0f)
    val mappings = datapoint.map(x => {
      val pairs = x.split(":")
      (pairs.head.toInt, pairs.last.toFloat)
    }).toMap
    d ++ mappings
  }

  def generate_labelled_data(lines: RDD[String]) = {
    lines.map(line => {
      val elements = line.trim().split(" ")
      val label = elements.head.toInt
      val mappings = generate_mappings(elements.tail.tail)
      (label, mappings)
    }).collect().toList.unzip
  }

  def load_sample_reuters_data(sc: SparkContext, train_path: String, topics_path: String, test_paths: List[String], selected_cat: String, train: Boolean) = {
    val (labels, mappings) = {
      if (train) {
        val source = sc.textFile(train_path).take(2000)
        generate_labelled_data(sc.parallelize(source))
      }
      else {
        var labels_tmp = List[Int]()
        var data_i = List[Map[Int, Float]]()
        for (path <- test_paths) {
          val source = sc.textFile(path).take(2000)
          val (label, mapping) = generate_labelled_data(sc.parallelize(source))
          data_i ++= mapping
          labels_tmp ++= label
        }
        (labels_tmp, data_i)
      }}

    val categories = get_category_dict(topics_path)
    val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
    sc.parallelize(labels.zip(mappings.zip(cat_labels)))
  }

  def load_reuters_data(sc: SparkContext, train_path: String, topics_path: String, test_paths: List[String], selected_cat: String, train: Boolean) = {
    val (labels, mappings) = {
      if (train) {
        val source = sc.textFile(train_path)
        generate_labelled_data(source)
      }
      else {
        var labels_tmp = List[Int]()
        var data_i = List[Map[Int, Float]]()
        for (path <- test_paths) {
          val source = sc.textFile(path)
          val (label, mapping) = generate_labelled_data(source)
          data_i ++= mapping
          labels_tmp ++= label
        }
        (labels_tmp, data_i)
      }}

    val categories = get_category_dict(topics_path)
    val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
    sc.parallelize(labels.zip(mappings.zip(cat_labels)))
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

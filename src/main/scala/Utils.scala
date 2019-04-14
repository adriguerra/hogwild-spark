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

  def generate_labelled_data(sc: SparkContext, lines: RDD[String], topics_path: String, selected_cat: String) = {
    lines.mapPartitions(it => {
      val categories = get_category_dict(topics_path)
      it.map(line => {
        val elements = line.trim().split(" ")
        val label = elements.head.toInt
        val mappings = generate_mappings(elements.tail.tail)
        //      val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
        (label, (mappings, if (categories.get(label).contains(selected_cat)) 1 else -1))
      })
    })
    /*lines.map(line => {
      val elements = line.trim().split(" ")
      val label = elements.head.toInt
      val mappings = generate_mappings(elements.tail.tail)
      //      val cat_labels = labels.map(label => if (categories.get(label).contains(selected_cat)) 1 else -1)
      (label, (mappings, if (categories.value.toMap.get(label).contains(selected_cat)) 1 else -1))
    })*/
  }

  def load_sample_reuters_data(sc: SparkContext, train_path: String, topics_path: String, test_paths: List[String], selected_cat: String, train: Boolean) = {
    if (train) {
      val source = sc.textFile(train_path).take(20)
      generate_labelled_data(sc, sc.parallelize(source), topics_path, selected_cat)
    }
    else {
      val res = sc.emptyRDD[(Int, (Map[Int, Float], Int))]
      for (path <- test_paths) {
        val source = sc.textFile(path).take(20)
        res.union(generate_labelled_data(sc, sc.parallelize(source), topics_path, selected_cat))
      }
      res
    }
  }

  def load_reuters_data(sc: SparkContext, train_path: String, topics_path: String, test_paths: List[String], selected_cat: String, train: Boolean) = {
    if (train) {
      val source = sc.textFile(train_path)
      generate_labelled_data(sc, source, topics_path, selected_cat)
    }
    else {
      val res = sc.emptyRDD[(Int, (Map[Int, Float], Int))]
      for (path <- test_paths) {
        val source = sc.textFile(path)
        res.union(generate_labelled_data(sc, source, topics_path, selected_cat))
      }
      res
    }
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


object SGD {

  def compute_gradient(X: Map[Int, Float], y: Array[Double], wsub: Array[Double], regParam: Double) = {
    X.map(x => x._2 * (y - X.map(x => wsub(x._1) * x._2).sum))
  }
}

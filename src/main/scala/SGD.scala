object SGD {

  def sgd_subset(train_XY: Vector[(Int, (Map[Int, Float], Int))], W: Vector[Double], batch_size: Int, alpha: Double, regParam: Double) =  {
    /*    Computes stochastic gradient descent for a partition (in memory) */

    val N = train_XY.length
    val D = 47236

    val wsub = W
    //wsub += W

    val grads = train_XY.flatMap(xy => compute_gradient(xy._2._1, xy._2._2, wsub, regParam, N)).sum
    wsub.map(i => i - alpha * grads/batch_size)
  }

  def is_support(yn: Double, xn: Map[Int, Float], w: Vector[Double]) = {
    xn.map(x => w(x._1) * x._2).sum < 1
  }

  def compute_gradient(xn: Map[Int, Float], yn: Double, wsub: Vector[Double], regParam: Double, N: Double) = {
    val grad = xn.mapValues(xi => N * {if(is_support(yn, xn, wsub)) xi * -yn else 0})
    grad.map(x => x._2 + regParam*wsub(x._1)).toVector
  }

}

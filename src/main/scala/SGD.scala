object SGD {

  def sgd_subset(train_XY: Vector[(Int, (Map[Int, Float], Int))], W: Vector[Double], batch_size: Int, alpha: Double, regParam: Double) =  {
    /*    Computes stochastic gradient descent for a partition (in memory) */

    val N = train_XY.length
    val D = 47237

    val wsub = W

    val grads = train_XY.map(xy => compute_gradient(xy._2._1, xy._2._2, wsub, regParam, D))
      .reduce((a,b) => (a,b).zipped.map(_+_))
    grads

    //wsub.map(i => i - alpha * grads/batch_size)
  }

  def compute_gradient(xn: Map[Int, Float], yn: Double, wsub: Vector[Double], regParam: Double, D: Int) = {

    val grad_tmp = {for(i <- 0 until D)
      yield (i, 0.0f)}.toMap

    val grad = grad_tmp.map(g => {
      if (xn.contains(g._1) && is_support(yn, xn, wsub)) xn.get(g._1).get * -yn
      else 0} + regParam * wsub(g._1)).toVector

    grad
  }

  def is_support(yn: Double, xn: Map[Int, Float], w: Vector[Double]) = {
    yn * xn.map(x => w(x._1) * x._2).sum < 1
  }

  def compute_loss(train_XY: Vector[(Int, (Map[Int, Float], Int))], W: Vector[Double], regParam: Double) = {
    val wsub = W
    val loss = hinge_loss(train_XY, wsub).sum
    val reg = wsub.map(w => w * w).sum * regParam / 2
    reg + loss
  }

  def hinge_loss(XY: Vector[(Int, (Map[Int, Float], Int))], w: Vector[Double]) = {
    XY.map(v => {
      val xy = v._2
      val y = xy._2
      val x = xy._1

      val res = 1 - y * x.map(m => m._2 * w(m._1)).sum

      if (res < 0)
        0
      else
        res
    })
  }

}

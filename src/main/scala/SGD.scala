object SGD {

  def sgd_subset(train_XY: Vector[(Int, (Map[Int, Float], Int))], W: Vector[Double], batch_size: Int, alpha: Double, regParam: Double) =  {
    /*    Computes stochastic gradient descent for a partition (in memory) */

    val N = train_XY.length
    val D = 47237

    val wsub = W

    val grads = train_XY.map(xy => compute_gradient(xy._2._1, xy._2._2, wsub, regParam, N))
      .reduce((a,b) => (a,b).zipped.map(_+_))
    grads

    //wsub.map(i => i - alpha * grads/batch_size)
  }

  def compute_gradient(xn: Map[Int, Float], yn: Double, wsub: Vector[Double], regParam: Double, N: Double) = {
    val grad = xn.mapValues(xi => { if(is_support(yn, xn, wsub)) xi * -yn else 0})
    grad.map(x => x._2 + regParam * wsub(x._1)).toVector
  }

  def is_support(yn: Double, xn: Map[Int, Float], w: Vector[Double]) = {
    yn * xn.map(x => w(x._1) * x._2).sum < 1
  }

  def compute_loss(train_XY: Vector[(Int, (Map[Int, Float], Int))], W: Vector[Double], regParam: Double) = {
    val wsub = W
    val loss = hinge_loss(train_XY, wsub).sum
    println("Compute loss: " + loss)
    val reg = wsub.map(w => w * w).sum * regParam / 2
    println("reg: " + reg)
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

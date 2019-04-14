import org.scalatest.FunSuite

class SGDTest extends FunSuite {
  test("SGDTest.compute_gradient") {
    val xn = Map(0 -> 1.0f, 2 -> 3.0f)
    val yn = 3.0d
    val wsub = Vector[Double](5, 4, 0, 1)
    val regParam = 10e-5
    val N = 4

    assert(SGD.compute_gradient(xn, yn, wsub, regParam, N).equals(Vector(10e-5 + 3, 4*10e-5, 9, 10e-5)))
  }
}
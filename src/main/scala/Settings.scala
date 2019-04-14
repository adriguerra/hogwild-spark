object Settings {

  val seed = 42
  val train_proportion = 0.9
  val D = 47237
  val N = 23149
  val workers = 10
  val nb_epochs = 10
  val batch_size = 128
  val alpha = 0.03 * (100.0 / batch_size) / workers
  val regParam = 1e-5

}

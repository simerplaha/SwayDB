package swaydb.testkit

/**
 * Makes it a little easier to run tests on a tuple of same type using iteration.
 */
case class Tuple2Iterable[T](left: T, right: T) extends Iterable[T] {

  def toTuple: (T, T) =
    (left, right)

  override def iterator: Iterator[T] = Iterator(left, right)
}

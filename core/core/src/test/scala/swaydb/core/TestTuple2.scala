package swaydb.core

/**
 * Makes it a little easier to run tests on a tuple of same type using iteration.
 */
case class TestTuple2[T](left: T, right: T) extends Iterable[T] {

  def toTuple: (T, T) =
    (left, right)

  override def iterator: Iterator[T] = Iterator(left, right)
}

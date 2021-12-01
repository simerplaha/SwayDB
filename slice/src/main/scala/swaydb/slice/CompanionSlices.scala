package swaydb.slice

import scala.reflect.ClassTag

/**
 * Companion implementation for [[Slices]].
 *
 * This is a trait because the [[Slices]] class itself is getting too
 * long even though inheritance such as like this is discouraged.
 */
trait CompanionSlices {

  @inline def apply[A: ClassTag](data: A*): Slices[A] =
    new Slices(Array(Slice[A](data: _*)))

  @inline def apply[A: ClassTag](slices: Array[Slice[A]]): Slices[A] =
    new Slices(slices)

}

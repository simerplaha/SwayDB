/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.utils

/**
 * Useful for reducing allocations when using `Option[(L, R)]` which when contains a value requires double allocations
 *  - 1 for scala.Some`
 *  - And 1 for `Tuple2[L, R]`
 */
private[swaydb] sealed trait TupleOrNone[+L, +R] extends SomeOrNoneCovariant[TupleOrNone[L, R], TupleOrNone.Some[L, R]] {
  override def noneC: TupleOrNone[Nothing, Nothing] = TupleOrNone.None

  def toOption(): Option[(L, R)]

}

private[swaydb] case object TupleOrNone {
  final case object None extends TupleOrNone[Nothing, Nothing] {
    override def isNoneC: Boolean = true
    override def getC: Some[Nothing, Nothing] = throw new Exception(s"${TupleOrNone.productPrefix} is of type ${None.productPrefix}")
    override def toOption(): Option[(Nothing, Nothing)] = scala.None
  }

  case class Some[+L, +R](left: L, right: R) extends TupleOrNone[L, R] {
    override def isNoneC: Boolean = false
    override def getC: Some[L, R] = this
    override def toOption(): Option[(L, R)] = scala.Some((left, right))
  }
}

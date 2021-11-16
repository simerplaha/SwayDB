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

package swaydb.java

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Pair {

  implicit class PairImplicit[K](pair: Pair[K, java.time.Duration]) {
    @inline final def toScala: (K, scala.concurrent.duration.Deadline) =
      (pair.left, pair.right.toScala.fromNow)

    @inline final def asScala: (K, FiniteDuration) =
      (pair.left, pair.right.toScala)

    @inline final def asScalaDeadline: (K, Deadline) =
      (pair.left, pair.right.toScala.fromNow)
  }

  implicit class TupleImplicits[L, R](tuple: (L, R)) {
    @inline final def toPair: Pair[L, R] =
      Pair(tuple._1, tuple._2)
  }

  @inline final def apply[L, R](tuple: (L, R)): Pair[L, R] =
    new Pair(tuple._1, tuple._2)

  @inline final def apply[L, R](left: L, right: R): Pair[L, R] =
    new Pair(left, right)

  @inline final def of[L, R](left: L, right: R): Pair[L, R] =
    new Pair(left, right)

  @inline final def of[I](leftAndRight: I): Pair[I, I] =
    new Pair(leftAndRight, leftAndRight)
}

class Pair[+L, +R](val left: L, val right: R) {
  def toTuple: (L, R) =
    (left, right)

  def toKeyVal =
    KeyVal(left, right)

  override def equals(other: Any): Boolean =
    other match {
      case other: Pair[L, R] =>
        left == other.left && right == other.right

      //      case other: KeyVal[L, R] =>
      //        left == other.left && right == other.right

      case _ =>
        false
    }

  override def hashCode(): Int =
    toTuple.hashCode()

  override def toString: String =
    s"Pair(left = $left, right = $right)"
}

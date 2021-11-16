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

object KeyVal {
  def apply[K, V](keyVal: (K, V)): KeyVal[K, V] =
    new KeyVal(keyVal._1, keyVal._2)

  def of[K, V](key: K, value: V): KeyVal[K, V] =
    new KeyVal(key, value)

  def of[T](keyAndVal: T): KeyVal[T, T] =
    new KeyVal(keyAndVal, keyAndVal)

  implicit class TupleImplicits[K, V](tuple: (K, V)) {
    @inline final def asKeyVal: KeyVal[K, V] =
      KeyVal(tuple._1, tuple._2)

    @inline final def asPair: Pair[K, V] =
      Pair(tuple._1, tuple._2)
  }
}

case class KeyVal[+K, +V](key: K, value: V) extends Pair(key, value) {
  override def equals(other: Any): Boolean =
    other match {
      case other: KeyVal[K, V] =>
        left == other.left && right == other.right

      case _ =>
        false
    }

  override def toString: String =
    s"KeyVal(key = $key, value = $value)"
}

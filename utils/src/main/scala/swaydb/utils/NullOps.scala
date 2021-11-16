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

object NullOps {

  def find[A >: Null](array: Array[A], p: A => Boolean): A = {
    var i = 0
    while (i < array.length) {
      val item = array(i)
      if (p(item))
        return item
      else
        i += 1
    }

    null
  }

  @inline final def map[A, T >: Null](value: A, f: A => T): T =
    if (value == null)
      null
    else
      f(value)

  @inline final def foreach[A](value: A, f: A => Unit): Unit =
    if (value != null)
      f(value)

  @inline final def foldLeft[A, T](initial: T)(value: A, f: (T, A) => T): T =
    if (value == null)
      initial
    else
      f(initial, value)

  @inline final def getOrElse[A](value: A, or: => A): A =
    if (value == null)
      or
    else
      value

  @inline final def forall[A](item: A, condition: A => Boolean): Boolean =
    item == null || condition(item)

  @inline final def tryOrNull[T >: Null](f: => T): T =
    try
      f
    catch {
      case _: Exception =>
        null
    }
}

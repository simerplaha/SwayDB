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

private[swaydb] object Options {

  val `false`: Option[Boolean] = Some(false)
  val `true`: Option[Boolean] = Some(true)
  val unit: Some[Unit] = Some(())
  val zero: Some[Int] = Some(0)
  val one: Some[Int] = Some(1)

  @inline final def when[T](condition: Boolean)(success: => Option[T]): Option[T] =
    if (condition)
      success
    else
      None

  @inline final def when[T](condition: Boolean, none: T)(success: => T): T =
    if (condition)
      success
    else
      none

  implicit class OptionsImplicits[A](option: Option[A]) {
    @inline final def valueOrElse[B](value: A => B, orElse: => B): B =
      Options.valueOrElse[A, B](option, value, orElse)
  }

  @inline final def valueOrElse[A, B](option: Option[A], value: A => B, orElse: => B): B =
    if (option.isDefined)
      value(option.get)
    else
      orElse

  def nullCheck[V](option: Option[V]): Option[V] =
    option match {
      case Some(value) =>
        if (value == null)
          None
        else
          option

      case None =>
        None
    }
}

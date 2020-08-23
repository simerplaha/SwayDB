/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.util

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

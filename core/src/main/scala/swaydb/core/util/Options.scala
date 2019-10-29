/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.util

private[swaydb] object Options {

  val `false`: Option[Boolean] = Some(false)
  val `true`: Option[Boolean] = Some(true)
  val unit: Option[Unit] = Some(())
  val zero: Option[Int] = Option(0)
  val one: Option[Int] = Option(1)

  def when[T](condition: Boolean)(success: => Option[T]): Option[T] =
    if (condition)
      success
    else
      None

  implicit class OptionsImplicits[A](option: Option[A]) {
    @inline def valueOrElse[B](value: A => B, orElse: => B): B =
      if (option.isDefined)
        value(option.get)
      else
        orElse
  }
}

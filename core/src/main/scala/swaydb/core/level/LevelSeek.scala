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
 */

package swaydb.core.level

import swaydb.IO

private[level] sealed trait LevelSeek[+A] {
  def isDefined: Boolean
  def isEmpty: Boolean

  def map[B](f: A => B): LevelSeek[B]

  def flatMap[B](f: A => LevelSeek[B]): LevelSeek[B]
}

private[level] object LevelSeek {

  val none = IO.Right[Nothing, LevelSeek.None.type](LevelSeek.None)(IO.ExceptionHandler.Nothing)

  def apply[T](segmentId: Long,
               result: Option[T]): LevelSeek[T] =
    if (result.isDefined)
      LevelSeek.Some(
        segmentId = segmentId,
        result = result.get
      )
    else
      LevelSeek.None

  final case class Some[T](segmentId: Long,
                           result: T) extends LevelSeek[T] {
    override def isDefined: Boolean = true
    override def isEmpty: Boolean = false
    override def map[B](f: T => B): LevelSeek[B] =
      copy(
        segmentId = segmentId,
        result = f(result)
      )

    override def flatMap[B](f: T => LevelSeek[B]): LevelSeek[B] =
      f(result)
  }

  final case object None extends LevelSeek[Nothing] {
    override def isDefined: Boolean = false
    override def isEmpty: Boolean = true
    override def map[B](f: Nothing => B): LevelSeek[B] = this
    override def flatMap[B](f: Nothing => LevelSeek[B]): LevelSeek[B] = this
  }
}
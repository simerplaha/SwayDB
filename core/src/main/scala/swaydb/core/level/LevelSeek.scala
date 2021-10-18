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

package swaydb.core.level

import swaydb.IO

/**
 * Search result of a [[Level]].
 */
private[core] sealed trait LevelSeek[+A] {
  def isDefined: Boolean
  def isEmpty: Boolean

  def map[B](f: A => B): LevelSeek[B]

  def flatMap[B](f: A => LevelSeek[B]): LevelSeek[B]
}

private[core] object LevelSeek {

  val none = IO.Right[Nothing, LevelSeek.None.type](LevelSeek.None)(IO.ExceptionHandler.Nothing)

  def apply[T](segmentNumber: Long,
               result: Option[T]): LevelSeek[T] =
    if (result.isDefined)
      LevelSeek.Some(
        segmentNumber = segmentNumber,
        result = result.get
      )
    else
      LevelSeek.None

  final case class Some[T](segmentNumber: Long,
                           result: T) extends LevelSeek[T] {
    override def isDefined: Boolean = true
    override def isEmpty: Boolean = false
    override def map[B](f: T => B): LevelSeek[B] =
      copy(
        segmentNumber = segmentNumber,
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

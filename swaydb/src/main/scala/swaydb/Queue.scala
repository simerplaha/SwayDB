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

package swaydb

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.concurrent.duration.{Deadline, FiniteDuration}

case class Queue[A](private val map: SetMap[Long, A, Nothing, Bag.Less],
                    private val pushIds: AtomicLong,
                    private val popIds: AtomicLong) {

  private val nullA = null.asInstanceOf[A]

  def push(elem: A): OK =
    map.put(pushIds.getAndIncrement(), elem)

  def push(elem: A, expireAfter: FiniteDuration): OK =
    map.put(pushIds.getAndIncrement(), elem, expireAfter.fromNow)

  def push(elem: A, expireAt: Deadline): OK =
    map.put(pushIds.getAndIncrement(), elem, expireAt)

  def push(keyValues: A*): OK =
    push(keyValues)

  def push(keyValues: Stream[A]): OK =
    map.put {
      keyValues.map {
        item =>
          (pushIds.getAndIncrement(), item)
      }
    }

  def push(keyValues: Iterable[A]): OK =
    push(keyValues.iterator)

  def push(keyValues: Iterator[A]): OK =
    map.put {
      keyValues.map {
        item =>
          (pushIds.getAndIncrement(), item)
      }
    }

  def pop(): Option[A] =
    Option(popOrElse(nullA))

  def popOrNull(): A =
    popOrElse(nullA)

  @tailrec
  final def popOrElse[B <: A](other: => B): A =
    if (popIds.get() < pushIds.get()) {
      val jobId = popIds.getAndIncrement()
      //todo - handle failure if getKeyValue fails.
      map.getKeyValue(jobId) match {
        case Some((key, value)) =>
          map.remove(key)
          value

        case None =>
          val headOrNull = map.headOrNull
          if (headOrNull == null) {
            other
          } else {
            popIds.compareAndSet(jobId, headOrNull._1)
            popOrElse(other)
          }
      }
    } else {
      other
    }

  def stream: Stream[A] =
    map
      .stream
      .map(_._2)
}

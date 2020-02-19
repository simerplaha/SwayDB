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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.concurrent.duration.{Deadline, FiniteDuration}

case class Queue[A](private val map: SetMap[Long, A, Nothing, Bag.Less],
                    private val pushIds: AtomicLong,
                    private val popIds: AtomicLong) extends LazyLogging {

  private val nullA = null.asInstanceOf[A]

  /**
   * Stores all failed items that get processed first before picking
   * next task in [[map]].
   */
  private val retryQueue = new ConcurrentLinkedQueue[java.lang.Long]()

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

  /**
   * Safely pick the next job.
   */
  @inline private def popAndRecover(nextId: Long): Option[A] =
    try
      map.get(nextId) match {
        case some @ Some(_) =>
          map.remove(nextId)
          some

        case None =>
          None
      }
    catch {
      case throwable: Throwable =>
        retryQueue add nextId
        logger.error(s"Failed to process taskId: $nextId", throwable)
        None
    }

  /**
   * If threads were racing forward than there were actual items to process, then reset
   * [[popIds]] so that the race/overflow is controlled and pushed back to the last
   * queued job.
   */
  @inline private def brakeRecover(nextId: Long): Boolean =
    popIds.get() == nextId && {
      try {
        val headOrNull = map.headOrNull
        //Only the last thread that accessed popIds can reset the popIds counter and continue
        //processing to avoid any conflicting updates.
        headOrNull != null && popIds.compareAndSet(nextId, headOrNull._1)
      } catch {
        case throwable: Throwable =>
          logger.error(s"Failed to brakeRecover taskId: $nextId", throwable)
          false
      }
    }

  @tailrec
  final def popOrElse[B <: A](other: => B): A =
    if (popIds.get() < pushIds.get()) {
      //check if there is a previously failed job to process
      val retryId = retryQueue.poll()

      val nextId: Long =
        if (retryId == null)
          popIds.getAndIncrement() //pick next job
        else
        retryId

      //pop the next job from the map safely.
      popAndRecover(nextId) match {
        case Some(value) =>
          value

        case None =>
          if (brakeRecover(nextId))
            popOrElse(other)
          else
            other
      }
    } else {
      other
    }

  def stream: Stream[A] =
    map
      .stream
      .map(_._2)
}

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
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.annotation.tailrec
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Queue {
  /**
   * Combines two serialisers into a single Serialiser.
   */
  def serialiser[A](bSerializer: Serializer[A]): Serializer[(Long, A)] =
    new Serializer[(Long, A)] {
      override def write(data: (Long, A)): Slice[Byte] = {
        val valueBytes =
          if (data._2 == null)
            Slice.emptyBytes //value can be null when
          else
          bSerializer.write(data._2)

        Slice
          .create[Byte](Bytes.sizeOfUnsignedLong(data._1) + Bytes.sizeOfUnsignedInt(valueBytes.size) + valueBytes.size)
          .addUnsignedLong(data._1)
          .addUnsignedInt(valueBytes.size)
          .addAll(valueBytes)
      }

      override def read(data: Slice[Byte]): (Long, A) = {
        val reader = data.createReader()

        val key = reader.readUnsignedLong()

        val valuesBytes = reader.read(reader.readUnsignedInt())
        val value = bSerializer.read(valuesBytes)

        (key, value)
      }
    }

  /**
   * Partial ordering based on [[SetMap.serialiser]].
   */
  def ordering: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      override def compare(left: Slice[Byte], right: Slice[Byte]): Int =
        left.readUnsignedLong() compare right.readUnsignedLong()

      private[swaydb] override def comparableKey(key: Slice[Byte]): Slice[Byte] = {
        val (_, byteSize) = key.readUnsignedLongWithByteSize()
        key.take(byteSize)
      }
    }
}

case class Queue[A] private(private val set: Set[(Long, A), Nothing, Bag.Less],
                            private val pushIds: AtomicLong,
                            private val popIds: AtomicLong) extends LazyLogging {

  private val nullA = null.asInstanceOf[A]

  /**
   * Stores all failed items that get processed first before picking
   * next task in [[set]].
   */
  private val retryQueue = new ConcurrentLinkedQueue[java.lang.Long]()

  def push(elem: A): OK =
    set.add((pushIds.getAndIncrement(), elem))

  def push(elem: A, expireAfter: FiniteDuration): OK =
    set.add((pushIds.getAndIncrement(), elem), expireAfter.fromNow)

  def push(elem: A, expireAt: Deadline): OK =
    set.add((pushIds.getAndIncrement(), elem), expireAt)

  def push(elems: A*): OK =
    push(elems)

  def push(elems: Stream[A]): OK =
    set.add {
      elems.map {
        item =>
          (pushIds.getAndIncrement(), item)
      }
    }

  def push(elems: Iterable[A]): OK =
    push(elems.iterator)

  def push(elems: Iterator[A]): OK =
    set.add {
      elems.map {
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
  @inline private def popAndRecoverOrNull(nextId: Long): A =
    try
      set.get((nextId, nullA)) match {
        case Some(keyValue) =>
          set.remove(keyValue)
          keyValue._2

        case None =>
          nullA
      }
    catch {
      case throwable: Throwable =>
        retryQueue add nextId
        logger.error(s"Failed to process taskId: $nextId", throwable)
        nullA
    }

  /**
   * If threads were racing forward than there were actual items to process, then reset
   * [[popIds]] so that the race/overflow is controlled and pushed back to the last
   * queued job.
   */
  @inline private def brakeRecover(nextId: Long): Boolean =
    popIds.get() == nextId && {
      try {
        val headOrNull = set.headOrNull
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
  final def popOrElse[B <: A](orElse: => B): A =
    if (popIds.get() < pushIds.get()) {
      //check if there is a previously failed job to process
      val retryId = retryQueue.poll()

      val nextId: Long =
        if (retryId == null)
          popIds.getAndIncrement() //pick next job
        else
          retryId

      //pop the next job from the map safely.
      val valueOrNull = popAndRecoverOrNull(nextId)

      if (valueOrNull == null)
        if (brakeRecover(nextId + 1))
          popOrElse(orElse)
        else
          orElse
      else
        valueOrNull
    } else {
      orElse
    }

  def stream: Stream[A] =
    set
      .stream
      .map(_._2)

  def close(): Unit =
    set.close()

  def delete(): Unit =
    set.delete()
}

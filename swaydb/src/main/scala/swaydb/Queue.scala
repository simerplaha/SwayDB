/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Bytes
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.stream.StreamFree
import swaydb.serializers.Serializer

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Queue {

  private[swaydb] def fromSet[A, BAG[_]](set: swaydb.Set[(Long, A), Nothing, BAG])(implicit bag: Bag[BAG]): BAG[Queue[A]] =
    bag.flatMap(set.head) {
      headOption =>
        bag.map(set.last) {
          lastOption =>

            val first: Long =
              headOption match {
                case Some((first, _)) =>
                  first

                case None =>
                  0
              }

            val last: Long =
              lastOption match {
                case Some((used, _)) =>
                  used + 1

                case None =>
                  0
              }

            swaydb.Queue(
              set = set.toBag[Glass],
              pushIds = new AtomicLong(last),
              popIds = new AtomicLong(first)
            )
        }
    }

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
          .of[Byte](Bytes.sizeOfUnsignedLong(data._1) + Bytes.sizeOfUnsignedInt(valueBytes.size) + valueBytes.size)
          .addUnsignedLong(data._1)
          .addUnsignedInt(valueBytes.size)
          .addAll(valueBytes)
      }

      override def read(slice: Slice[Byte]): (Long, A) = {
        val reader = slice.createReader()

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

/**
 * Provides a [[Set]] instance the ability to be used as a queue.
 */
case class Queue[A] private(private val set: Set[(Long, A), Nothing, Glass],
                            private val pushIds: AtomicLong,
                            private val popIds: AtomicLong) extends Stream[A, Glass]()(Bag.glass) with LazyLogging {

  private val nullA = null.asInstanceOf[A]

  /**
   * Stores all failed items that get processed first before picking
   * next task from [[set]].
   */
  private val retryQueue = new ConcurrentLinkedQueue[java.lang.Long]()

  def path: Path =
    set.path

  def push(elem: A): OK =
    set.add((pushIds.getAndIncrement(), elem))

  def push(elem: A, expireAfter: FiniteDuration): OK =
    set.add((pushIds.getAndIncrement(), elem), expireAfter.fromNow)

  def push(elem: A, expireAt: Deadline): OK =
    set.add((pushIds.getAndIncrement(), elem), expireAt)

  def push(elems: A*): OK =
    push(elems)

  def push(elems: Stream[A, Glass]): OK =
    set.add {
      elems.map {
        item =>
          (pushIds.getAndIncrement(), item)
      }
    }

  def push(elems: IterableOnce[A]): OK =
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
        logger.warn(s"Failed to process taskId: $nextId. Added it to retry queue.", throwable)
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

  override private[swaydb] def free: StreamFree[A] =
    set.free.map(_._2)

  private def copy(): Unit = ()

  def levelZeroMeter: LevelZeroMeter =
    set.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    set.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    set.sizeOfSegments

  def blockCacheSize(): Option[Long] =
    set.blockCacheSize()

  def cachedKeyValuesSize(): Option[Long] =
    set.cachedKeyValuesSize()

  def openedFiles(): Option[Long] =
    set.openedFiles()

  def pendingDeletes(): Option[Long] =
    set.pendingDeletes()

  def close(): Unit =
    set.close()

  def delete(): Unit =
    set.delete()

  override def equals(other: Any): Boolean =
    other match {
      case other: Queue[_] =>
        other.path == this.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()

  override def toString(): String =
    s"Queue(path = $path)"
}

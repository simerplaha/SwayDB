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

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.Segment
import swaydb.data.Reserve
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

/**
  * Reserves a range of keys for processing by a single thread.
  *
  * This is used to ensure that multiple threads do not concurrent perform compaction on overlapping keys within
  * the same Level.
  */
object ReserveRange extends LazyLogging {

  case class Range[T](from: Slice[Byte], to: Slice[Byte], toInclusive: Boolean, reserve: Reserve[T])
  case class State[T](ranges: ListBuffer[Range[T]])

  def create[T](): State[T] =
    State(ListBuffer.empty)

  def get[T](from: Slice[Byte],
             to: Slice[Byte])(implicit state: State[T],
                              ordering: KeyOrder[Slice[Byte]]): Option[T] = {
    import ordering._
    state.synchronized {
      state
        .ranges
        .find(range => from.equiv(range.from) && to.equiv(range.to))
        .flatMap(_.reserve.info)
    }
  }

  def reserveOrGet[T](from: Slice[Byte],
                      to: Slice[Byte],
                      toInclusive: Boolean,
                      info: T)(implicit state: State[T],
                               ordering: KeyOrder[Slice[Byte]]): Option[T] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        toInclusive = toInclusive,
        info = info
      ) match {
        case Left(range) =>
          range.reserve.info

        case Right(_) =>
          None
      }
    }

  def reserveOrListen[T](from: Slice[Byte],
                         to: Slice[Byte],
                         toInclusive: Boolean,
                         info: T)(implicit state: State[T],
                                  ordering: KeyOrder[Slice[Byte]]): Either[Future[Unit], Slice[Byte]] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        toInclusive = toInclusive,
        info = info
      ) match {
        case Left(range) =>
          val promise = Promise[Unit]()
          range.reserve.savePromise(promise)
          Left(promise.future)

        case Right(value) =>
          Right(value)
      }
    }

  def free[T](from: Slice[Byte])(implicit state: State[T],
                                 ordering: KeyOrder[Slice[Byte]]): Unit =
    state.synchronized {
      import ordering._
      state
        .ranges
        .find(from equiv _.from)
        .foreach {
          range =>
            state.ranges -= range
            Reserve.setFree(range.reserve)
        }
    }

  def isUnreserved[T](from: Slice[Byte],
                      to: Slice[Byte],
                      toInclusive: Boolean)(implicit state: State[T],
                                            ordering: KeyOrder[Slice[Byte]]): Boolean =
    state
      .ranges
      .forall {
        range =>
          !Slice.intersects((range.from, range.to, range.toInclusive), (from, to, toInclusive))
      }

  def isUnreserved[T](segment: Segment)(implicit state: State[T],
                                        ordering: KeyOrder[Slice[Byte]]): Boolean =
    isUnreserved(
      from = segment.minKey,
      to = segment.maxKey.maxKey,
      toInclusive = segment.maxKey.inclusive
    )

  private def reserveOrGetRange[T](from: Slice[Byte],
                                   to: Slice[Byte],
                                   toInclusive: Boolean,
                                   info: T)(implicit state: State[T],
                                            ordering: KeyOrder[Slice[Byte]]): Either[Range[T], Slice[Byte]] =
    state.synchronized {
      state
        .ranges
        .find(range => Slice.intersects((from, to, toInclusive), (range.from, range.to, range.toInclusive)))
        .map(Left(_))
        .getOrElse {
          state.ranges += ReserveRange.Range(from, to, toInclusive, Reserve(info))
          val waitingCount = state.ranges.size
          //Helps debug situations if too many threads and try to compact into the same Segment.
          if (waitingCount >= 100) logger.warn(s"Too many listeners: $waitingCount")
          Right(from)
        }
    }
}

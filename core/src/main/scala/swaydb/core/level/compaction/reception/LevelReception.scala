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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.compaction.reception

import swaydb.Error.Level.ExceptionHandler
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner}
import swaydb.core.util.AtomicRanges
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.concurrent.{Future, Promise}

private[core] sealed trait LevelReception[-A] {

  def reserve(item: A,
              levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]]

}

private[core] case object LevelReception {

  /**
   * Tries to reserve input [[Segment]]s for merge.
   *
   * @return Either a Promise which is complete when this Segment becomes available or returns the key which
   *         can be used to free the Segment.
   */
  implicit object SegmentReception extends LevelReception[Iterable[Assignable.Collection]] {

    override def reserve(segments: Iterable[Assignable.Collection],
                         levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                           keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] = {
      IO {
        SegmentAssigner.assignMinMaxOnlyUnsafeNoGaps(
          inputSegments = segments,
          targetSegments = levelSegments
        )
      } map {
        assigned =>
          Segment.minMaxKey(
            left = assigned,
            right = segments
          ) match {
            case Some((minKey, maxKey, toInclusive)) =>
              reservations.writeOrPromise(
                fromKey = minKey,
                toKey = maxKey,
                toKeyInclusive = toInclusive
              )

            case None =>
              scala.Left(Promise.successful(()))
          }
      }
    }
  }

  /**
   * Tries to reserve input [[Map]]s for merge.
   *
   * @return Either a Promise which is complete when this Map becomes available or returns the key which
   *         can be used to free the Map.
   *
   */
  implicit object MapReception extends LevelReception[Map[Slice[Byte], Memory, LevelZeroMapCache]] {
    override def reserve(map: Map[Slice[Byte], Memory, LevelZeroMapCache],
                         levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                           keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] =
      IO {
        SegmentAssigner.assignMinMaxOnlyUnsafeNoGaps(
          input = map.cache.skipList,
          targetSegments = levelSegments
        )
      } map {
        assigned =>
          Segment.minMaxKey(
            left = assigned,
            right = map.cache.skipList
          ) match {
            case Some((minKey, maxKey, toInclusive)) =>
              reservations.writeOrPromise(
                fromKey = minKey,
                toKey = maxKey,
                toKeyInclusive = toInclusive
              )

            case None =>
              scala.Left(Promise.successful(()))
          }
      }
  }

  @inline def validateReservations[A](reservationKey: AtomicRanges.Key[Slice[Byte]],
                                      collections: Iterable[Assignable.Collection])(f: => Future[A])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                                     keyOrder: KeyOrder[Slice[Byte]]): Future[A] = {
    import keyOrder._

    if (!reservations.contains(reservationKey))
      Future.failed(new Exception("Key is not reserved."))
    else if (reservationKey.fromKey > collections.head.key)
      Future.failed(new Exception("Incorrect reservation. MinKey is lesser."))
    else if (reservationKey.toKey < collections.head.maxKey.maxKey)
      Future.failed(new Exception("Incorrect reservation. MaxKey is lesser."))
    else
      f
  }

  @inline def validateMapReservation[A](reservationKey: AtomicRanges.Key[Slice[Byte]],
                                        map: Map[Slice[Byte], Memory, LevelZeroMapCache])(f: => Future[A])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                                           keyOrder: KeyOrder[Slice[Byte]]): Future[A] = {
    import keyOrder._

    if (!reservations.contains(reservationKey))
      Future.failed(new Exception("Key is not reserved."))
    else if (reservationKey.fromKey > map.cache.skipList.headKey.getC)
      Future.failed(new Exception("Incorrect reservation. MinKey is lesser."))
    else if (reservationKey.toKey < map.cache.skipList.last().getS.toKey)
      Future.failed(new Exception("Incorrect reservation. MaxKey is lesser."))
    else
      f
  }

  @inline def validateReservation[E: IO.ExceptionHandler, A](reservationKey: AtomicRanges.Key[Slice[Byte]],
                                                             collection: Assignable.Collection)(f: => IO[E, A])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                                                keyOrder: KeyOrder[Slice[Byte]]): IO[E, A] = {
    import keyOrder._

    if (!reservations.contains(reservationKey))
      IO.failed[E, A]("Key is not reserved.")
    else if (reservationKey.fromKey > collection.key)
      IO.failed[E, A]("Incorrect reservation. MinKey is lesser.")
    else if (reservationKey.toKey < collection.maxKey.maxKey)
      IO.failed[E, A]("Incorrect reservation. MaxKey is lesser.")
    else
      f
  }

  /**
   * Reserves the input [[I]] and executes the block if reservation was successful.
   */
  @inline def reserve[I](input: I,
                         levelSegments: Iterable[Segment])(implicit reception: LevelReception[I],
                                                           reservations: AtomicRanges[Slice[Byte]],
                                                           keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] =
    reception.reserve(
      item = input,
      levelSegments = levelSegments
    )
}

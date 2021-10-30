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

package swaydb.core.level.compaction.reception

import swaydb.Error.Level.ExceptionHandler
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, Assigner}
import swaydb.core.util.AtomicRanges
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.concurrent.Promise

private[core] sealed trait LevelReception[-A] {

  def reserve(item: A,
              levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]]

}

private[core] case object LevelReception {

  /**
   * Tries to reserve input [[Segment]]s for merge.
   *
   * @return Either a Promise which is completed when this Segment becomes available or returns the key which
   *         can be used to free the Segment.
   */
  implicit object AssignableCollectionReception extends LevelReception[Iterable[Assignable.Collection]] {

    override def reserve(segments: Iterable[Assignable.Collection],
                         levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                           keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] = {
      IO {
        Assigner.assignMinMaxOnlyUnsafeNoGaps(
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
              scala.Left(Promise.failed(throw new Exception(s"Failed to reserve assigned size: ${assigned.size}")))
          }
      }
    }
  }

  /**
   * Tries to reserve input [[swaydb.core.log.Log]]s for merge.
   *
   * @return Either a Promise which is complete when this Map becomes available or returns the key which
   *         can be used to free the Map.
   *
   */
  implicit object MapReception extends LevelReception[LevelZeroLog] {
    override def reserve(map: LevelZeroLog,
                         levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                           keyOrder: KeyOrder[Slice[Byte]]): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] =
      IO {
        Assigner.assignMinMaxOnlyUnsafeNoGaps(
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
              scala.Left(Promise.failed(throw new Exception(s"Failed to reserve assigned size: ${assigned.size}")))
          }
      }
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

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

import swaydb.Exception.InvalidLevelReservation
import swaydb.IO
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.segment.assigner.Assignable
import swaydb.core.util.AtomicRanges
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.concurrent.Future

private[core] sealed trait LevelReceptionKeyValidator[-A] {

  protected def validateOrNull(reservedItems: A,
                               reservationKey: AtomicRanges.Key[Slice[Byte]])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                              keyOrder: KeyOrder[Slice[Byte]]): InvalidLevelReservation

}

private[core] case object LevelReceptionKeyValidator {

  /**
   * Allow all keys that are within the reservationKey's range.
   *
   * @return null if validation passes else returns Exception.
   *         Why null? Because the validation logic is private
   *         to this class and we don't want to create objects.
   */
  @inline private def validateOrNull(minKey: Slice[Byte],
                                     maxKey: MaxKey[Slice[Byte]],
                                     reservationKey: AtomicRanges.Key[Slice[Byte]])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                    keyOrder: KeyOrder[Slice[Byte]]): InvalidLevelReservation = {
    import keyOrder._

    if (!reservations.containsExact(reservationKey))
      InvalidLevelReservation("Key is not reserved.")
    else if (reservationKey.fromKey > minKey)
      InvalidLevelReservation("Incorrect reservation. MinKey is lesser.")
    else if (reservationKey.toKeyInclusive) //reservationKey is inclusive the maxKey's inclusivity is not important
      if (maxKey.maxKey <= reservationKey.toKey)
        null
      else
        InvalidLevelReservation("Incorrect reservation. MaxKey is invalid.")
    else if (maxKey.inclusive) //reservationKey is exclusive and maxKey is inclusive so maxKey should be less than toKey
      if (maxKey.maxKey < reservationKey.toKey)
        null
      else
        InvalidLevelReservation("Incorrect reservation. MaxKey is invalid.")
    else //reservationKey and maxKey both are exclusive
      if (maxKey.maxKey <= reservationKey.toKey)
        null
      else
        InvalidLevelReservation("Incorrect reservation. MaxKey is invalid.")
  }

  implicit object IterableCollectionKeyValidator extends LevelReceptionKeyValidator[Iterable[Assignable.Collection]] {
    protected override def validateOrNull(collections: Iterable[Assignable.Collection],
                                          reservationKey: AtomicRanges.Key[Slice[Byte]])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                         keyOrder: KeyOrder[Slice[Byte]]): InvalidLevelReservation =
      LevelReceptionKeyValidator.validateOrNull(
        minKey = collections.head.key,
        maxKey = collections.last.maxKey,
        reservationKey = reservationKey
      )
  }

  implicit object MapKeyValidator extends LevelReceptionKeyValidator[LevelZeroLog] {
    protected override def validateOrNull(map: LevelZeroLog,
                                          reservationKey: AtomicRanges.Key[Slice[Byte]])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                         keyOrder: KeyOrder[Slice[Byte]]): InvalidLevelReservation =
      LevelReceptionKeyValidator.validateOrNull(
        minKey = map.cache.skipList.headKey.getC,
        maxKey = map.cache.skipList.last().getS.maxKey,
        reservationKey = reservationKey
      )
  }

  implicit object CollectionKeyValidator extends LevelReceptionKeyValidator[Assignable.Collection] {
    protected override def validateOrNull(collection: Assignable.Collection,
                                          reservationKey: AtomicRanges.Key[Slice[Byte]])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                                                         keyOrder: KeyOrder[Slice[Byte]]): InvalidLevelReservation =
      LevelReceptionKeyValidator.validateOrNull(
        minKey = collection.key,
        maxKey = collection.maxKey,
        reservationKey = reservationKey
      )
  }

  def validateIO[E: IO.ExceptionHandler, A, B](reservedItems: A,
                                               reservationKey: AtomicRanges.Key[Slice[Byte]])(f: => IO[E, B])(implicit validator: LevelReceptionKeyValidator[A],
                                                                                                              reservations: AtomicRanges[Slice[Byte]],
                                                                                                              keyOrder: KeyOrder[Slice[Byte]]): IO[E, B] = {
    val exception = validator.validateOrNull(reservedItems, reservationKey)
    if (exception != null)
      IO.failed[E, B](exception)
    else
      f
  }

  def validateFuture[A, B](reservedItems: A,
                           reservationKey: AtomicRanges.Key[Slice[Byte]])(f: => Future[B])(implicit validator: LevelReceptionKeyValidator[A],
                                                                                           reservations: AtomicRanges[Slice[Byte]],
                                                                                           keyOrder: KeyOrder[Slice[Byte]]): Future[B] = {
    val exception = validator.validateOrNull(reservedItems, reservationKey)
    if (exception != null)
      Future.failed(exception)
    else
      f
  }
}

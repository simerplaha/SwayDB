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

import swaydb.Exception.InvalidLevelReservation
import swaydb.IO
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.Map
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

  implicit object MapKeyValidator extends LevelReceptionKeyValidator[Map[Slice[Byte], Memory, LevelZeroMapCache]] {
    protected override def validateOrNull(map: Map[Slice[Byte], Memory, LevelZeroMapCache],
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

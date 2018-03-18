/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import swaydb.core.data.KeyValue.{FixedWriteOnly, RangeWriteOnly}
import swaydb.core.data.Transient.Range
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.Slice

import scala.util.{Success, Try}

private[core] sealed trait Transient extends KeyValue.WriteOnly {
  val value: Option[Slice[Byte]]
}

private[core] object Transient {

  object Remove {
    val id: Int = 0

    def apply(key: Slice[Byte]): Remove =
      new Remove(key, Stats(key, None, 0.1, hasRemoveRange = false, None))

    def apply(key: Slice[Byte], falsePositiveRate: Double): Remove =
      new Remove(key, Stats(key, None, falsePositiveRate, hasRemoveRange = false, None))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Remove =
      new Remove(key, Stats(key, None, falsePositiveRate, hasRemoveRange = previous.exists(_.isRemoveRange), previous))
  }

  case class Remove(key: Slice[Byte],
                    stats: Stats) extends Transient with FixedWriteOnly {
    override def id: Int = Remove.id

    override val isRemoveRange =
      false

    override val value: Option[Slice[Byte]] = None

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)

    override def updateStats(falsePositiveRate: Double,
                             keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(stats = Stats(key, None, falsePositiveRate, keyValue.exists(_.isRemoveRange), keyValue))

    override def isRemove: Boolean = true
  }

  object Put {
    val id = 1

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly]): Put =
      new Put(key, value, Stats(key, value, falsePositiveRate, previousMayBe.exists(_.isRemoveRange), previousMayBe))

    def apply(key: Slice[Byte]): Put =
      Put(key, None, Stats(key, falsePositiveRate = 0.1, hasRemoveRange = false))

    def apply(key: Slice[Byte], value: Slice[Byte]): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = 0.1, hasRemoveRange = false))

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = falsePositiveRate, hasRemoveRange = false))

    def apply(key: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, None, falsePositiveRate, previous)

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = falsePositiveRate, previous.exists(_.isRemoveRange), previous))
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 stats: Stats) extends Transient with FixedWriteOnly {

    override def id: Int = Put.id

    override val isRemoveRange =
      false

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(stats = Stats(key, value, falsePositiveRate, keyValue.exists(_.isRemoveRange), keyValue))

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)

    override def isRemove: Boolean = false
  }

  object Range {
    val id = 2

    def mergeKeys(fromKey: Slice[Byte], toKey: Slice[Byte]): Slice[Byte] = {
      val fromKeySize = ByteUtilCore.sizeUnsignedInt(fromKey.size)
      val toKeySize = ByteUtilCore.sizeUnsignedInt(toKey.size)
      val key = Slice.create[Byte](fromKeySize + fromKey.size + toKeySize + toKey.size)
      key addIntUnsigned fromKey.size
      key addAll fromKey
      key addIntUnsigned toKey.size
      key addAll toKey
    }

    def unMergeKeys(key: Slice[Byte]): Try[(Slice[Byte], Slice[Byte])] = {
      val reader = Reader(key)
      for {
        fromKeySize <- reader.readIntUnsigned()
        fromKey <- reader.read(fromKeySize)
        toKeySize <- reader.readIntUnsigned()
        toKey <- reader.read(toKeySize)
      } yield {
        (fromKey, toKey)
      }
    }

    def apply[F <: Value.Fixed, R <: Value.Fixed](fromKey: Slice[Byte],
                                                  toKey: Slice[Byte],
                                                  fromValue: Option[F],
                                                  rangeValue: R)(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range =
      Range(fromKey, toKey, fromValue, rangeValue, falsePositiveRate = 0.1, None)

    def apply[R <: Value.Fixed](fromKey: Slice[Byte],
                                toKey: Slice[Byte],
                                rangeValue: R,
                                falsePositiveRate: Double,
                                previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId((), rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write((), rangeValue, _))

      new Range(
        id = rangeId,
        fromKey = fromKey,
        toKey = toKey,
        fromValue = None,
        rangeValue = rangeValue,
        value = value,
        stats = Stats(Range.mergeKeys(fromKey, toKey), value, falsePositiveRate, rangeValue.isRemove || previous.exists(_.isRemoveRange), previous)
      )
    }

    def apply[F <: Value.Fixed, R <: Value.Fixed](fromKey: Slice[Byte],
                                                  toKey: Slice[Byte],
                                                  fromValue: Option[F],
                                                  rangeValue: R,
                                                  falsePositiveRate: Double,
                                                  previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write(fromValue, rangeValue, _))

      new Range(
        id = rangeId,
        fromKey = fromKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        value = value,
        stats = Stats(Range.mergeKeys(fromKey, toKey), value, falsePositiveRate, rangeValue.isRemove || previous.exists(_.isRemoveRange), previous)
      )
    }
  }

  case class Range(id: Int,
                   fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fromValue: Option[Value.Fixed],
                   rangeValue: Value.Fixed,
                   value: Option[Slice[Byte]],
                   stats: Stats) extends Transient with RangeWriteOnly {

    override def key = fromKey

    override val isRemoveRange =
      rangeValue.isRemove

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(stats = Stats(Range.mergeKeys(fromKey, toKey), value, falsePositiveRate, isRemoveRange || keyValue.exists(_.isRemoveRange), keyValue))

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def fetchRangeValue: Try[Value.Fixed] =
      Success(rangeValue)

    override def fetchFromValue: Try[Option[Value.Fixed]] =
      Success(fromValue)

    override def fetchFromAndRangeValue: Try[(Option[Value.Fixed], Value.Fixed)] =
      Success(fromValue, rangeValue)

    override def isRemove: Boolean = false
  }

}
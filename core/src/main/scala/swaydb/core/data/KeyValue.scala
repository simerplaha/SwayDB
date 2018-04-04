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

import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Success, Try}

private[core] sealed trait KeyValue {
  def key: Slice[Byte]

  def isRemove: Boolean

  def notRemove: Boolean = !isRemove

  def keyLength =
    key.size

  def getOrFetchValue: Try[Option[Slice[Byte]]]
}

private[core] object KeyValue {

  sealed trait FindResponse extends KeyValue {
    def key: Slice[Byte]

    def valueLength: Int
  }

  /**
    * Read-only instances are only created for Key-values read from disk for Persistent Segments
    * and are stored in-memory after merge for Memory Segments.
    */
  sealed trait ReadOnly extends KeyValue

  object ReadOnly {
    sealed trait Fixed extends ReadOnly

    implicit class FixedReadOnlyImplicit(fixed: KeyValue.ReadOnly.Fixed) {
      def toValue: Try[Value] =
        fixed match {
          case _: Memory.Remove | _: Persistent.Remove =>
            Success(Value.Remove)

          case put: Memory.Put =>
            Success(Value.Put(put.value))

          case put: Persistent.Put =>
            put.getOrFetchValue.map(Value.Put(_))
        }
    }

    sealed trait Range extends ReadOnly {
      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fetchRangeValue: Try[Value]

      def fetchFromValue: Try[Option[Value]]

      def fetchFromAndRangeValue: Try[(Option[Value], Value)]
    }
  }

  /**
    * Write-only instances are only created after a successful merge of key-values and are used to write to Persistent
    * and Memory Segments.
    */
  sealed trait WriteOnly extends KeyValue {

    val id: Int
    val stats: Stats
    val isRemoveRange: Boolean
    val isRange: Boolean

    def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly

  }

  object WriteOnly {

    sealed trait Fixed extends KeyValue.WriteOnly

    implicit class FixedWriteOnlyImplicit(fixed: Fixed) {
      def toValue: Try[Value] =
        fixed match {
          case _: Transient.Remove =>
            Success(Value.Remove)
          case put: Transient.Put =>
            Success(Value.Put(put.value))
        }
    }

    sealed trait Range extends KeyValue.WriteOnly {
      val isRange: Boolean = true

      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fullKey: Slice[Byte]

      def fetchRangeValue: Try[Value]

      def fetchFromValue: Try[Option[Value]]

      def fetchFromAndRangeValue: Try[(Option[Value], Value)]
    }
  }

  type KeyValueTuple = (Slice[Byte], Option[Slice[Byte]])
}

private[core] sealed trait Transient extends KeyValue.WriteOnly {
  val value: Option[Slice[Byte]]
}

private[core] object Transient {

  object Remove {
    val id: Int = 0

    def apply(key: Slice[Byte]): Remove =
      new Remove(key, Stats(key, None, 0.1, isRemoveRange = false, isRange = false, None))

    def apply(key: Slice[Byte], falsePositiveRate: Double): Remove =
      new Remove(key, Stats(key, None, falsePositiveRate, isRemoveRange = false, isRange = false, None))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Remove =
      new Remove(key, Stats(key, None, falsePositiveRate, isRemoveRange = false, isRange = false, previous))
  }

  case class Remove(key: Slice[Byte],
                    stats: Stats) extends Transient with KeyValue.WriteOnly.Fixed {
    override val id: Int = Remove.id

    override val isRange: Boolean = false

    override val isRemoveRange =
      false

    override val value: Option[Slice[Byte]] = None

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)

    override def updateStats(falsePositiveRate: Double,
                             keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = key,
            value = None,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = isRange,
            previous = keyValue
          )
      )

    override def isRemove: Boolean = true
  }

  object Put {
    val id = 1

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly]): Put =
      new Put(key, value, Stats(key, value, falsePositiveRate, false, false, previousMayBe))

    def apply(key: Slice[Byte]): Put =
      Put(key, None, Stats(key, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte]): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = falsePositiveRate))

    def apply(key: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, None, falsePositiveRate, previous)

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, Some(value), Stats(key, value, falsePositiveRate = falsePositiveRate, previous))
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 stats: Stats) extends Transient with KeyValue.WriteOnly.Fixed {

    override val id: Int = Put.id

    override val isRemoveRange =
      false

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = key,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = isRange,
            previous = keyValue
          )
      )

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)

    override def isRemove: Boolean = false

    override val isRange: Boolean = false
  }

  object Range {
    val id = 2

    def apply[F <: Value, R <: Value](fromKey: Slice[Byte],
                                      toKey: Slice[Byte],
                                      fromValue: Option[F],
                                      rangeValue: R)(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range =
      Range(fromKey, toKey, fromValue, rangeValue, falsePositiveRate = 0.1, None)

    def apply[R <: Value](fromKey: Slice[Byte],
                          toKey: Slice[Byte],
                          rangeValue: R,
                          falsePositiveRate: Double,
                          previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId((), rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write((), rangeValue, _))
      val fullKey = ByteUtilCore.compress(fromKey, toKey)
      new Range(
        id = rangeId,
        fromKey = fromKey,
        toKey = toKey,
        fullKey = fullKey,
        fromValue = None,
        rangeValue = rangeValue,
        value = value,
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = rangeValue.isRemove,
            isRange = true,
            previous = previous
          )
      )
    }

    def apply[F <: Value, R <: Value](fromKey: Slice[Byte],
                                      toKey: Slice[Byte],
                                      fromValue: Option[F],
                                      rangeValue: R,
                                      falsePositiveRate: Double,
                                      previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write(fromValue, rangeValue, _))
      val fullKey = ByteUtilCore.compress(fromKey, toKey)

      new Range(
        id = rangeId,
        fromKey = fromKey,
        fullKey = fullKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        value = value,
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = rangeValue.isRemove,
            isRange = true,
            previous = previous
          )
      )
    }
  }

  case class Range(id: Int,
                   fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fullKey: Slice[Byte],
                   fromValue: Option[Value],
                   rangeValue: Value,
                   value: Option[Slice[Byte]],
                   stats: Stats) extends Transient with KeyValue.WriteOnly.Range {

    override def key = fromKey

    override val isRemoveRange =
      rangeValue.isRemove

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = true,
            previous = keyValue
          )
      )

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def fetchRangeValue: Try[Value] =
      Success(rangeValue)

    override def fetchFromValue: Try[Option[Value]] =
      Success(fromValue)

    override def fetchFromAndRangeValue: Try[(Option[Value], Value)] =
      Success(fromValue, rangeValue)

    override def isRemove: Boolean = false
  }

}

private[swaydb] sealed trait Memory extends KeyValue.ReadOnly {

  def key: Slice[Byte]

}

private[swaydb] object Memory {

  sealed trait Fixed extends Memory

  object Put {
    def apply(key: Slice[Byte],
              value: Slice[Byte]): Put =
      new Put(key, Some(value))

    def apply(key: Slice[Byte]): Put =
      new Put(key, None)
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]]) extends Memory.Fixed with KeyValue.FindResponse with KeyValue.ReadOnly.Fixed {

    override def isRemove: Boolean = false

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def valueLength: Int =
      value.map(_.size).getOrElse(0)
  }

  case class Remove(key: Slice[Byte]) extends Memory.Fixed with KeyValue.ReadOnly.Fixed {

    override def isRemove: Boolean = true

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(None)
  }

  case class Range(fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fromValue: Option[Value],
                   rangeValue: Value) extends Memory with KeyValue.ReadOnly.Range {

    override def isRemove: Boolean = false

    override def key: Slice[Byte] = fromKey

    override def fetchRangeValue: Try[Value] =
      Success(rangeValue)

    override def fetchFromValue: Try[Option[Value]] =
      Success(fromValue)

    override def fetchFromAndRangeValue: Try[(Option[Value], Value)] =
      Success(fromValue, rangeValue)

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Failure(new IllegalAccessError(s"${classOf[Range].getSimpleName} do not store value."))
  }
}

private[core] sealed trait Persistent extends KeyValue.ReadOnly {

  val indexOffset: Int
  val nextIndexOffset: Int
  val nextIndexSize: Int

  def key: Slice[Byte]

  def isValueDefined: Boolean

  def getOrFetchValue: Try[Option[Slice[Byte]]]

  def valueLength: Int

  def unsliceKey: Unit
}

private[core] object Persistent {

  sealed trait Fixed extends Persistent

  object Put {
    def apply(valueReader: Reader,
              indexOffset: Int)(key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Put =
      Put(key, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]]): Put =
      value match {
        case Some(value) =>
          Put(key, Reader(value), 0, 0, 0, 0, value.size)
        case None =>
          Put(key, Reader.emptyReader, 0, 0, 0, 0, 0)
      }
  }

  case class Put(private var _key: Slice[Byte],
                 valueReader: Reader,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 indexOffset: Int,
                 valueOffset: Int,
                 valueLength: Int) extends Persistent.Fixed with LazyValue with KeyValue.FindResponse with KeyValue.ReadOnly.Fixed {
    override def unsliceKey: Unit =
      _key = _key.unslice()

    override def key: Slice[Byte] =
      _key

    override def isRemove: Boolean =
      false
  }

  object Remove {
    def apply(indexOffset: Int)(key: Slice[Byte],
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Remove =
      Remove(key, indexOffset, nextIndexOffset, nextIndexSize)

    def apply(key: Slice[Byte]): Remove =
      Remove(key, 0, 0, 0)

  }

  case class Remove(private var _key: Slice[Byte],
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextIndexSize: Int) extends Persistent.Fixed with KeyValue.ReadOnly.Fixed {
    def key = _key

    override def unsliceKey(): Unit =
      _key = _key.unslice()

    override val valueLength: Int = 0

    override def isValueDefined: Boolean = true

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)

    override def isRemove: Boolean = true
  }

  object Range {
    def apply(valueReader: Reader,
              indexOffset: Int)(id: Int,
                                key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Try[Range] =
      ByteUtilCore.uncompress(key) map {
        case (fromKey, toKey) =>
          Range(id, fromKey, toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)
      }
  }

  case class Range(id: Int,
                   private var _fromKey: Slice[Byte],
                   private var _toKey: Slice[Byte],
                   valueReader: Reader,
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int) extends Persistent with LazyRangeValue with KeyValue.ReadOnly.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def unsliceKey: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    override def isRemove: Boolean = false
  }
}
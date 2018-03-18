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
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Success, Try}

private[core] sealed trait SegmentEntryType {

  val nextIndexOffset: Int
  val nextIndexSize: Int

  def key: Slice[Byte]

  def isValueDefined: Boolean

  def getOrFetchValue: Try[Option[Slice[Byte]]]

}

private[core] sealed trait SegmentEntryReadOnly extends SegmentEntryType with KeyValue.ReadOnly {
  val indexOffset: Int

  def unsliceKey: Unit
}

private[core] sealed trait SegmentEntry extends SegmentEntryType with KeyValue.WriteOnly

private[core] object SegmentEntry {

  object Put {
    def apply(key: Slice[Byte],
              valueReader: Reader,
              valueLength: Int,
              valueOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Put =
      new Put(key, valueReader, nextIndexOffset, nextIndexSize, valueOffset, Stats(key, valueLength, falsePositiveRate, previous.exists(_.isRemoveRange), previous))

    def apply(valueReader: Reader,
              falsePositiveRate: Double,
              previous: Option[SegmentEntry])(key: Slice[Byte],
                                              valueLength: Int,
                                              valueOffset: Int,
                                              nextIndexOffset: Int,
                                              nextIndexSize: Int): SegmentEntry.Put =
      SegmentEntry.Put(key, valueReader, valueLength, valueOffset, nextIndexOffset, nextIndexSize, falsePositiveRate, previous)
  }

  /**
    * @param valueOffset This valueOffset is the position of the value in the Segment this key-value belongs to and is
    *                    not the same as stats.valueOffset. stats.valueOffset is the value's position in the
    *                    List/Slice of key-values it currently belongs to.
    */
  case class Put(key: Slice[Byte],
                 valueReader: Reader,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 valueOffset: Int,
                 stats: Stats) extends SegmentEntry with FixedWriteOnly with LazyValue {

    override def id: Int =
      Transient.Put.id

    //since this is not a thread safe operation. unslice should only occur at the time of creation.
    override def valueLength: Int = stats.valueLength

    //TODO should this return a try ?
    //call updateStats will eager fetch the KeyValue from the old reader.
    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]) =
      getOrFetchValue map {
        value =>
          val updatedKeyValue = this.copy(stats = Stats(key, value, falsePositiveRate, keyValue.exists(_.isRemoveRange), keyValue))
          //value is fetched and the offset is changed, set the value fetched from the old reader.
          updatedKeyValue.valueOption = value
          updatedKeyValue
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          throw exception
      }

    override val isRemoveRange: Boolean = false

    override def isRemove: Boolean = false
  }

  object Range {

    def apply(id: Int,
              fromKey: Slice[Byte],
              toKey: Slice[Byte],
              valueLength: Int,
              valueReader: Reader,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              valueOffset: Int,
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Range =
      new Range(
        id = id,
        fromKey = fromKey,
        toKey = toKey,
        valueReader = valueReader,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        valueOffset = valueOffset,
        stats = Stats(Transient.Range.mergeKeys(fromKey, toKey), valueLength, falsePositiveRate, id == RangeValueSerializer.removeRangeId || previous.exists(_.isRemoveRange), previous)
      )

    def apply(valueReader: Reader,
              falsePositiveRate: Double,
              previous: Option[SegmentEntry])(id: Int,
                                              key: Slice[Byte],
                                              valueLength: Int,
                                              valueOffset: Int,
                                              nextIndexOffset: Int,
                                              nextIndexSize: Int): Try[SegmentEntry.Range] =
      Transient.Range.unMergeKeys(key) map {
        case (fromKey, toKey) =>
          new Range(
            id = id,
            fromKey = fromKey,
            toKey = toKey,
            valueReader = valueReader,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            valueOffset = valueOffset,
            stats = Stats(Transient.Range.mergeKeys(fromKey, toKey), valueLength, falsePositiveRate, id == RangeValueSerializer.removeRangeId || previous.exists(_.isRemoveRange), previous)
          )
      }
  }

  case class Range(id: Int,
                   fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   valueReader: Reader,
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   valueOffset: Int,
                   stats: Stats) extends SegmentEntry with RangeWriteOnly with LazyRangeValue {

    def key = fromKey

    override def valueLength: Int = stats.valueLength

    //call updateStats will eager fetch the KeyValue from the old reader.
    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]) =
      getOrFetchValue map {
        value =>
          val updatedKeyValue = this.copy(stats = Stats(Transient.Range.mergeKeys(fromKey, toKey), value, falsePositiveRate, this.isRemoveRange || keyValue.exists(_.isRemoveRange), keyValue))
          //value is fetched and the offset is changed, set the value fetched from the old reader.
          updatedKeyValue.valueOption = value
          updatedKeyValue
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          throw exception
      }

    override val isRemoveRange: Boolean = id == RangeValueSerializer.removeRangeId

    override def isRemove: Boolean = false
  }

  object PutReadOnly {
    def apply(valueReader: Reader,
              indexOffset: Int)(key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): SegmentEntry.PutReadOnly =
      SegmentEntry.PutReadOnly(key, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)
  }

  case class PutReadOnly(private var _key: Slice[Byte],
                         valueReader: Reader,
                         nextIndexOffset: Int,
                         nextIndexSize: Int,
                         indexOffset: Int,
                         valueOffset: Int,
                         valueLength: Int) extends SegmentEntryReadOnly with LazyValue {
    override def unsliceKey: Unit =
      _key = _key.unslice()

    override def key: Slice[Byte] =
      _key

    override def id: Int = Transient.Put.id

    override def isRemove: Boolean = false
  }

  object Remove {
    val id: Int = Transient.Remove.id

    def apply(key: Slice[Byte],
              nextIndexOffset: Int,
              nextIndexSize: Int,
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly]): Remove =
      new Remove(key, nextIndexOffset, nextIndexSize, Stats(key, None, falsePositiveRate, previousMayBe.exists(_.isRemoveRange), previousMayBe))

    def apply(falsePositiveRate: Double,
              previous: Option[SegmentEntry])(key: Slice[Byte],
                                              nextIndexOffset: Int,
                                              nextIndexSize: Int): SegmentEntry.Remove =
      SegmentEntry.Remove(key, nextIndexOffset, nextIndexSize, falsePositiveRate, previous)
  }

  case class Remove(private var _key: Slice[Byte],
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    override val stats: Stats) extends SegmentEntry with FixedWriteOnly {
    def key = _key

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(stats = Stats(key, None, falsePositiveRate, keyValue.exists(_.isRemoveRange), keyValue))

    override def isValueDefined: Boolean = false

    override def id: Int = Transient.Remove.id

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)

    override val isRemoveRange: Boolean = false

    override def isRemove: Boolean = true
  }

  object RemoveReadOnly {
    def apply(indexOffset: Int)(key: Slice[Byte],
                                nextIndexOffset: Int,
                                nextIndexSize: Int): SegmentEntry.RemoveReadOnly =
      SegmentEntry.RemoveReadOnly(key, indexOffset, nextIndexOffset, nextIndexSize)

  }

  case class RemoveReadOnly(private var _key: Slice[Byte],
                            indexOffset: Int,
                            nextIndexOffset: Int,
                            nextIndexSize: Int) extends SegmentEntryReadOnly {
    def key = _key

    override def unsliceKey(): Unit =
      _key = _key.unslice()

    override val valueLength: Int = 0

    override def isValueDefined: Boolean = false

    override def id: Int = Transient.Remove.id

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)

    override def isRemove: Boolean = true
  }

  object RangeReadOnly {
    def apply(valueReader: Reader,
              indexOffset: Int)(id: Int,
                                key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Try[SegmentEntry.RangeReadOnly] =
      Transient.Range.unMergeKeys(key) map {
        case (fromKey, toKey) =>
          SegmentEntry.RangeReadOnly(id, fromKey, toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)
      }
  }

  case class RangeReadOnly(id: Int,
                           private var _fromKey: Slice[Byte],
                           private var _toKey: Slice[Byte],
                           valueReader: Reader,
                           nextIndexOffset: Int,
                           nextIndexSize: Int,
                           indexOffset: Int,
                           valueOffset: Int,
                           valueLength: Int) extends SegmentEntryReadOnly with LazyRangeValue {

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
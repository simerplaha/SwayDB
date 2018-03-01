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

import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Success, Try}

private[core] sealed trait PersistentType {

  val nextIndexOffset: Int
  val nextIndexSize: Int

  def key: Slice[Byte]

  def isValueDefined: Boolean

  def getOrFetchValue: Try[Option[Slice[Byte]]]

  def unsliceKey: Unit
}

private[core] sealed trait PersistentReadOnly extends PersistentType with KeyValueReadOnly {
  val indexOffset: Int
}

private[core] sealed trait Persistent extends PersistentType with KeyValue

private[core] object Persistent {

  object Put {
    val id = 1

    def apply(key: Slice[Byte],
              valueReader: Reader,
              valueLength: Int,
              valueOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              falsePositiveRate: Double,
              previous: Option[KeyValue]): Put = {
      new Put(key, valueReader, nextIndexOffset, nextIndexSize, valueOffset, Stats(key, valueLength, false, falsePositiveRate, previous))
    }

    def apply(valueReader: Reader,
              falsePositiveRate: Double,
              previous: Option[Persistent])(key: Slice[Byte],
                                            valueLength: Int,
                                            valueOffset: Int,
                                            nextIndexOffset: Int,
                                            nextIndexSize: Int): Persistent.Put =
      Persistent.Put(key, valueReader, valueLength, valueOffset, nextIndexOffset, nextIndexSize, falsePositiveRate, previous)
  }


  /**
    * @param valueOffset This valueOffset is the position of the value in the Segment this key-value belongs to and is
    *                    not the same as stats.valueOffset. stats.valueOffset is the value's position in the
    *                    List/Slice of key-values it currently belongs to.
    */
  case class Put(private var _key: Slice[Byte],
                 valueReader: Reader,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 valueOffset: Int,
                 stats: Stats) extends Persistent with LazyValue {

    def key = _key

    //since this is not a thread safe operation. unslice should only occur at the time of creation.
    def unsliceKey: Unit =
      _key = _key.unslice()

    override def valueLength: Int = stats.valueLength

    //call updateStats will eager fetch the KeyValue from the old reader.
    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]) =
      getOrFetchValue map {
        value =>
          val updatedKeyValue = this.copy(stats = Stats(key, value, isDelete = false, falsePositiveRate, keyValue))
          //value is fetched and the offset is changed, set the value fetched from the old reader.
          updatedKeyValue.valueOption = value
          updatedKeyValue
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          throw exception
      }

  }

  object PutReadOnly {
    def apply(valueReader: Reader,
              indexOffset: Int)(key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Persistent.PutReadOnly =
      Persistent.PutReadOnly(key, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)
  }

  case class PutReadOnly(private var _key: Slice[Byte],
                         valueReader: Reader,
                         nextIndexOffset: Int,
                         nextIndexSize: Int,
                         indexOffset: Int,
                         valueOffset: Int,
                         valueLength: Int) extends PersistentReadOnly with LazyValue {
    override def unsliceKey: Unit =
      _key = _key.unslice()

    override def key: Slice[Byte] =
      _key

  }

  object Removed {
    val id: Int = 0

    def apply(key: Slice[Byte],
              nextIndexOffset: Int,
              nextIndexSize: Int,
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue]): Removed =
      new Removed(key, nextIndexOffset, nextIndexSize, Stats(key, None, true, falsePositiveRate, previousMayBe))

    def apply(falsePositiveRate: Double,
              previous: Option[Persistent])(key: Slice[Byte],
                                            nextIndexOffset: Int,
                                            nextIndexSize: Int): Persistent.Removed =
      Persistent.Removed(key, nextIndexOffset, nextIndexSize, falsePositiveRate, previous)
  }

  sealed trait RemovedBase {
    def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(None)

    def isValueDefined: Boolean = false

    def getValue: Option[Slice[Byte]] = None

    def isRemove: Boolean = true

    def id: Int = 0
  }

  case class Removed(private var _key: Slice[Byte],
                     nextIndexOffset: Int,
                     nextIndexSize: Int,
                     override val stats: Stats) extends Persistent with RemovedBase {
    def key = _key

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]): KeyValue =
      this.copy(stats = Stats(key, None, true, falsePositiveRate, keyValue))

    override def unsliceKey(): Unit =
      _key = _key.unslice()

  }
  object RemovedReadOnly {
    def apply(indexOffset: Int)(key: Slice[Byte],
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Persistent.RemovedReadOnly =
      Persistent.RemovedReadOnly(key, indexOffset, nextIndexOffset, nextIndexSize)

  }

  case class RemovedReadOnly(private var _key: Slice[Byte],
                             indexOffset: Int,
                             nextIndexOffset: Int,
                             nextIndexSize: Int) extends PersistentReadOnly with RemovedBase {
    def key = _key

    override def unsliceKey(): Unit =
      _key = _key.unslice()

    override val valueLength: Int = 0
  }

}